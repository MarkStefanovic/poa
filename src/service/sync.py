from __future__ import annotations

import datetime
import typing

from src import data

__all__ = ("sync",)


def sync(
    *,
    src_ds: data.SrcDs,
    dst_ds: data.DstDs,
    incremental: bool,
    compare_cols: set[str] | None,
    increasing_cols: set[str] | None,
    skip_if_row_counts_match: bool,
    recreate: bool,
) -> data.SyncResult:
    start_time = datetime.datetime.now()
    try:
        if recreate:
            incremental = False
            dst_ds.drop_table()
            dst_ds.create()
        elif not dst_ds.table_exists():
            incremental = False
            dst_ds.create()

        if incremental:
            if skip_if_row_counts_match:
                src_row_ct = src_ds.get_row_count()
                dst_row_ct = dst_ds.get_row_count()
                if src_row_ct == dst_row_ct:
                    return data.SyncResult.skipped(reason="row counts match.")

            if compare_cols:
                return _incremental_compare_refresh(
                    src_ds=src_ds,
                    dst_ds=dst_ds,
                    compare_cols=compare_cols,
                    start_time=start_time,
                )
            else:
                assert increasing_cols is not None, "No increasing_cols were provided."

                dst_ds.add_increasing_col_indices(increasing_cols)

                if after := dst_ds.get_max_values(increasing_cols):
                    if [1 for val in after.values() if val is not None]:
                        return _incremental_refresh_from_last(
                            src_ds=src_ds,
                            dst_ds=dst_ds,
                            after=after,
                            start_time=start_time,
                        )
                return _full_refresh(src_ds=src_ds, dst_ds=dst_ds, start_time=start_time)
        else:
            return _full_refresh(src_ds=src_ds, dst_ds=dst_ds, start_time=start_time)
    except Exception as e:
        return data.SyncResult.failed(
            error_message=f"An error occurred while running sync(): {e!s}\n{e.__traceback__}"
        )


def _full_refresh(
    *,
    src_ds: data.SrcDs,
    dst_ds: data.DstDs,
    start_time: datetime.datetime,
) -> data.SyncResult:
    dst_ds.truncate()
    src_rows = src_ds.fetch_rows(col_names=None, after=None)
    dst_ds.upsert_rows(src_rows)
    execution_millis = int((datetime.datetime.now() - start_time).total_seconds() * 1000)
    return data.SyncResult.succeeded(
        rows_added=len(src_rows),
        rows_deleted=0,
        rows_updated=0,
        execution_millis=execution_millis,
    )


def _incremental_refresh_from_last(
    *,
    src_ds: data.SrcDs,
    dst_ds: data.DstDs,
    after: dict[str, typing.Hashable] | None,
    start_time: datetime.datetime,
) -> data.SyncResult:
    assert after is not None, "after is required for _incremental_refresh_from_last, but got None."
    assert [1 for val in after.values() if val is not None], "after was empty."

    src_table = src_ds.get_table()

    src_rows = src_ds.fetch_rows(col_names=None, after=after)
    dst_rows = dst_ds.fetch_rows(col_names=None, after=after)

    row_diff = data.compare_rows(
        src_rows=src_rows,
        dst_rows=dst_rows,
        key_cols=src_table.pk,
    )

    dst_ds.upsert_rows(
        list(row_diff.added.values()) +
        list(src_row for src_row, dst_row in row_diff.updated.values())
    )

    execution_millis = int((datetime.datetime.now() - start_time).total_seconds() * 1000)

    return data.SyncResult.succeeded(
        rows_added=len(row_diff.added),
        rows_deleted=0,
        rows_updated=len(row_diff.updated),
        execution_millis=execution_millis,
    )


def _incremental_compare_refresh(
    *,
    src_ds: data.SrcDs,
    dst_ds: data.DstDs,
    compare_cols: set[str] | None,
    start_time: datetime.datetime,
) -> data.SyncResult:
    assert compare_cols, "compare_cols was empty."

    src_table = src_ds.get_table()

    min_cols = compare_cols.union(src_table.pk)

    min_src_rows = src_ds.fetch_rows(col_names=min_cols, after=None)

    src_row_ct = len(min_src_rows)

    if src_row_ct == 0:
        return data.SyncResult.skipped(
            reason=f"{src_table.db_name}.{src_table.schema_name}.{src_table.table_name} is empty."
        )

    min_dst_rows = dst_ds.fetch_rows(col_names=min_cols, after=None)

    row_diff = data.compare_rows(
        src_rows=min_src_rows,
        dst_rows=min_dst_rows,
        key_cols=src_table.pk,
    )

    added_keys = set(row_diff.added.keys())
    deleted_keys = set(row_diff.deleted.keys())
    updated_keys = set(row_diff.updated.keys())

    chg_row_ct = len(added_keys) + len(deleted_keys) + len(updated_keys)

    if chg_row_ct == 0:
        return data.SyncResult.skipped(reason="src and dst were compared, and they were the same.")

    chg_pct = chg_row_ct/src_row_ct
    if chg_row_ct > 1000 and chg_pct > 0.3:
        return _full_refresh(src_ds=src_ds, dst_ds=dst_ds, start_time=start_time)

    if added_keys or updated_keys:
        upsert_rows = src_ds.fetch_rows_by_key(
            col_names=None,
            keys=added_keys.union(updated_keys),
        )

        dst_ds.upsert_rows(upsert_rows)

    if deleted_keys:
        dst_ds.delete_rows(keys=deleted_keys)

    execution_millis = int((datetime.datetime.now() - start_time).total_seconds() * 1000)

    return data.SyncResult.succeeded(
        rows_added=len(row_diff.added),
        rows_deleted=len(row_diff.deleted),
        rows_updated=len(row_diff.updated),
        execution_millis=execution_millis,
    )

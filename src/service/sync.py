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
) -> data.SyncResult:
    try:
        if not dst_ds.table_exists():
            incremental = False
            dst_ds.create()

        if incremental:
            sync_table_spec = dst_ds.get_sync_table_spec()

            after = dst_ds.get_increasing_col_values()

            return _incremental_refresh(
                src_ds=src_ds,
                dst_ds=dst_ds,
                sync_table_spec=sync_table_spec,
                after=after,
            )

        return _full_refresh(src_ds=src_ds, dst_ds=dst_ds)
    except Exception as e:
        return data.SyncResult.failed(
            error_message=f"An error occurred while running sync(): {e!s}\n{e.__traceback__}"
        )


def _full_refresh(
    *,
    src_ds: data.SrcDs,
    dst_ds: data.DstDs,
) -> data.SyncResult:
    start = datetime.datetime.now()
    dst_ds.truncate()
    src_rows = src_ds.fetch_rows(col_names=None, after=None)
    dst_ds.upsert_rows(src_rows)
    execution_millis = int((datetime.datetime.now() - start).total_seconds() * 1000)
    return data.SyncResult.succeeded(
        rows_added=len(src_rows),
        rows_deleted=0,
        rows_updated=0,
        execution_millis=execution_millis,
    )


def _incremental_refresh(
    *,
    src_ds: data.SrcDs,
    dst_ds: data.DstDs,
    sync_table_spec: data.SyncTableSpec,
    after: dict[str, typing.Hashable] | None,
) -> data.SyncResult:
    if not after or not [1 for val in after.values() if val is not None]:
        after = None

    if sync_table_spec.skip_if_row_counts_match:
        src_row_ct = src_ds.get_row_count()
        dst_row_ct = dst_ds.get_row_count()
        if src_row_ct == dst_row_ct:
            return data.SyncResult.skipped(reason="row counts match.")

    src_table = src_ds.get_table()

    start_time = datetime.datetime.now()

    if sync_table_spec.compare_cols:
        min_cols = sync_table_spec.compare_cols.union(src_table.pk)

        min_src_rows = src_ds.fetch_rows(col_names=min_cols, after=None)
        min_dst_rows = dst_ds.fetch_rows(col_names=min_cols, after=None)

        row_diff = data.compare_rows(
            src_rows=min_src_rows,
            dst_rows=min_dst_rows,
            key_cols=src_table.pk,
        )

        upsert_rows = src_ds.fetch_rows_by_key(
            col_names=None,
            keys=set(row_diff.added.keys()).union(row_diff.updated.keys()),
        )

        dst_ds.upsert_rows(upsert_rows)

        dst_ds.delete_rows(keys=set(row_diff.deleted.keys()))
    else:
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
        rows_deleted=len(row_diff.deleted),
        rows_updated=len(row_diff.updated),
        execution_millis=execution_millis,
    )

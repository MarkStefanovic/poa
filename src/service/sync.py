import datetime
import pathlib
import traceback
import typing

from loguru import logger

from src import data, adapter

__all__ = ("sync",)

from src.service import inspect


def sync(
    *,
    src_db_config: data.DbConfig,
    src_schema_name: str | None,
    src_table_name: str,
    dst_db_config: data.DbConfig,
    dst_schema_name: str,
    dst_table_name: str,
    incremental: bool,
    pk: list[str],
    compare_cols: set[str] | None,
    increasing_cols: set[str] | None,
    skip_if_row_counts_match: bool,
    recreate: bool,
    batch_ts: datetime.datetime,
    track_history: bool,
    after: dict[str, datetime.date],
    batch_size: int,
) -> None | data.Error:
    try:
        log = adapter.log.create(db_config=dst_db_config)
        if isinstance(log, data.Error):
            return log

        sync_id = log.sync_started(
            src_db_name=src_db_config.db_name,
            src_schema_name=src_schema_name,
            src_table_name=src_table_name,
            incremental=incremental,
        )
        if isinstance(sync_id, data.Error):
            return sync_id

        src_table = inspect(
            src_config=src_db_config,
            src_schema_name=src_schema_name,
            src_table_name=src_table_name,
            dst_config=dst_db_config,
            pk=pk,
        )
        if isinstance(src_table, data.Error):
            return src_table

        src_cursor_provider = adapter.cursor_provider.create(db_config=src_db_config)
        if isinstance(src_cursor_provider, data.Error):
            return src_cursor_provider

        with src_cursor_provider.open() as src_cur:
            if isinstance(src_cur, data.Error):
                return src_cur

            src_ds = adapter.src_ds.create(
                cur=src_cur,
                api=src_db_config.api,
                db_name=src_db_config.db_name,
                schema_name=src_schema_name,
                table_name=src_table_name,
                pk_cols=tuple(pk),
                after=after,
            )
            if isinstance(src_ds, data.Error):
                return src_ds

            dst_cursor_provider = adapter.cursor_provider.create(db_config=dst_db_config)
            if isinstance(dst_cursor_provider, data.Error):
                return dst_cursor_provider

            with dst_cursor_provider.open() as dst_cur:
                if isinstance(dst_cur, data.Error):
                    return dst_cur

                dst_ds = adapter.dst_ds.create(
                    api=dst_db_config.api,
                    cur=dst_cur,
                    dst_db_name=dst_db_config.db_name,
                    dst_schema_name=dst_schema_name,
                    dst_table_name=dst_table_name,
                    src_table=src_table,
                    batch_ts=batch_ts,
                    after=after,
                )

                result = _sync(
                    src_ds=src_ds,
                    dst_ds=dst_ds,
                    incremental=incremental,
                    compare_cols=compare_cols,
                    increasing_cols=increasing_cols,
                    skip_if_row_counts_match=skip_if_row_counts_match,
                    recreate=recreate,
                    batch_size=batch_size,
                    track_history=track_history,
                )
                if isinstance(result, data.Error):
                    return result

            if result.status == "succeeded":
                log_result = log.sync_succeeded(
                    sync_id=sync_id,
                    rows_added=result.rows_added,
                    rows_deleted=result.rows_deleted,
                    rows_updated=result.rows_updated,
                    execution_millis=result.execution_millis or 0,
                )
                if isinstance(log_result, data.Error):
                    return log_result
            elif result.status == "failed":
                log_result = log.sync_failed(
                    sync_id=sync_id,
                    reason=result.error_message or "No error message was provided.",
                )
                if isinstance(log_result, data.Error):
                    return log_result
            elif result.status == "skipped":
                log_result = log.sync_skipped(
                    sync_id=sync_id,
                    reason=result.skip_reason or "No skip reason was provided.",
                )
                if isinstance(log_result, data.Error):
                    return log_result
            else:
                return data.Error.new(
                    f"Unexpected result.status: {result.status!r}",
                    src_db_config=src_db_config,
                    src_schema_name=src_schema_name,
                    src_table_name=src_table_name,
                    dst_db_config=dst_db_config,
                    dst_schema_name=dst_schema_name,
                    dst_table_name=dst_table_name,
                    incremental=incremental,
                    pk=tuple(pk),
                    compare_cols=tuple(compare_cols),
                    increasing_cols=tuple(increasing_cols),
                    skip_if_row_counts_match=skip_if_row_counts_match,
                    recreate=recreate,
                    batch_ts=batch_ts,
                    track_history=track_history,
                    after=tuple(after.items()),
                )
    except Exception as e:
        return data.Error.new(
            str(e),
            src_db_config=src_db_config,
            src_schema_name=src_schema_name,
            src_table_name=src_table_name,
            dst_db_config=dst_db_config,
            dst_schema_name=dst_schema_name,
            dst_table_name=dst_table_name,
            incremental=incremental,
            pk=tuple(pk),
            compare_cols=tuple(compare_cols),
            increasing_cols=tuple(increasing_cols),
            skip_if_row_counts_match=skip_if_row_counts_match,
            recreate=recreate,
            batch_ts=batch_ts,
            track_history=track_history,
            after=tuple(after.items()),
        )


def _sync(
    *,
    src_ds: data.SrcDs,
    dst_ds: data.DstDs,
    incremental: bool,
    compare_cols: set[str] | None,
    increasing_cols: set[str] | None,
    skip_if_row_counts_match: bool,
    recreate: bool,
    batch_size: int,
    track_history: bool,
) -> data.SyncResult | data.Error:
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
                result = _incremental_compare_refresh(
                    src_ds=src_ds,
                    dst_ds=dst_ds,
                    compare_cols=compare_cols,
                    start_time=start_time,
                    batch_size=batch_size,
                )
            else:
                assert increasing_cols is not None, "No increasing_cols were provided."

                dst_ds.add_increasing_col_indices(increasing_cols)

                after = dst_ds.get_max_values(increasing_cols)

                result = _incremental_refresh_from_last(
                    src_ds=src_ds,
                    dst_ds=dst_ds,
                    after=after,
                    start_time=start_time,
                    batch_size=batch_size,
                )
        else:
            result = _full_refresh(
                src_ds=src_ds,
                dst_ds=dst_ds,
                start_time=start_time,
                batch_size=batch_size,
            )

        if track_history and (
            result.rows_added > 0 or result.rows_deleted > 0 or result.rows_updated > 0
        ):
            dst_ds.create_history_table()
            dst_ds.update_history_table()

        return result
    except Exception as e:
        return data.SyncResult.failed(
            error_message=f"An error occurred while running sync(): {e!s}\n{traceback.format_exc()}"
        )


def _full_refresh(
    *,
    src_ds: data.SrcDs,
    dst_ds: data.DstDs,
    start_time: datetime.datetime,
    batch_size: int,
) -> data.SyncResult:
    dst_ds.truncate()

    src_rows = src_ds.fetch_rows(col_names=None, after=None)
    rows_to_upsert = len(src_rows)
    rows_upserted = 0
    for chunk in iter_chunk(items=src_rows, n=batch_size):
        logger.info(
            f"Upserting rows {rows_upserted} to {rows_upserted + len(chunk)} of {rows_to_upsert}..."
        )
        dst_ds.upsert_rows(chunk)
        rows_upserted += len(chunk)

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
    batch_size: int,
) -> data.SyncResult:
    if after is None:
        final_after: dict[str, typing.Hashable] | None = None
    else:
        nonzero_after = {k: v for k, v in after.items() if v is not None}
        if nonzero_after:
            final_after = nonzero_after
        else:
            final_after = None

    src_table = src_ds.get_table()

    src_rows = src_ds.fetch_rows(col_names=None, after=final_after)
    dst_rows = dst_ds.fetch_rows(col_names=None, after=final_after)

    row_diff = data.compare_rows(
        src_rows=src_rows,
        dst_rows=dst_rows,
        key_cols=src_table.pk,
    )

    rows = list(row_diff.added.values()) + list(
        src_row for src_row, dst_row in row_diff.updated.values()
    )
    rows_to_upsert = len(rows)
    rows_upserted = 0
    for chunk in iter_chunk(items=rows, n=batch_size):
        logger.info(
            f"Upserting rows {rows_upserted} to {rows_upserted + len(chunk)} of {rows_to_upsert}..."
        )
        dst_ds.upsert_rows(chunk)
        rows_upserted += len(chunk)

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
    batch_size: int,
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

    changed_keys = set(row_diff.added.keys()).union(row_diff.updated.keys())
    deleted_keys = set(row_diff.deleted.keys())

    logger.info(
        f"There were {len(row_diff.added.keys())} rows added, {len(row_diff.updated.keys())} updated, "
        f"and {len(row_diff.deleted.keys())} rows deleted from src."
    )

    chg_row_ct = len(changed_keys) + len(deleted_keys)

    if chg_row_ct == 0:
        return data.SyncResult.skipped(reason="src and dst were compared, and they were the same.")

    if (proportion_chg := chg_row_ct / src_row_ct) > 0.5:
        logger.info(
            f"There were {chg_row_ct} rows that have changed of {src_row_ct} totals rows "
            f"({int(proportion_chg * 100)}%), so the full table will be pulled."
        )
        src_rows = src_ds.fetch_rows(col_names=None, after=None)
    else:
        src_rows = src_ds.fetch_rows_by_key(col_names=None, keys=changed_keys)

    if src_rows:
        rows_to_upsert = len(src_rows)
        rows_upserted = 0
        for chunk in iter_chunk(items=src_rows, n=batch_size):
            logger.info(
                f"Upserting rows {rows_upserted} to {rows_upserted + len(chunk)} of {rows_to_upsert}..."
            )
            dst_ds.upsert_rows(chunk)
            rows_upserted += len(chunk)

    if deleted_keys:
        keys_to_delete = len(deleted_keys)
        keys_deleted = 0
        for chunk in iter_chunk(items=list(deleted_keys), n=batch_size):
            logger.info(
                f"Deleting rows {keys_deleted} to {keys_deleted + len(chunk)} of {keys_to_delete}..."
            )
            dst_ds.delete_rows(keys=set(chunk))
            keys_deleted += len(chunk)

    execution_millis = int((datetime.datetime.now() - start_time).total_seconds() * 1000)

    return data.SyncResult.succeeded(
        rows_added=len(row_diff.added),
        rows_deleted=len(row_diff.deleted),
        rows_updated=len(row_diff.updated),
        execution_millis=execution_millis,
    )


def iter_chunk(items: list[typing.Any], n: int) -> typing.Generator[typing.Any, None, None]:
    for i in range(0, len(items), n):
        yield items[i : i + n]

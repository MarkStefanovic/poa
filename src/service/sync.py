import datetime
import pathlib
import traceback
import typing

from loguru import logger

from src import data, adapter

__all__ = ("sync",)


def sync(
    *,
    src_db_name: str,
    src_schema_name: str | None,
    src_table_name: str,
    dst_db_name: str,
    dst_schema_name: str,
    dst_table_name: str,
    incremental: bool,
    pk: list[str],
    compare_cols: set[str] | None,
    increasing_cols: set[str] | None,
    skip_if_row_counts_match: bool,
    recreate: bool,
    log_folder: pathlib.Path,
    batch_ts: datetime.datetime,
    track_history: bool,
    after: dict[str, datetime.date],
) -> None:
    try:
        if src_schema_name:
            prefix = f"sync.{src_db_name}.{src_schema_name}.{src_table_name}."
        else:
            prefix = f"sync.{src_db_name}.{src_table_name}."

        logger.add(log_folder / f"{prefix}.info.log", rotation="5 MB", retention="7 days", level="INFO")
        logger.add(log_folder / f"{prefix}.error.log", rotation="5 MB", retention="7 days", level="ERROR")

        logger.info(
            f"Starting sync using the following parameters:\n  {src_db_name=!r}\n  {dst_db_name=!r}\n  "
            f"{dst_schema_name=!r}\n  {dst_table_name=!r}\n  {src_schema_name=!r}\n  {src_table_name=!r}\n  "
            f"{incremental=!r}\n  {pk=!r}\n  {compare_cols=!r}\n  {increasing_cols=!r}\n  "
            f"{skip_if_row_counts_match=!r}\n  {recreate=!r}\n..."
        )

        config_file = adapter.fs.get_config_path()

        dst_api = adapter.config.get_api(config_file=config_file, name=dst_db_name)
        dst_connection_str = adapter.config.get_connection_str(config_file=config_file, name=dst_db_name)

        log_cursor_provider = adapter.cursor_provider.create(api=dst_api, connection_str=dst_connection_str)
        log = adapter.log.create(api=dst_api, cursor_provider=log_cursor_provider)

        sync_id = log.sync_started(
            src_db_name=src_db_name,
            src_schema_name=src_schema_name,
            src_table_name=src_table_name,
            incremental=incremental,
        )

        try:
            src_api = adapter.config.get_api(config_file=config_file, name=src_db_name)
            src_connection_str = adapter.config.get_connection_str(config_file=config_file, name=src_db_name)
            src_cursor_provider = adapter.cursor_provider.create(api=src_api, connection_str=src_connection_str)

            dst_api = adapter.config.get_api(config_file=config_file, name=dst_db_name)
            dst_connection_str = adapter.config.get_connection_str(config_file=config_file, name=dst_db_name)
            dst_cursor_provider = adapter.cursor_provider.create(api=dst_api, connection_str=dst_connection_str)

            with src_cursor_provider.open() as src_cur, dst_cursor_provider.open() as dst_cur:
                src_ds = adapter.src_ds.create(
                    api=src_api,
                    cur=src_cur,
                    db_name=src_db_name,
                    schema_name=src_schema_name,
                    table_name=src_table_name,
                    pk_cols=tuple(pk),
                    after=after,
                )

                cache = adapter.cache.create(api=dst_api, cur=dst_cur)

                if cached_src_table := cache.get_table_def(
                    db_name=src_db_name,
                    schema_name=src_schema_name,
                    table_name=src_table_name,
                ):
                    if cached_src_table.pk != tuple(pk):
                        raise Exception(
                            f"The cached primary key columns for {src_table_name}, {', '.join(cached_src_table.pk)} "
                            f"does not match the pk argument, {', '.join(pk)}."
                        )
                    src_table = cached_src_table
                else:
                    src_table = src_ds.get_table()
                    cache.add_table_def(table=src_table)

                dst_ds = adapter.dst_ds.create(
                    api=dst_api,
                    cur=dst_cur,
                    dst_db_name=dst_db_name,
                    dst_schema_name=dst_schema_name,
                    dst_table_name=dst_table_name,
                    src_table=src_table,
                    batch_ts=batch_ts,
                    after=after,
                )

                batch_size = adapter.config.get_batch_size(config_file=config_file)

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

                if result.status == "succeeded":
                    log.sync_succeeded(
                        sync_id=sync_id,
                        rows_added=result.rows_added,
                        rows_deleted=result.rows_deleted,
                        rows_updated=result.rows_updated,
                        execution_millis=result.execution_millis or 0,
                    )
                elif result.status == "failed":
                    logger.error(result.error_message or "No error message was provided.")
                    log.sync_failed(sync_id=sync_id, reason=result.error_message or "No error message was provided.")
                elif result.status == "skipped":
                    log.sync_skipped(sync_id=sync_id, reason=result.skip_reason or "No skip reason was provided.")
                else:
                    raise Exception(f"Unexpected result.status: {result.status!r}")
        except Exception as e1:
            log.sync_failed(
                sync_id=sync_id,
                reason=(
                    f"An error occurred while running sync({src_db_name=!r}, {src_schema_name=!r}, "
                    f"{src_table_name=!r}, {dst_db_name=!r}, {dst_schema_name=!r}, {incremental=}): "
                    f"{e1!s}\n{traceback.format_exc()}"
                ),
            )
            raise
    except Exception as e2:
        logger.error(
            f"An error occurred while running sync({src_db_name=!r}, {src_schema_name=!r}, "
            f"{src_table_name=!r}, {dst_db_name=!r}, {dst_schema_name=!r}, {incremental=}): "
            f"{e2!s}\n{traceback.format_exc()}"
        )
        raise


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
            result.rows_added > 0 or
            result.rows_deleted > 0 or
            result.rows_updated > 0
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
        logger.info(f"Upserting rows {rows_upserted} to {rows_upserted + len(chunk)} of {rows_to_upsert}...")
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

    rows = list(row_diff.added.values()) + list(src_row for src_row, dst_row in row_diff.updated.values())
    rows_to_upsert = len(rows)
    rows_upserted = 0
    for chunk in iter_chunk(items=rows, n=batch_size):
        logger.info(f"Upserting rows {rows_upserted} to {rows_upserted + len(chunk)} of {rows_to_upsert}...")
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

    if (proportion_chg := chg_row_ct/src_row_ct) > 0.5:
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
            logger.info(f"Upserting rows {rows_upserted} to {rows_upserted + len(chunk)} of {rows_to_upsert}...")
            dst_ds.upsert_rows(chunk)
            rows_upserted += len(chunk)

    if deleted_keys:
        keys_to_delete = len(deleted_keys)
        keys_deleted = 0
        for chunk in iter_chunk(items=list(deleted_keys), n=batch_size):
            logger.info(f"Deleting rows {keys_deleted} to {keys_deleted + len(chunk)} of {keys_to_delete}...")
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
        yield items[i:i + n]

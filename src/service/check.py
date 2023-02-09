import datetime
import pathlib
import traceback

from loguru import logger

from src import data, adapter

__all__ = ("check",)


def check(
    *,
    src_db_name: str,
    src_schema_name: str | None,
    src_table_name: str,
    dst_db_name: str,
    dst_schema_name: str | None,
    dst_table_name: str,
    pk: list[str],
    after: dict[str, datetime.date],
    log_folder: pathlib.Path,
    batch_ts: datetime.datetime,
) -> None:
    try:
        if src_schema_name:
            prefix = f"check.{src_db_name}.{src_schema_name}.{src_table_name}."
        else:
            prefix = f"check.{src_db_name}.{src_table_name}."

        logger.add(log_folder / f"{prefix}.info.log", rotation="5 MB", retention="7 days", level="INFO")
        logger.add(log_folder / f"{prefix}.error.log", rotation="5 MB", retention="7 days", level="ERROR")

        logger.info(
            f"Starting check using the following parameters:\n  {src_db_name=!r}\n  {dst_db_name=!r}\n  "
            f"{src_schema_name=!r}\n  {src_table_name=!r}\n  {pk=!r}\n..."
        )

        config_file = adapter.fs.get_config_path()

        dst_api = adapter.config.get_api(config_file=config_file, name=dst_db_name)
        dst_connection_str = adapter.config.get_connection_str(config_file=config_file, name=dst_db_name)

        log_cursor_provider = adapter.cursor_provider.create(api=dst_api, connection_str=dst_connection_str)
        log = adapter.log.create(api=dst_api, cursor_provider=log_cursor_provider)

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

                result = _check(
                    src_ds=src_ds,
                    dst_ds=dst_ds,
                    src_db_name=src_db_name,
                    src_schema_name=src_schema_name,
                    src_table_name=src_table_name,
                    dst_db_name=dst_db_name,
                    dst_schema_name=dst_schema_name,
                    dst_table_name=dst_table_name,
                    pk=tuple(pk),
                )

                dst_ds.add_check_result(result)
        except Exception as e1:
            log.error(
                f"An error occurred while running check({src_db_name=!r}, {src_schema_name=!r}, "
                f"{src_table_name=!r}, {dst_db_name=!r}, {dst_schema_name=!r}, {e1!s}\n{traceback.format_exc()}"
            )
            raise
    except Exception as e2:
        logger.error(
            f"An error occurred while running check({src_db_name=!r}, {src_schema_name=!r}, "
            f"{src_table_name=!r}, {dst_db_name=!r}, {dst_schema_name=!r}): {e2!s}\n{traceback.format_exc()}"
        )
        raise


def _check(
    *,
    src_ds: data.SrcDs,
    dst_ds: data.DstDs,
    src_db_name: str,
    src_schema_name: str | None,
    src_table_name: str,
    dst_db_name: str,
    dst_schema_name: str | None,
    dst_table_name: str,
    pk: tuple[str, ...],
) -> data.CheckResult:
    try:
        assert pk, "pk is required."

        start = datetime.datetime.now()

        src_row_ct = src_ds.get_row_count()
        dst_row_ct = dst_ds.get_row_count()

        src_keys = set(data.FrozenDict(row) for row in src_ds.fetch_rows(col_names=set(pk), after=None))
        dst_keys = set(data.FrozenDict(row) for row in src_ds.fetch_rows(col_names=set(pk), after=None))

        extra_keys = dst_keys - src_keys
        missing_keys = src_keys - dst_keys

        execution_millis = int((datetime.datetime.now() - start).total_seconds() * 1000)

        return data.CheckResult(
            src_db_name=src_db_name,
            src_schema_name=src_schema_name,
            src_table_name=src_table_name,
            dst_db_name=dst_db_name,
            dst_schema_name=dst_schema_name,
            dst_table_name=dst_table_name,
            src_rows=src_row_ct,
            dst_rows=dst_row_ct,
            extra_keys=frozenset(extra_keys),
            missing_keys=frozenset(missing_keys),
            execution_millis=execution_millis,
        )
    except Exception as e:
        raise data.error.CheckError(
            f"An error occurred while running check(src_ds=..., dst_ds=..., {src_db_name=!r}, "
            f"{src_schema_name=!r}, {src_table_name=!r}, {dst_db_name=!r}, {dst_schema_name=!r}, "
            f"{dst_table_name=!r}): {e!s}\n{traceback.format_exc()}"
        )

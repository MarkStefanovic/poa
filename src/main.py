from __future__ import annotations

import datetime
import pathlib
import sys

import loguru
import typer

from src import adapter, service

app = typer.Typer()


@app.command()
def cleanup(ds_name: str, days_logs_to_keep: int = 3) -> None:
    try:
        _init_loguru_logger(filename_prefix="cleanup.")

        config_file = adapter.fs.get_config_path()

        dst_api = adapter.config.get_api(config_file=config_file, name=ds_name)
        dst_connection_str = adapter.config.get_api(config_file=config_file, name=ds_name)

        logger_cursor_provider = adapter.cursor_provider.create(api=dst_api, connection_str=dst_connection_str)

        log = adapter.log.create(api=dst_api, cursor_provider=logger_cursor_provider)
        try:
            service_cursor_provider = adapter.cursor_provider.create(api=dst_api, connection_str=dst_connection_str)
            service.cleanup(
                cursor_provider=service_cursor_provider,
                days_logs_to_keep=adapter.config.get_days_logs_to_keep(config_file=config_file),
            )
            log.cleanup_succeeded()
        except Exception as e1:
            loguru.logger.exception(e1)
            raise
    except Exception as e2:
        loguru.logger.error(
            f"An error occurred while running cleanup({ds_name=!r}, {days_logs_to_keep=}): "
            f"{e2!s}\n{e2.__traceback__}")
        raise


@app.command()
def sync(
    src_db_name: str = typer.Option(None, "--src-db"),
    dst_db_name: str = typer.Option(None, "--dst-db"),
    src_schema_name: str = typer.Option(None, "--src-schema"),
    src_table_name: str = typer.Option(None, "--src-table"),
    incremental: bool = typer.Option(True, "--incremental"),
    pk: str = typer.Option(None, "--pk"),
) -> None:
    print(f"{src_db_name=}, {dst_db_name=}, {src_schema_name=}, {src_table_name=}, {incremental=}")
    try:
        if src_schema_name:
            prefix = f"sync.{src_db_name}.{src_schema_name}.{src_table_name}."
        else:
            prefix = f"sync.{src_db_name}.{src_table_name}."

        _init_loguru_logger(filename_prefix=prefix)

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
            dst_schema_name = adapter.config.get_dst_schema(config_file=config_file)

            if pk:
                pk_cols = tuple(c.strip() for c in pk.split(","))
            else:
                pk_cols = None

            with src_cursor_provider.open() as src_cur, dst_cursor_provider.open() as dst_cur:
                src_ds = adapter.src_ds.create(
                    api=src_api,
                    cur=src_cur,
                    db_name=src_db_name,
                    schema_name=src_schema_name,
                    table_name=src_table_name,
                    pk_cols=pk_cols,
                )
                dst_ds = adapter.dst_ds.create(
                    api=dst_api,
                    cur=dst_cur,
                    dst_db_name=dst_db_name,
                    dst_schema_name=dst_schema_name,
                    src_table=src_ds.get_table(),
                )
                start = datetime.datetime.now()
                result = service.sync(
                    src_ds=src_ds,
                    dst_ds=dst_ds,
                    incremental=incremental,
                )
                if result.status == "succeeded":
                    execution_millis = int((datetime.datetime.now() - start).total_seconds() * 1000)
                    log.sync_succeeded(sync_id=sync_id, execution_millis=execution_millis)
                elif result.status == "failed":
                    log.sync_failed(sync_id=sync_id, reason=result.error_message or "No error message was provided.")
                elif result.status == "skipped":
                    log.sync_skipped(sync_id=sync_id, reason=result.skip_reason or "No skip reason was provided.")
                else:
                    raise Exception(f"Unexpected result.status: {result.status!r}")
        except Exception as e1:
            log.sync_failed(
                sync_id=sync_id,
                reason=(
                    f"An error occurred while running sync({src_db_name=!r}, {dst_db_name=!r}, "
                    f"{src_schema_name=!r}, {src_table_name=!r}, {incremental=}): {e1!s}\n{e1.__traceback__}"
                ),
            )
    except Exception as e2:
        loguru.logger.error(
            f"An error occurred while running sync({src_db_name=!r}, {dst_db_name=!r}, "
            f"{src_schema_name=!r}, {src_table_name=!r}, {incremental=}): {e2!s}\n{e2.__traceback__}"
        )
        raise


def _init_loguru_logger(*, log_folder: pathlib.Path = adapter.fs.get_log_folder(), filename_prefix: str = "") -> None:
    if getattr(sys, "frozen", False):
        loguru.logger.add(sys.stderr, format="{time} {level} {message}", level="DEBUG")

    loguru.logger.add(log_folder / f"{filename_prefix}error.log", rotation="5 MB", retention="7 days", level="ERROR")


if __name__ == '__main__':
    app()

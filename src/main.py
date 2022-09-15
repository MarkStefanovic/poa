from __future__ import annotations

import argparse
import datetime
import pathlib
import sys

import loguru

from src import adapter, service


def cleanup(ds_name: str, log_folder: pathlib.Path, days_logs_to_keep: int = 3) -> None:
    try:
        loguru.logger.add(log_folder / f"cleanup.{ds_name}.info.log", rotation="5 MB", retention="7 days", level="INFO")
        loguru.logger.add(log_folder / f"cleanup.{ds_name}.error.log", rotation="5 MB", retention="7 days", level="ERROR")

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


def sync(
    src_db_name: str,
    dst_db_name: str,
    src_schema_name: str | None,
    src_table_name: str,
    incremental: bool,
    pk: list[str],
    compare_cols: set[str] | None,
    increasing_cols: set[str] | None,
    skip_if_row_counts_match: bool,
    log_folder: pathlib.Path,
) -> None:
    try:
        if src_schema_name:
            prefix = f"sync.{src_db_name}.{src_schema_name}.{src_table_name}."
        else:
            prefix = f"sync.{src_db_name}.{src_table_name}."

        loguru.logger.add(log_folder / f"{prefix}.info.log", rotation="5 MB", retention="7 days", level="INFO")
        loguru.logger.add(log_folder / f"{prefix}.error.log", rotation="5 MB", retention="7 days", level="ERROR")

        loguru.logger.info(
            f"Starting sync using the following parameters:\n  {src_db_name=!r}\n  "
            f"{dst_db_name=!r}\n  {src_schema_name=!r}\n  {src_table_name=!r}\n  "
            f"{incremental=!r}\n  {pk=!r}\n  {compare_cols=!r}\n  {increasing_cols=!r}\n  {skip_if_row_counts_match=!r}"
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
            dst_schema_name = adapter.config.get_dst_schema(config_file=config_file)

            with src_cursor_provider.open() as src_cur, dst_cursor_provider.open() as dst_cur:
                src_ds = adapter.src_ds.create(
                    api=src_api,
                    cur=src_cur,
                    db_name=src_db_name,
                    schema_name=src_schema_name,
                    table_name=src_table_name,
                    pk_cols=tuple(pk),
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
                    compare_cols=compare_cols,
                    increasing_cols=increasing_cols,
                    skip_if_row_counts_match=skip_if_row_counts_match,
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


if __name__ == '__main__':
    if getattr(sys, "frozen", False):
        loguru.logger.add(sys.stderr, format="{time} {level} {message}", level="DEBUG")

    logging_folder = adapter.fs.get_log_folder()

    loguru.logger.add(logging_folder / f"error.log", rotation="5 MB", retention="7 days", level="ERROR")

    try:
        parser = argparse.ArgumentParser()
        subparser = parser.add_subparsers(dest="command")

        cleanup_parser = subparser.add_parser("cleanup")
        full_sync_parser = subparser.add_parser("full-sync")
        incremental_sync_parser = subparser.add_parser("incremental-sync")

        cleanup_parser.add_argument("--db-name", type=str, required=True)
        cleanup_parser.add_argument("--days-to-keep", type=int, default=3)

        full_sync_parser.add_argument("--src-db", type=str, required=True)
        full_sync_parser.add_argument("--dst-db", type=str, required=True)
        full_sync_parser.add_argument("--src-schema", type=str, required=True)
        full_sync_parser.add_argument("--src-table", type=str, required=True)
        full_sync_parser.add_argument("--pk", nargs="+")

        incremental_sync_parser.add_argument("--src-db", type=str, required=True)
        incremental_sync_parser.add_argument("--dst-db", type=str, required=True)
        incremental_sync_parser.add_argument("--src-schema", type=str, required=True)
        incremental_sync_parser.add_argument("--src-table", type=str, required=True)
        incremental_sync_parser.add_argument("--pk", nargs="+")
        incremental_strategy_options = incremental_sync_parser.add_mutually_exclusive_group()
        incremental_strategy_options.add_argument("--compare", nargs="+")
        incremental_strategy_options.add_argument("--increasing", nargs="+")
        incremental_sync_parser.add_argument("--skip-if-row-counts-match", action="store_true")

        # print(parser.parse_args("cleanup --db-name dw --days-to-keep 5".split()))
        # print(parser.parse_args("full-sync --src-db src --dst-db dw --src-schema sales --src-table customer --pk first_name last_name".split()))
        # print(parser.parse_args("incremental-sync --src-db src --dst-db dw --src-schema sales --src-table customer --pk first_name last_name --compare first_name last_name --skip-if-row-counts-match".split()))
        # print(parser.parse_args("incremental-sync --src-db src --dst-db dw --src-schema sales --src-table customer --pk first_name last_name --increasing date_added date_updated --skip-if-row-counts-match".split()))

        args = parser.parse_args(sys.argv[1:])
        if args.command == "cleanup":
            cleanup(
                ds_name=args.db_name,
                log_folder=logging_folder,
                days_logs_to_keep=args.days_to_keep,
            )
        elif args.command == "full-sync":
            assert args.pk, "--pk is required."

            sync(
                src_db_name=args.src_db,
                dst_db_name=args.dst_db,
                src_schema_name=args.src_schmea,
                src_table_name=args.src_table,
                pk=args.pk,
                incremental=False,
                compare_cols=None,
                increasing_cols=None,
                skip_if_row_counts_match=False,
                log_folder=logging_folder,
            )
        elif args.command == "incremental-sync":
            assert args.pk, "--pk is required."

            if args.compare:
                compare_cols: set[str] | None = set(args.compare)
            else:
                compare_cols = None

            if args.increasing:
                increasing_cols: set[str] | None = set(args.increasing)
            else:
                increasing_cols = None

            assert compare_cols is not None or increasing_cols is not None, \
                "Either --compare or --increasing is required, but neither were provided."

            sync(
                src_db_name=args.src_db,
                dst_db_name=args.dst_db,
                src_schema_name=args.src_schmea,
                src_table_name=args.src_table,
                pk=args.pk,
                incremental=True,
                compare_cols=args.compare,
                increasing_cols=args.increasing,
                skip_if_row_counts_match=args.skip_if_row_counts_match,
                log_folder=logging_folder,
            )
    except Exception as e:
        loguru.logger.exception(e)
        raise

from __future__ import annotations

import argparse
import datetime
import pathlib
import sys
import traceback

import loguru

from src import adapter, service


def check(
    src_db_name: str,
    src_schema_name: str | None,
    src_table_name: str,
    dst_db_name: str,
    dst_schema_name: str | None,
    dst_table_name: str,
    pk: list[str],
    log_folder: pathlib.Path,
    batch_ts: datetime.datetime,
) -> None:
    try:
        if src_schema_name:
            prefix = f"check.{src_db_name}.{src_schema_name}.{src_table_name}."
        else:
            prefix = f"check.{src_db_name}.{src_table_name}."

        loguru.logger.add(log_folder / f"{prefix}.info.log", rotation="5 MB", retention="7 days", level="INFO")
        loguru.logger.add(log_folder / f"{prefix}.error.log", rotation="5 MB", retention="7 days", level="ERROR")

        loguru.logger.info(
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
                )
                result = service.check(
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
        loguru.logger.error(
            f"An error occurred while running check({src_db_name=!r}, {src_schema_name=!r}, "
            f"{src_table_name=!r}, {dst_db_name=!r}, {dst_schema_name=!r}): {e2!s}\n{traceback.format_exc()}"
        )
        raise


def cleanup(*, db_name: str, log_folder: pathlib.Path, days_logs_to_keep: int = 3) -> None:
    try:
        loguru.logger.add(log_folder / f"cleanup.{db_name}.info.log", rotation="5 MB", retention="7 days", level="INFO")
        loguru.logger.add(log_folder / f"cleanup.{db_name}.error.log", rotation="5 MB", retention="7 days", level="ERROR")

        config_file = adapter.fs.get_config_path()

        dst_api = adapter.config.get_api(config_file=config_file, name=db_name)
        dst_connection_str = adapter.config.get_connection_str(config_file=config_file, name=db_name)

        logger_cursor_provider = adapter.cursor_provider.create(api=dst_api, connection_str=dst_connection_str)

        days_logs_to_keep = adapter.config.get_days_logs_to_keep(config_file=config_file)
        log = adapter.log.create(api=dst_api, cursor_provider=logger_cursor_provider)

        try:
            log.delete_old_logs(days_to_keep=days_logs_to_keep)
        except Exception as e1:
            log.error(
                f"An error occurred while running service.cleanup({db_name=!r}, log_folder=..., {days_logs_to_keep=!r}): "
                f"{e1!s}\n{traceback.format_exc()}"
            )
            raise
    except Exception as e2:
        loguru.logger.error(
            f"An error occurred while running cleanup({db_name=!r}, log_folder=..., {days_logs_to_keep=!r}): "
            f"{e2!s}\n{traceback.format_exc()}")
        raise


def inspect(
    *,
    src_db_name: str,
    src_schema_name: str | None,
    src_table_name: str,
    cache_db_name: str,
    pk: list[str],
    log_folder: pathlib.Path,
) -> None:
    try:
        if src_schema_name:
            prefix = f"inspect.{src_db_name}.{src_schema_name}.{src_table_name}."
        else:
            prefix = f"inspect.{src_db_name}.{src_table_name}."

        loguru.logger.add(log_folder / f"{prefix}.info.log", rotation="5 MB", retention="7 days", level="INFO")
        loguru.logger.add(log_folder / f"{prefix}.error.log", rotation="5 MB", retention="7 days", level="ERROR")

        loguru.logger.info(
            f"Starting inspect using the following parameters:\n  {src_db_name=!r}\n  {src_schema_name=!r}\n  "
            f"{src_table_name=!r}\n  {cache_db_name=!r}\n  {pk=!r}\n  log_folder={log_folder.resolve()!s}\n..."
        )

        config_file = adapter.fs.get_config_path()

        src_api = adapter.config.get_api(config_file=config_file, name=src_db_name)
        src_connection_str = adapter.config.get_connection_str(config_file=config_file, name=src_db_name)
        src_cursor_provider = adapter.cursor_provider.create(api=src_api, connection_str=src_connection_str)

        cache_api = adapter.config.get_api(config_file=config_file, name=cache_db_name)
        cache_connection_str = adapter.config.get_connection_str(config_file=config_file, name=cache_db_name)
        cache_cursor_provider = adapter.cursor_provider.create(api=cache_api, connection_str=cache_connection_str)

        service.inspect(
            src_api=src_api,
            cache_api=cache_api,
            src_cursor_provider=src_cursor_provider,
            cache_cursor_provider=cache_cursor_provider,
            src_db_name=src_db_name,
            src_schema_name=src_schema_name,
            src_table_name=src_table_name,
            pk=tuple(pk),
        )
    except Exception as ie:
        loguru.logger.error(
            f"An error occurred while running inspect({src_db_name=!r}, {src_schema_name=!r}, "
            f"{src_table_name=!r}, {cache_db_name=!r}, {pk=!r}, log_folder='"
            f"{log_folder.resolve()!s}'): {ie!s}\n{traceback.format_exc()}"
        )
        raise


def sync(
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
) -> None:
    try:
        if src_schema_name:
            prefix = f"sync.{src_db_name}.{src_schema_name}.{src_table_name}."
        else:
            prefix = f"sync.{src_db_name}.{src_table_name}."

        loguru.logger.add(log_folder / f"{prefix}.info.log", rotation="5 MB", retention="7 days", level="INFO")
        loguru.logger.add(log_folder / f"{prefix}.error.log", rotation="5 MB", retention="7 days", level="ERROR")

        loguru.logger.info(
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
                )

                batch_size = adapter.config.get_batch_size(config_file=config_file)

                result = service.sync(
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
                    loguru.logger.error(result.error_message or "No error message was provided.")
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
        loguru.logger.error(
            f"An error occurred while running sync({src_db_name=!r}, {src_schema_name=!r}, "
            f"{src_table_name=!r}, {dst_db_name=!r}, {dst_schema_name=!r}, {incremental=}): "
            f"{e2!s}\n{traceback.format_exc()}"
        )
        raise


if __name__ == '__main__':
    batch_ts = datetime.datetime.utcnow()

    logging_folder = adapter.fs.get_log_folder()

    loguru.logger.remove()
    loguru.logger.add(sys.stderr, level="INFO")
    loguru.logger.add(logging_folder / f"error.log", rotation="5 MB", retention="7 days", level="ERROR")

    try:
        parser = argparse.ArgumentParser()
        subparser = parser.add_subparsers(dest="command")

        check_parser = subparser.add_parser("check")
        cleanup_parser = subparser.add_parser("cleanup")
        full_sync_parser = subparser.add_parser("full-sync")
        inspect_parser = subparser.add_parser("inspect")
        incremental_sync_parser = subparser.add_parser("incremental-sync")

        check_parser.add_argument("--src-db", type=str, required=True)
        check_parser.add_argument("--src-schema", type=str, required=True)
        check_parser.add_argument("--src-table", type=str, required=True)
        check_parser.add_argument("--dst-db", type=str, required=True)
        check_parser.add_argument("--dst-schema", type=str, required=True)
        check_parser.add_argument("--dst-table", type=str, required=True)
        check_parser.add_argument("--pk", nargs="+")

        cleanup_parser.add_argument("--db", type=str, required=True)
        cleanup_parser.add_argument("--days-to-keep", type=int, default=3)

        full_sync_parser.add_argument("--src-db", type=str, required=True)
        full_sync_parser.add_argument("--src-schema", type=str, required=True)
        full_sync_parser.add_argument("--src-table", type=str, required=True)
        full_sync_parser.add_argument("--dst-db", type=str, required=True)
        full_sync_parser.add_argument("--dst-schema", type=str, required=True)
        full_sync_parser.add_argument("--dst-table", type=str, required=True)
        full_sync_parser.add_argument("--pk", nargs="+")
        full_sync_parser.add_argument("--recreate", action="store_true")
        full_sync_parser.add_argument("--track-history", action="store_true")

        incremental_sync_parser.add_argument("--src-db", type=str, required=True)
        incremental_sync_parser.add_argument("--src-schema", type=str, required=True)
        incremental_sync_parser.add_argument("--src-table", type=str, required=True)
        incremental_sync_parser.add_argument("--dst-db", type=str, required=True)
        incremental_sync_parser.add_argument("--dst-schema", type=str, required=True)
        incremental_sync_parser.add_argument("--dst-table", type=str, required=True)
        incremental_sync_parser.add_argument("--pk", nargs="+")
        incremental_strategy_options = incremental_sync_parser.add_mutually_exclusive_group()
        incremental_strategy_options.add_argument("--compare", nargs="+")
        incremental_strategy_options.add_argument("--increasing", nargs="+")
        incremental_sync_parser.add_argument("--skip-if-row-counts-match", action="store_true")
        incremental_sync_parser.add_argument("--track-history", action="store_true")

        inspect_parser.add_argument("--src-db", type=str, required=True)
        inspect_parser.add_argument("--src-schema", type=str, required=True)
        inspect_parser.add_argument("--src-table", type=str, required=True)
        inspect_parser.add_argument("--cache-db", type=str, required=True)
        inspect_parser.add_argument("--pk", nargs="+")

        # print(parser.parse_args("cleanup --db-name dw --days-to-keep 5".split()))
        # print(parser.parse_args("full-sync --src-db src --dst-db dw --src-schema sales --src-table customer --pk first_name last_name".split()))
        # print(parser.parse_args("incremental-sync --src-db src --dst-db dw --src-schema sales --src-table customer --pk first_name last_name --compare first_name last_name --skip-if-row-counts-match".split()))
        # print(parser.parse_args("incremental-sync --src-db src --dst-db dw --src-schema sales --src-table customer --pk first_name last_name --increasing date_added date_updated --skip-if-row-counts-match".split()))

        args = parser.parse_args(sys.argv[1:])
        if args.command == "check":
            assert args.src_db, "--src-db is required."
            assert args.src_table, "--src-table is required."
            assert args.dst_db, "--dst-db is required."
            assert args.dst_table, "--dst-table is required."
            assert args.pk, "--pk is required."

            check(
                src_db_name=args.src_db,
                src_schema_name=args.src_schema,
                src_table_name=args.src_table,
                dst_db_name=args.dst_db,
                dst_schema_name=args.dst_schema,
                dst_table_name=args.dst_table,
                pk=args.pk,
                log_folder=logging_folder,
                batch_ts=batch_ts,
            )
        elif args.command == "cleanup":
            assert args.db, "--db is required"

            cleanup(
                db_name=args.db,
                log_folder=logging_folder,
                days_logs_to_keep=args.days_to_keep,
            )
        elif args.command == "full-sync":
            assert args.src_db, "--src-db is required."
            assert args.src_table, "--src-table is required."
            assert args.dst_db, "--dst-db is required."
            assert args.dst_schema, "--dst-schema is required."
            assert args.dst_table, "--dst-table is required."
            assert args.pk, "--pk is required."

            sync(
                src_db_name=args.src_db,
                src_schema_name=args.src_schema,
                src_table_name=args.src_table,
                dst_db_name=args.dst_db,
                dst_schema_name=args.dst_schema,
                dst_table_name=args.dst_table,
                pk=args.pk,
                incremental=False,
                compare_cols=None,
                increasing_cols=None,
                skip_if_row_counts_match=False,
                recreate=args.recreate,
                log_folder=logging_folder,
                batch_ts=batch_ts,
                track_history=args.track_history,
            )
        elif args.command == "incremental-sync":
            assert args.src_db, "--src-db is required."
            assert args.src_table, "--src-table is required."
            assert args.dst_db, "--dst-db is required."
            assert args.dst_schema, "--dst-schema is required."
            assert args.pk, "--pk is required."

            if args.compare:
                compare: set[str] | None = set(args.compare)
            else:
                compare = None

            if args.increasing:
                increasing: set[str] | None = set(args.increasing)
            else:
                increasing = None

            assert compare is not None or increasing is not None, \
                "Either --compare or --increasing is required, but neither were provided."

            sync(
                src_db_name=args.src_db,
                src_schema_name=args.src_schema,
                src_table_name=args.src_table,
                dst_db_name=args.dst_db,
                dst_schema_name=args.dst_schema,
                dst_table_name=args.dst_table,
                pk=args.pk,
                incremental=True,
                compare_cols=compare,
                increasing_cols=increasing,
                skip_if_row_counts_match=args.skip_if_row_counts_match,
                recreate=False,
                log_folder=logging_folder,
                batch_ts=batch_ts,
                track_history=args.track_history,
            )
        elif args.command == "inspect":
            assert args.pk, "--pk is required."
            assert args.src_db, "--src-db is required."
            assert args.src_table, "--src-table is required."
            assert args.cache_db, "--cache-db is required."

            inspect(
                src_db_name=args.src_db,
                src_schema_name=args.src_schema,
                src_table_name=args.src_table,
                cache_db_name=args.cache_db,
                pk=args.pk,
                log_folder=logging_folder,
            )
    except Exception as e:
        loguru.logger.exception(e)
        sys.exit(1)

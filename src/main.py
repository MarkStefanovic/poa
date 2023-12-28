import argparse
import dataclasses
import datetime
import sys

from loguru import logger
import pydantic

from src import adapter, service, data


@pydantic.dataclasses.dataclass(frozen=True, kw_only=True)
class CheckArgs:
    src_db: str
    src_schema: str
    src_table: str
    dst_db: str
    dst_schema: str
    dst_table: str
    pk: tuple[str, ...]
    after: dict[str, datetime.date]


@pydantic.dataclasses.dataclass(frozen=True, kw_only=True)
class CleanupArgs:
    db: str


@pydantic.dataclasses.dataclass(frozen=True, kw_only=True)
class FullSyncArgs:
    src_db: str
    src_schema: str
    src_table: str
    dst_db: str
    dst_schema: str
    dst_table: str
    pk: tuple[str, ...]
    recreate: bool
    track_history: bool


@pydantic.dataclasses.dataclass(frozen=True, kw_only=True)
class IncrementalSyncArgs:
    src_db: str
    src_table: str
    dst_db: str
    dst_schema: str
    pk: tuple[str, ...]


@pydantic.dataclasses.dataclass(frozen=True, kw_only=True)
class InspectArgs:
    src_db: str
    src_table: str
    cache_db_name: str
    pk: tuple[str, ...]


def parse_args(
    args: argparse.Namespace, /
) -> CheckArgs | CleanupArgs | FullSyncArgs | IncrementalSyncArgs | InspectArgs | data.Error:
    try:
        after: dict[str, datetime.date] = {}
        if hasattr(args, "after") and args.after:
            if len(args.after) % 2 != 0:
                return data.Error.new(
                    f"If after is provided, then it must have an even number of elements, but got {args.after!r}."
                )

            after = {
                args.after[i]: datetime.datetime.strptime(args.after[i + 1], "%Y-%m-%d").date()
                for i in range(0, len(args.after), 2)
            }

        match args.command:
            case "check":
                if not args.src_db:
                    return data.Error.new("--src-db is required.")

                if not args.src_table:
                    return data.Error.new("--src-table is required.")

                if not args.dst_db:
                    return data.Error.new("--dst-db is required.")

                if not args.dst_table:
                    return data.Error.new("--dst-table is required.")

                if not args.pk:
                    return data.Error.new("--pk is required.")

                return CheckArgs(
                    src_db=args.src_db,
                    src_schema=args.src_schema,
                    src_table=args.src_table,
                    dst_db=args.dst_db,
                    dst_schema=args.dst_schema,
                    dst_table=args.dst_table,
                    pk=tuple(args.pk),
                    after=after,
                )
            case "cleanup":
                if not args.db:
                    return data.Error.new("--db is required.")

                if not isinstance(args.db, str):
                    return data.Error.new("--db must be a string.")

                return CleanupArgs(db=args.db)
            case "full-sync":
                if not args.src_db:
                    return data.Error.new("--src-db is required.")

                if not args.src_table:
                    return data.Error.new("--src-table is required.")

                if not args.dst_db:
                    return data.Error.new("--dst-db is required.")

                if not args.dst_schema:
                    return data.Error.new("--dst-schema is required.")

                if not args.dst_table:
                    return data.Error.new("--dst-table is required.")

                if not args.pk:
                    return data.Error.new("--pk is required.")

                return FullSyncArgs(
                    src_db=args.src_db,
                    src_schema=args.src_schema,
                    src_table=args.src_table,
                    dst_db=args.dst_db,
                    dst_schema=args.dst_schema,
                    dst_table=args.dst_table,
                    pk=args.pk,
                    recreate=args.recreate,
                    track_history=args.track_history,
                )
            case "incremental-sync":
                if not args.src_db:
                    return data.Error.new("--src-db is required.")

                if not args.src_table:
                    return data.Error.new("--src-table is required.")

                if not args.dst_db:
                    return data.Error.new("--dst-db is required.")

                if not args.dst_schema:
                    return data.Error.new("--dst-schema is required.")

                if not args.pk:
                    return data.Error.new("--pk is required.")

                if args.compare:
                    compare: set[str] | None = set(args.compare)
                else:
                    compare = None

                if args.increasing:
                    increasing: set[str] | None = set(args.increasing)
                else:
                    increasing = None

                if compare is None and increasing is None:
                    return data.Error.new(
                        "Either --compare or --increasing is required, but neither were provided."
                    )

                return IncrementalSyncArgs(
                    src_db=args.src_db,
                    src_table=args.src_table,
                    dst_db=args.dst_db,
                    dst_schema=args.dst_schema,
                    pk=tuple(args.pk),
                )
            case "inspect":
                assert args.pk, "--pk is required."
                assert args.src_db, "--src-db is required."
                assert args.src_table, "--src-table is required."
                assert args.cache_db, "--cache-db is required."

                return InspectArgs(
                    src_db=args.src_db,
                    src_table=args.src_table,
                    cache_db_name=args.cache_db,
                    pk=tuple(args.pk),
                )
            case _:
                return data.Error.new(f"{args.command} is invalid.")
    except Exception as e:
        return data.Error.new(f"An error occurred while parsing command line args: {e!s}")


if __name__ == "__main__":
    try:
        batch_ts = datetime.datetime.utcnow()

        log_folder = adapter.fs.get_log_folder()
        if isinstance(log_folder, data.Error):
            logger.error(f"An error occurred while looking up log folder: {log_folder!s}")
            sys.exit(1)

        if not getattr(sys, "frozen", False):
            logger.remove()
            logger.add(sys.stderr, level="INFO")

        logger.add(log_folder / "error.log", rotation="5 MB", retention="7 days", level="ERROR")

        config_file_path = adapter.fs.get_config_path()
        if isinstance(config_file_path, data.Error):
            logger.error(
                f"An error occurred while looking up config_file_path: {config_file_path!s}"
            )
            sys.exit(1)

        config = adapter.config.load(config_file=config_file_path)
        if isinstance(config, data.Error):
            logger.error(f"An error occurred while loading config file: {config!s}")
            sys.exit(1)

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
        check_parser.add_argument("--pk", nargs="+", type=str)
        check_parser.add_argument("--after", nargs="+", type=str)

        cleanup_parser.add_argument("--db", type=str, required=True)
        cleanup_parser.add_argument("--days-to-keep", type=int, default=3)

        full_sync_parser.add_argument("--src-db", type=str, required=True)
        full_sync_parser.add_argument("--src-schema", type=str, required=True)
        full_sync_parser.add_argument("--src-table", type=str, required=True)
        full_sync_parser.add_argument("--dst-db", type=str, required=True)
        full_sync_parser.add_argument("--dst-schema", type=str, required=True)
        full_sync_parser.add_argument("--dst-table", type=str, required=True)
        full_sync_parser.add_argument("--pk", nargs="+", type=str)
        full_sync_parser.add_argument("--after", nargs="+", type=str)
        full_sync_parser.add_argument("--recreate", action="store_true")
        full_sync_parser.add_argument("--track-history", action="store_true")

        incremental_sync_parser.add_argument("--src-db", type=str, required=True)
        incremental_sync_parser.add_argument("--src-schema", type=str, required=True)
        incremental_sync_parser.add_argument("--src-table", type=str, required=True)
        incremental_sync_parser.add_argument("--dst-db", type=str, required=True)
        incremental_sync_parser.add_argument("--dst-schema", type=str, required=True)
        incremental_sync_parser.add_argument("--dst-table", type=str, required=True)
        incremental_sync_parser.add_argument("--pk", nargs="+", type=str)
        incremental_strategy_options = incremental_sync_parser.add_mutually_exclusive_group()
        incremental_strategy_options.add_argument("--compare", nargs="+", type=str)
        incremental_strategy_options.add_argument("--increasing", nargs="+", type=str)
        incremental_sync_parser.add_argument("--skip-if-row-counts-match", action="store_true")
        incremental_sync_parser.add_argument("--track-history", action="store_true")
        incremental_sync_parser.add_argument("--after", nargs="+", type=str)

        inspect_parser.add_argument("--src-db", type=str, required=True)
        inspect_parser.add_argument("--src-schema", type=str, required=True)
        inspect_parser.add_argument("--src-table", type=str, required=True)
        inspect_parser.add_argument("--cache-db", type=str, required=True)
        inspect_parser.add_argument("--pk", nargs="+")

        args = parser.parse_args(sys.argv[1:])

        match parse_args(args):
            case CheckArgs(
                src_db=src_db,
                src_schema=src_schema,
                src_table=src_table,
                dst_db=dst_db,
                dst_schema=dst_schema,
                dst_table=dst_table,
                pk=pk,
                after=after,
            ):
                src_config = config.db(name=args.src_db)
                if src_config is None:
                    logger.error(
                        f"--src-db was {args.src_db}, but could not find database entry by that name in the config file."
                    )
                    sys.exit(1)

                dst_config = config.db(name=args.dst_db)
                if dst_config is None:
                    logger.error(
                        f"--dst-db was {args.dst_db}, but could not find database entry by that name in the config file."
                    )
                    sys.exit(1)

                check_result = service.check(
                    src_db_config=src_config,
                    src_schema_name=src_schema,
                    src_table_name=src_table,
                    dst_db_config=dst_config,
                    dst_schema_name=dst_schema,
                    dst_table_name=dst_table,
                    pk=pk,
                    batch_ts=batch_ts,
                    after=after,
                )
            case CleanupArgs(db=db):
                db_config = config.db(name=db)
                if db_config is None:
                    logger.error(
                        f"--dst-db was {db}, but could not find database entry by that name in the config file."
                    )
                    sys.exit(1)

                cleanup_result = service.cleanup(
                    dst_config=db_config,
                    days_logs_to_keep=args.days_to_keep,
                )
                if isinstance(cleanup_result, data.Error):
                    logger.error(
                        f"An error occurred while running cleanup service for db, {db}: {cleanup_result!s}"
                    )
                    sys.exit(1)
            case FullSyncArgs(
                src_db=src_db,
                src_schema=src_schema,
                src_table=src_table,
                dst_db=dst_db,
                dst_schema=dst_schema,
                dst_table=dst_table,
                pk=pk,
                recreate=recreate,
                track_history=track_history,
            ):
                full_sync_result = service.sync(
                    src_db_name=src_db,
                    src_schema_name=src_schema,
                    src_table_name=src_table,
                    dst_db_name=dst_db,
                    dst_schema_name=dst_schema,
                    dst_table_name=dst_table,
                    pk=pk,
                    incremental=incremental,
                    compare_cols=compare_cols,
                    increasing_cols=increasing_cols,
                    skip_if_row_counts_match=skip_if_row_counts_match,
                    recreate=recreate,
                    log_folder=log_folder,
                    batch_ts=batch_ts,
                    track_history=track_history,
                    after=after,
                )
                if isinstance(full_sync_result, data.Error):
                    logger.error(f"An error occurred while running full sync: {full_sync_result}")
                    sys.exit(1)

            case IncrementalSyncArgs(
                src_db=src_db,
                src_table=src_table,
                dst_db=dst_db,
                dst_schema=dst_schema,
                pk=pk,
            ):
                service.sync(
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
                    log_folder=log_folder,
                    batch_ts=batch_ts,
                    track_history=args.track_history,
                    after=after,
                )
            case InspectArgs(
                src_db=src_db,
                src_table=src_table,
                cache_db_name=cache_db_name,
                pk=pk,
            ):
                inspect_result = service.inspect(
                    src_db_name=src_db,
                    src_schema_name=src_schema,
                    src_table_name=src_table,
                    cache_db_name=cache_db_name,
                    pk=pk,
                    log_folder=log_folder,
                )
                if isinstance(inspect_result, data.Error):
                    logger.error(
                        f"An error occurred while inspecting {src_db}.{src_schema}.{src_table}: {inspect_result!s}."
                    )
                    sys.exit(1)

            case data.Error(
                file=_,
                fn=_,
                fn_args=_,
                error_message=error_message,
            ):
                logger.error(error_message)
                sys.exit(1)
    except Exception as e:
        logger.exception(e)
        sys.exit(1)

import argparse
import datetime
import sys

import pydantic
from loguru import logger

from src import adapter, service, data


@pydantic.dataclasses.dataclass(frozen=True, kw_only=True)
class CheckArgs:
    src_db: str
    src_schema: str | None
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
    src_schema: str | None
    src_table: str
    dst_db: str
    dst_schema: str
    dst_table: str
    pk: tuple[str, ...]
    recreate: bool
    track_history: bool
    after: dict[str, datetime.date]


@pydantic.dataclasses.dataclass(frozen=True, kw_only=True)
class IncrementalSyncArgs:
    src_db: str
    src_schema: str | None
    src_table: str
    dst_db: str
    dst_schema: str
    pk: tuple[str, ...]


@pydantic.dataclasses.dataclass(frozen=True, kw_only=True)
class InspectArgs:
    src_db: str
    src_schema: str | None
    src_table: str
    cache_db_name: str
    pk: tuple[str, ...]


# def parse_args(
#         args: argparse.Namespace, /
# ) -> CheckArgs | CleanupArgs | FullSyncArgs | IncrementalSyncArgs | InspectArgs | data.Error:
#     try:
#         after: dict[str, datetime.date] = {}
#         if hasattr(args, "after") and args.after:
#             if len(args.after) % 2 != 0:
#                 return data.Error.new(
#                     f"If after is provided, then it must have an even number of elements, but got {args.after!r}."
#                 )
#
#             after = {
#                 args.after[i]: datetime.datetime.strptime(args.after[i + 1], "%Y-%m-%d").date()
#                 for i in range(0, len(args.after), 2)
#             }
#
#         match args.command:
#             case "check":
#                 if not args.src_db:
#                     return data.Error.new("--src-db is required.")
#
#                 if not args.src_table:
#                     return data.Error.new("--src-table is required.")
#
#                 if not args.dst_db:
#                     return data.Error.new("--dst-db is required.")
#
#                 if not args.dst_table:
#                     return data.Error.new("--dst-table is required.")
#
#                 if not args.pk:
#                     return data.Error.new("--pk is required.")
#
#                 return CheckArgs(
#                     src_db=args.src_db,
#                     src_schema=args.src_schema,
#                     src_table=args.src_table,
#                     dst_db=args.dst_db,
#                     dst_schema=args.dst_schema,
#                     dst_table=args.dst_table,
#                     pk=tuple(args.pk),
#                     after=after,
#                 )
#             case "cleanup":
#                 if not args.db:
#                     return data.Error.new("--db is required.")
#
#                 if not isinstance(args.db, str):
#                     return data.Error.new("--db must be a string.")
#
#                 return CleanupArgs(db=args.db)
#             case "full-sync":
#                 if not args.src_db:
#                     return data.Error.new("--src-db is required.")
#
#                 if not args.src_table:
#                     return data.Error.new("--src-table is required.")
#
#                 if not args.dst_db:
#                     return data.Error.new("--dst-db is required.")
#
#                 if not args.dst_schema:
#                     return data.Error.new("--dst-schema is required.")
#
#                 if not args.dst_table:
#                     return data.Error.new("--dst-table is required.")
#
#                 if not args.pk:
#                     return data.Error.new("--pk is required.")
#
#                 return FullSyncArgs(
#                     src_db=args.src_db,
#                     src_schema=args.src_schema,
#                     src_table=args.src_table,
#                     dst_db=args.dst_db,
#                     dst_schema=args.dst_schema,
#                     dst_table=args.dst_table,
#                     pk=args.pk,
#                     recreate=args.recreate,
#                     track_history=args.track_history,
#                     after=after,
#                 )
#             case "incremental-sync":
#                 if not args.src_db:
#                     return data.Error.new("--src-db is required.")
#
#                 if not args.src_table:
#                     return data.Error.new("--src-table is required.")
#
#                 if not args.dst_db:
#                     return data.Error.new("--dst-db is required.")
#
#                 if not args.dst_schema:
#                     return data.Error.new("--dst-schema is required.")
#
#                 if not args.pk:
#                     return data.Error.new("--pk is required.")
#
#                 if args.compare:
#                     compare: set[str] | None = set(args.compare)
#                 else:
#                     compare = None
#
#                 if args.increasing:
#                     increasing: set[str] | None = set(args.increasing)
#                 else:
#                     increasing = None
#
#                 if compare is None and increasing is None:
#                     return data.Error.new(
#                         "Either --compare or --increasing is required, but neither were provided."
#                     )
#
#                 return IncrementalSyncArgs(
#                     src_db=args.src_db,
#                     src_schema=args.src_schema,
#                     src_table=args.src_table,
#                     dst_db=args.dst_db,
#                     dst_schema=args.dst_schema,
#                     pk=tuple(args.pk),
#                 )
#             case "inspect":
#                 assert args.pk, "--pk is required."
#                 assert args.src_db, "--src-db is required."
#                 assert args.src_table, "--src-table is required."
#                 assert args.cache_db, "--cache-db is required."
#
#                 return InspectArgs(
#                     src_db=args.src_db,
#                     src_schema=args.src_schema
#                 src_table = args.src_table,
#                 cache_db_name = args.cache_db,
#                 pk = tuple(args.pk),
#                 )
#                 case
#                 _:
#                 return data.Error.new(f"{args.command} is invalid.")
#     except Exception as e:
#         return data.Error.new(f"An error occurred while parsing command line args: {e!s}")


def _check(*, check_args: CheckArgs, config: data.Config) -> None | data.Error:
    try:
        src_db_config = config.db(check_args.src_db)
        if src_db_config is None:
            return data.Error.new(
                f"--src-db was {check_args.src_db}, but could not find database entry by that "
                f"name in the config file.",
                inspect_args=check_args,
            )

        dst_db_config = config.db(check_args.dst_db)
        if dst_db_config is None:
            return data.Error.new(
                f"--dst-db was {check_args.dst_db}, but could not find database entry by that "
                f"name in the config file.",
                inspect_args=check_args,
            )

        return service.check(
            src_db_config=src_db_config,
            src_schema_name=check_args.src_schema,
            src_table_name=check_args.src_table,
            dst_db_config=dst_db_config,
            dst_schema_name=check_args.dst_schema,
            dst_table_name=check_args.dst_table,
            after=check_args.after,
            pk_cols=check_args.pk,
        )
    except Exception as e:
        return data.Error.new(str(e), args=check_args, config=config)


def _inspect(
    *,
    inspect_args: InspectArgs,
    config: data.Config,
) -> None | data.Error:
    # noinspection PyBroadException
    try:
        db_config = config.db(inspect_args.src_db)
        if db_config is None:
            return data.Error.new(
                f"--db was {inspect_args.src_db}, but could not find database entry by that "
                f"name in the config file.",
                inspect_args=inspect_args,
            )

        cache_db_config = config.db(inspect_args.cache_db_name)
        if cache_db_config is None:
            return data.Error.new(
                f"--cache-db was {inspect_args.cache_db_name}, but could not find database entry by that name "
                f"in the config file."
            )

        table = service.inspect(
            src_config=db_config,
            src_schema_name=inspect_args.src_schema,
            src_table_name=inspect_args.src_table,
            dst_config=cache_db_config,
            pk=tuple(inspect_args.pk),
        )
        if isinstance(table, data.Error):
            return table

        print(table)

        return None
    except Exception as run_error:
        return data.Error.new(str(run_error), args=inspect_args)


def _parse_check_args(args: argparse.Namespace, /) -> CheckArgs | data.Error:
    try:
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

        after: dict[str, datetime.date] = {}
        if hasattr(args, "after") and args.after:
            if len(args.after) % 2 != 0:
                return data.Error.new(
                    f"If after is provided, then it must have an even number of elements, but got {args.after!r}.",
                    check_args=args,
                )

            after = {
                args.after[i]: datetime.datetime.strptime(args.after[i + 1], "%Y-%m-%d").date()
                for i in range(0, len(args.after), 2)
            }

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
    except Exception as check_error:
        return data.Error.new(str(check_error), check_args=args)


def _parse_inspect_args(args: argparse.Namespace, /) -> InspectArgs | data.Error:
    try:
        if not args.pk:
            return data.Error.new("--pk is required.", inspect_args=args)

        if not args.db:
            return data.Error.new("--db is required.", inspect_args=args)

        if not args.schema:
            return data.Error.new("--schema is required.", inspect_args=args)

        if not args.table:
            return data.Error.new("--table is required.", inspect_args=args)

        if not args.cache_db:
            return data.Error.new("--cache-db is required.", inspect_args=args)

        return InspectArgs(
            src_db=args.src_db,
            src_schema=args.schema,
            src_table=args.table,
            cache_db_name=args.cache_db,
            pk=tuple(args.pk),
        )
    except Exception as inspect_error:
        return data.Error.new(str(inspect_error), inspect_args=args)


def _run(args: argparse.Namespace, /) -> None | data.Error:
    match cmd := args.command:
        case "check":
            check_args = _parse_check_args(args)
            if isinstance(check_args, data.Error):
                logger.error(str(check_args))
                sys.exit(1)

            return _check(check_args=check_args, config=cfg)
        case "inspect":
            inspect_args = _parse_inspect_args(args)
            if isinstance(inspect_args, data.Error):
                logger.error(str(inspect_args))
                sys.exit(1)

            return _inspect(inspect_args=inspect_args, config=cfg)
        case _:
            raise data.Error.new(f"Unrecognized command, {cmd!r}.", args=args)


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

        cfg = adapter.config.load(config_file=config_file_path)
        if isinstance(cfg, data.Error):
            logger.error(f"An error occurred while loading config file: {cfg!s}")
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

        inspect_parser.add_argument("--db", type=str, required=True)
        inspect_parser.add_argument("--schema", type=str, required=True)
        inspect_parser.add_argument("--table", type=str, required=True)
        inspect_parser.add_argument("--cache-db", type=str, required=True)
        inspect_parser.add_argument("--pk", nargs="+")

        result = _run(parser.parse_args(sys.argv[1:]))
        if isinstance(result, data.Error):
            logger.error(str(result))
            sys.exit(1)

        logger.info("Done.")
    except Exception as e:
        logger.exception(e)
        sys.exit(1)

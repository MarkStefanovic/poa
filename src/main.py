from __future__ import annotations

import argparse
import datetime
import sys

from loguru import logger

from src import adapter, service


if __name__ == '__main__':
    batch_ts = datetime.datetime.utcnow()

    logging_folder = adapter.fs.get_log_folder()

    if not getattr(sys, "frozen", False):
        logger.remove()
        logger.add(sys.stderr, level="INFO")

    logger.add(logging_folder / f"error.log", rotation="5 MB", retention="7 days", level="ERROR")

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

        after: dict[str, datetime.date] = {}
        if hasattr(args, "after") and args.after:
            assert len(args.after) % 2 == 0, \
                f"If after is provided, then it must have an even number of elements, but got {args.after!r}."

            after = {
                args.after[i]: datetime.datetime.strptime(args.after[i + 1], "%Y-%m-%d").date()
                for i in range(0, len(args.after), 2)
            }

        if args.command == "check":
            assert args.src_db, "--src-db is required."
            assert args.src_table, "--src-table is required."
            assert args.dst_db, "--dst-db is required."
            assert args.dst_table, "--dst-table is required."
            assert args.pk, "--pk is required."

            service.check(
                src_db_name=args.src_db,
                src_schema_name=args.src_schema,
                src_table_name=args.src_table,
                dst_db_name=args.dst_db,
                dst_schema_name=args.dst_schema,
                dst_table_name=args.dst_table,
                pk=args.pk,
                log_folder=logging_folder,
                batch_ts=batch_ts,
                after=after,
            )
        elif args.command == "cleanup":
            assert args.db, "--db is required"

            service.cleanup(
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

            service.sync(
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
                after=after,
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
                log_folder=logging_folder,
                batch_ts=batch_ts,
                track_history=args.track_history,
                after=after,
            )
        elif args.command == "inspect":
            assert args.pk, "--pk is required."
            assert args.src_db, "--src-db is required."
            assert args.src_table, "--src-table is required."
            assert args.cache_db, "--cache-db is required."

            service.inspect(
                src_db_name=args.src_db,
                src_schema_name=args.src_schema,
                src_table_name=args.src_table,
                cache_db_name=args.cache_db,
                pk=args.pk,
                log_folder=logging_folder,
            )
    except Exception as e:
        logger.exception(e)
        sys.exit(1)

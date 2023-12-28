import datetime

import psycopg

from src import data
from src.adapter.ds.dst_ds.pg import PgDstDs

__all__ = ("create",)


def create(
    *,
    api: data.API,
    cur: psycopg.Cursor,
    dst_db_name: str,
    dst_schema_name: str | None,
    dst_table_name: str,
    src_table: data.Table,
    batch_ts: datetime.datetime,
    after: dict[str, datetime.date],
) -> data.DstDs | data.Error:
    try:
        if api == data.API.PSYCOPG:
            return PgDstDs(
                cur=cur,
                dst_db_name=dst_db_name,
                dst_schema_name=dst_schema_name,
                dst_table_name=dst_table_name,
                src_table=src_table,
                batch_ts=batch_ts,
                after=after,
            )

        return data.Error.new(
            f"The api specified, {api!s}, does not have an DstDs implementation.",
            dst_db_name=dst_db_name,
            dst_schema_name=dst_schema_name,
            dst_table_name=dst_table_name,
            src_table=src_table,
            batch_ts=batch_ts,
            after=tuple(after.items()),
        )
    except Exception as e:
        return data.Error.new(
            str(e),
            dst_db_name=dst_db_name,
            dst_schema_name=dst_schema_name,
            dst_table_name=dst_table_name,
            src_table=src_table,
            batch_ts=batch_ts,
            after=tuple(after.items()),
        )

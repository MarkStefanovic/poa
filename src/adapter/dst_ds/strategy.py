from __future__ import annotations

import typing

from psycopg2.extras import RealDictCursor

from src import data
from src.adapter.dst_ds.pg import PgDstDs

__all__ = ("create",)


def create(
    *,
    api: data.API,
    cur: typing.Any,
    dst_db_name: str,
    dst_schema_name: str | None,
    dst_table_name: str,
    src_table: data.Table,
) -> data.DstDs:
    if api == data.API.PSYCOPG2:
        return PgDstDs(
            cur=typing.cast(RealDictCursor, cur),
            dst_db_name=dst_db_name,
            dst_schema_name=dst_schema_name,
            dst_table_name=dst_table_name,
            src_table=src_table,
        )

    raise NotImplementedError(f"The api specified, {api!s}, does not have an DstDs implementation.")

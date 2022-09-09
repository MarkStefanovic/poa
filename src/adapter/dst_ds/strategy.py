import typing

from psycopg2.extras import RealDictCursor

from src import data
from src.adapter.dst_ds.pg import PgDstDs

__all__ = ("create",)


def create(*, api: data.API, cur: typing.Any, table: data.Table) -> data.DstDs:
    if api == data.API.PSYCOPG2:
        return PgDstDs(cur=typing.cast(RealDictCursor, cur), table=table)

    raise NotImplementedError(f"The api specified, {api!s}, does not have an AbstractDstTable implementation.")

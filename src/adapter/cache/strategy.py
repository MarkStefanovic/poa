import typing

from psycopg2.extras import RealDictCursor

from src import data
from src.adapter.cache.pg import PgCache

__all__ = ("create",)


def create(*, api: data.API, cur: typing.Any) -> data.Cache:
    if api == data.API.PSYCOPG2:
        return PgCache(cur=typing.cast(RealDictCursor, cur))

    raise NotImplementedError(f"The api specified, {api!s}, does not have an Cache implementation.")

import typing

from src import data
from src.adapter.cache.pg import PgCache

__all__ = ("create",)


def create(*, api: data.API, cur: typing.Any, schema_name: str | None, table_name: str) -> data.Cache:
    if api == data.API.PSYCOPG2:
        return PgCache(cur=cur, schema_name=schema_name, table_name=table_name)
    raise NotImplementedError(f"Cache is not implemented for the {api!s} API.")

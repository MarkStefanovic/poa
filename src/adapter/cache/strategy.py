import typing

import psycopg

from src import data
from src.adapter.cache.pg import PgCache

__all__ = ("create",)


def create(*, cur: data.Cursor, api: data.API) -> data.Cache | data.Error:
    if api == data.API.PSYCOPG:
        if not isinstance(cur, psycopg.Cursor):
            return data.Error.new(
                f"The api specified was {api}, but the cursor provided was of type, {type(cur)}.",
                api=api,
            )
        return typing.cast(data.Cache[data.Cursor], PgCache(cur=cur))

    raise NotImplementedError(f"The api specified, {api!s}, does not have an Cache implementation.")

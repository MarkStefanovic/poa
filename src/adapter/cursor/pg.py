import typing

import psycopg

from src import data
from src.adapter.cursor import shared

__all__ = ("PgCursor",)


class PgCursor(data.Cursor):
    def __init__(self, *, cursor: psycopg.Cursor):
        self._cursor = cursor

    def execute(
        self,
        *,
        sql: str,
        params: typing.Iterable[typing.Hashable] | None,
    ) -> None | data.Error:
        return shared.execute(
            cur=self._cursor,
            sql=sql,
            params=params,
        )

    def execute_many(
        self,
        *,
        sql: str,
        params: typing.Iterable[typing.Iterable[typing.Hashable]] | None,
    ) -> None | data.Error:
        return shared.execute_many(
            cur=self._cursor,
            sql=sql,
            params=params,
        )

    def fetch_one(
        self,
        *,
        sql: str,
        params: typing.Iterable[typing.Hashable] | None,
    ) -> data.Row | None | data.Error:
        return shared.fetch_one(
            cur=self._cursor,
            sql=sql,
            params=params,
        )

    def fetch_all(
        self,
        *,
        sql: str,
        params: typing.Iterable[typing.Hashable] | None,
    ) -> tuple[data.Row, ...] | data.Error:
        return shared.fetch_all(
            cur=self._cursor,
            sql=sql,
            params=params,
        )

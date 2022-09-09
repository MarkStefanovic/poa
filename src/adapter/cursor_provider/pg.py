import typing

import psycopg2
from psycopg2.extras import RealDictCursor

from src import data

__all__ = ("PgCursorProvider",)


class PgCursorProvider(data.CursorProvider[RealDictCursor]):
    def __init__(self, *, connection_str: str):
        self._connection_str = connection_str

    def open(self) -> typing.Generator[data.CursorType, None, None]:
        with psycopg2.connect(self._connection_str) as con:
            with con.open(cursor_factory=RealDictCursor) as cur:
                cur.execute("SET TIME_ZONE = 'UTC';")
                yield cur

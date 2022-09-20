import contextlib
import typing

import psycopg2
from psycopg2._psycopg import cursor  # noqa
from psycopg2.extras import RealDictCursor

from src import data

__all__ = ("PgCursorProvider",)


class PgCursorProvider(data.CursorProvider):
    def __init__(self, *, connection_str: str):
        self._connection_str = connection_str

    @contextlib.contextmanager
    def open(self) -> typing.Generator[cursor, None, None]:
        with psycopg2.connect(self._connection_str) as con:
            with con.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SET TIME ZONE 'UTC';")
                yield cur

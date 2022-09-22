import contextlib
import typing

import psycopg2
from psycopg2.extras import RealDictCursor

from src import data

__all__ = ("PgCursorProvider",)


class PgCursorProvider(data.CursorProvider):
    def __init__(self, *, connection_str: str):
        self._connection_str = connection_str

    @contextlib.contextmanager
    def open(self) -> typing.Generator[RealDictCursor, None, None]:
        con = psycopg2.connect(self._connection_str)
        con.autocommit = True
        try:
            with con:
                with con.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SET SESSION idle_in_transaction_session_timeout = '15min';")
                    cur.execute("SET SESSION lock_timeout = '5min';")
                    cur.execute("SET SESSION TIME ZONE 'UTC';")
                    yield cur
        finally:
            con.close()

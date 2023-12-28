import contextlib
import typing

import pyodbc

from src import data

__all__ = ("OdbcCursorProvider",)


class OdbcCursorProvider(data.CursorProvider):
    def __init__(self, *, db_config: data.DbConfig):
        self._db_config: typing.Final[data.DbConfig] = db_config

    @contextlib.contextmanager
    def open(self) -> typing.Generator[pyodbc.Cursor, None, None]:
        if self._db_config.api == data.API.HH:
            autocommit = True
        else:
            autocommit = False

        with pyodbc.connect(self._connection_str, autocommit=autocommit) as con:
            with con.cursor() as cur:
                yield cur

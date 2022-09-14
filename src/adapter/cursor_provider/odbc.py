import typing

import pyodbc

from src import data

__all__ = ("OdbcCursorProvider",)


class OdbcCursorProvider(data.CursorProvider[pyodbc.Cursor]):
    def __init__(self, *, connection_str: str, api: data.API):
        self._connection_str = connection_str
        self._api = api

    def open(self) -> typing.Generator[data.CursorType, None, None]:
        if self._api == data.API.HH:
            autocommit = True
        else:
            autocommit = False

        with pyodbc.connect(self._connection_str, autocommit=autocommit) as con:
            with con.cursor() as cur:
                yield cur

import typing

import pyodbc

from src import data

__all__ = ("OdbcCursorProvider",)


class OdbcCursorProvider(data.CursorProvider[pyodbc.Cursor]):
    def __init__(self, *, connection_str: str):
        self._connection_str = connection_str

    def open(self) -> typing.Generator[data.CursorType, None, None]:
        with pyodbc.connect(self._connection_str) as con:
            with con.cursor() as cur:
                yield cur

import typing

import psycopg
import pyodbc

__all__ = ("Cursor",)

Cursor = typing.TypeVar("Cursor", bound=psycopg.Cursor | pyodbc.Cursor)

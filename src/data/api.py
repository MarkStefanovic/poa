from __future__ import annotations

import enum

__all__ = ("API",)


class API(str, enum.Enum):
    HH = "hh"
    PYODBC = "odbc"
    PSYCOPG2 = "pg"

    def __str__(self) -> str:
        return str.__str__(self)

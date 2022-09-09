from src import data
from src.adapter.cursor_provider.odbc import OdbcCursorProvider
from src.adapter.cursor_provider.pg import PgCursorProvider

__all__ = ("create",)


def create(*, api: data.API, connection_str: str) -> data.CursorProvider:
    if api in (data.API.HH, data.API.PYODBC):
        return OdbcCursorProvider(connection_str=connection_str)
    elif api == data.API.PSYCOPG2:
        return PgCursorProvider(connection_str=connection_str)
    else:
        raise NotImplementedError(f"data.CursorProvider is not implemented for the {api!s} api.")

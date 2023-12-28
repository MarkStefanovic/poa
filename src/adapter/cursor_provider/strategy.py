from src import data
from src.adapter.cursor_provider.odbc import OdbcCursorProvider
from src.adapter.cursor_provider.pg import PgCursorProvider

__all__ = ("create",)


def create(*, db_config: data.DbConfig) -> data.CursorProvider | data.Error:
    try:
        if db_config.api in (data.API.HH, data.API.MSSQL, data.API.PYODBC):
            return OdbcCursorProvider(db_config=db_config)
        elif db_config.api == data.API.PSYCOPG:
            return PgCursorProvider(db_config=db_config)
        else:
            return data.Error.new(
                f"CursorProvider is not implemented for the {db_config.api!s} api.",
                db_config=db_config,
            )
    except Exception as e:
        return data.Error.new(str(e), db_config=db_config)

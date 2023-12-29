import contextlib
import typing

import pydantic
import pyodbc

from src import data
from src.adapter.cursor.odbc import OdbcCursor

__all__ = ("OdbcCursorProvider",)


class OdbcCursorProvider(data.CursorProvider):
    def __init__(self, *, db_config: data.DbConfig):
        self._db_config: typing.Final[data.DbConfig] = db_config

    @contextlib.contextmanager
    def open(self) -> typing.Generator[data.Cursor | data.Error, None, None]:
        if self._db_config.api == data.API.HH:
            autocommit = True
        else:
            autocommit = False

        con_str = self._db_config.connection_string
        if con_str is None:
            yield data.Error.new("Connection string is required for OdbcCursorProvider")
        else:
            with _connect(connection_string=con_str, autocommit=autocommit) as con:
                if isinstance(con, data.Error):
                    yield con
                else:
                    with con.cursor() as cur:
                        yield OdbcCursor(cursor=cur)


@contextlib.contextmanager
def _connect(
    *,
    connection_string: pydantic.SecretStr,
    autocommit: bool,
) -> typing.Generator[data.Cursor | data.Error, None, None]:
    # noinspection PyBroadException
    try:
        con = pyodbc.connect(connection_string.get_secret_value(), autocommit=autocommit)
    except:  # noqa: E722
        yield data.Error.new("An error occurred while connecting to the database.")
    else:
        with con:
            yield con

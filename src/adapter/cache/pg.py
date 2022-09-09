import datetime
import typing

import psycopg2

from src import data

__all__ = ("PgCache",)


class PgCache(data.Cache):
    def __init__(self, *, cur: psycopg2.cursor, schema_name: str | None, table_name: str):
        self._cur = cur
        self._schema_name = schema_name
        self._table_name = table_name

    def add_increasing_col_value(self, *, col: str, value: typing.Hashable) -> None:
        pass

    def add_key_cols(self, /, key_cols: typing.Iterable[str]):
        pass

    def add_table_definition(self, /, table: data.Table) -> None:
        pass

    def get_key_cols(self) -> tuple[str] | None:
        pass

    def get_latest_incremental_col_values(self) -> dict[str, dict[str, typing.Hashable]]:
        pass

    def get_table_definition(self) -> data.Table | None:
        pass

    def get_table_exists(self) -> bool | None:
        pass

    def set_key_cols(self, /, key_cols: tuple[str]) -> None:
        pass

    def add_table_exists(self) -> None:
        pass

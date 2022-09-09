import dataclasses

import pyodbc

from src import data
from src.adapter.src_ds.odbc import OdbcSrcDs

__all__ = ("HHSrcTable",)


class HHSrcTable(OdbcSrcDs):
    def __init__(
        self,
        *,
        cache: data.Cache,
        cur: pyodbc.Cursor,
        schema_name: str,
        table_name: str,
        pk_cols: tuple[str],
    ):
        super().__init__(cache=cache, cur=cur, schema_name=schema_name, table_name=table_name)

        self._pk_cols = pk_cols

    def get_table(self) -> data.Table:
        table_def = super().get_table()
        return dataclasses.replace(table_def, pk=self._pk_cols)


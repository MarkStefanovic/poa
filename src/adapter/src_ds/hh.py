from __future__ import annotations

import dataclasses

import pyodbc

from src import data
from src.adapter.src_ds.odbc import OdbcSrcDs

__all__ = ("HHSrcDs",)


class HHSrcDs(OdbcSrcDs):
    def __init__(
        self,
        *,
        cur: pyodbc.Cursor,
        db_name: str,
        schema_name: str | None,
        table_name: str,
        pk_cols: tuple[str, ...],
    ):
        super().__init__(cur=cur, db_name=db_name, schema_name=schema_name, table_name=table_name)

        self._pk_cols = pk_cols

    def get_table(self) -> data.Table:
        table_def = super().get_table()
        col_defs = frozenset(
            dataclasses.replace(col, nullable=False) if col.name in self._pk_cols else col
            for col in table_def.columns
        )
        return dataclasses.replace(table_def, pk=self._pk_cols, columns=col_defs)


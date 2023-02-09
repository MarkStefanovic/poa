from __future__ import annotations

import dataclasses
import datetime
import itertools

import pyodbc

from src import data
from src.adapter.ds.src_ds.odbc import OdbcSrcDs

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
        after: dict[str, datetime.date],
    ):
        super().__init__(
            cur=cur,
            db_name=db_name,
            schema_name=schema_name,
            table_name=table_name,
            wrapper=_wrap_name,
            after=after,
        )

        self._pk_cols = pk_cols

    def get_table(self) -> data.Table:
        table_def = super().get_table()
        col_defs = (
            dataclasses.replace(col, nullable=False) if col.name in self._pk_cols else col
            for col in table_def.columns
        )
        col_defs = (
            dataclasses.replace(col, data_type=data.DataType.Timestamp)
            if col.data_type == data.DataType.TimestampTZ else col
            for col in col_defs
        )
        return dataclasses.replace(table_def, pk=self._pk_cols, columns=frozenset(col_defs))

    def fetch_rows_by_key(self, *, col_names: set[str] | None, keys: set[data.RowKey]) -> list[data.Row]:
        if keys:
            if col_names:
                cols = sorted(col_names)
            else:
                cols = sorted(c.name for c in self.get_table().columns)

            sql = "SELECT\n  "
            sql += "\n, ".join(_wrap_col_name_w_alias(col) for col in cols)
            sql += f"\nFROM {self._full_table_name}"

            if len(self.get_table().pk) > 1:
                raise NotImplementedError("HHSrcDs can only handle single-field primary keys.")

            key_col = list(next(itertools.islice(keys, 1)).keys())[0]

            key_list = list(itertools.chain(key[key_col] for key in keys))

            rows = []
            param_groups = []
            for i in range(0, len(key_list), 100):
                param_group = tuple(key_list[i:i+100])
                param_groups.append(param_group)

            for param_group in param_groups:
                where_clause = f"\nWHERE\n  {self._wrapper(key_col)} IN ({', '.join('?' for _ in param_group)});"
                self._cur.execute(sql + where_clause, param_group)
                rows += [dict(zip(cols, row)) for row in self._cur.fetchall()]

            return rows
        return []


def _wrap_name(name: str, /) -> str:
    return f"`{name}`"


def _wrap_col_name_w_alias(col_name: str, /) -> str:
    if col_name.lower() == col_name:
        return _wrap_name(col_name)
    return f"{_wrap_name(col_name)} AS {_wrap_name(col_name).lower()}"

from __future__ import annotations

import typing

import pyodbc

from src import data
from src.adapter.src_ds.hh import HHSrcDs
from src.adapter.src_ds.odbc import OdbcSrcDs

__all__ = ("create",)


def create(
    *,
    api: data.API,
    cur: typing.Any,
    db_name: str,
    schema_name: str | None,
    table_name: str,
    pk_cols: tuple[str, ...] | None = None
) -> data.SrcDs:
    if api == data.API.HH:
        assert pk_cols is not None and len(pk_cols) > 0, "pk_cols is required to create an HHSrcTable."
        return HHSrcDs(cur=typing.cast(pyodbc.Cursor, cur), db_name=db_name, schema_name=schema_name, table_name=table_name, pk_cols=pk_cols)
    elif api == data.API.PYODBC:
        return OdbcSrcDs(cur=typing.cast(pyodbc.Cursor, cur), db_name=db_name, schema_name=schema_name, table_name=table_name)
    else:
        raise NotImplementedError(f"The api specified, {api}, does not have an AbstractDstTable implementation.")

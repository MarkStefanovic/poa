import datetime
import typing

import pyodbc

from src import data
from src.adapter.ds.src_ds.hh import HHSrcDs
from src.adapter.ds.src_ds.ms import MSSrcDs
from src.adapter.ds.src_ds.odbc import OdbcSrcDs

__all__ = ("create",)


def create(
    *,
    cur: data.Cursor,
    api: data.API,
    db_name: str | None,
    schema_name: str | None,
    table_name: str,
    pk_cols: tuple[str, ...] | None,
    after: dict[str, datetime.date],
) -> data.SrcDs | data.Error:
    try:
        if api == data.API.HH:
            if pk_cols is None or len(pk_cols) == 0:
                return data.Error.new(
                    "pk_cols is required to create an HHSrcTable.",
                    api=api,
                    db_name=db_name,
                    schema_name=schema_name,
                    table_name=table_name,
                    after=tuple(after.items()),
                )

            return HHSrcDs(
                cur=typing.cast(pyodbc.Cursor, cur),
                db_name=db_name,
                schema_name=schema_name,
                table_name=table_name,
                pk_cols=pk_cols,
                after=after,
            )
        elif api == data.API.MSSQL:
            if pk_cols is None or len(pk_cols) == 0:
                return data.Error.new(
                    "pk_cols is required to create an MSSrcDs.",
                    api=api,
                    db_name=db_name,
                    schema_name=schema_name,
                    table_name=table_name,
                    after=tuple(after.items()),
                )

            return MSSrcDs(
                cur=typing.cast(pyodbc.Cursor, cur),
                db_name=db_name,
                schema_name=schema_name,
                table_name=table_name,
                pk_cols=pk_cols,
                after=after,
            )
        elif api == data.API.PYODBC:
            return OdbcSrcDs(
                cur=typing.cast(pyodbc.Cursor, cur),
                db_name=db_name,
                schema_name=schema_name,
                table_name=table_name,
                after=after,
            )
        else:
            raise NotImplementedError(
                f"The api specified, {api}, does not have an SrcDs implementation."
            )
    except Exception as e:
        return data.Error.new(
            str(e),
            api=api,
            db_name=db_name,
            schema_name=schema_name,
            table_name=table_name,
            pk_cols=pk_cols,
            after=tuple(after.items()),
        )

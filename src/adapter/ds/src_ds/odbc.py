from __future__ import annotations

import datetime
import itertools
import traceback
import typing

import pyodbc
from loguru import logger

from src import data
from src.adapter.ds import shared

__all__ = ("OdbcSrcDs",)

Wrapper: typing.TypeAlias = typing.Callable[[str], str]


def default_wrapper(name: str, /) -> str:
    return f'"{name}"'


class OdbcSrcDs(data.SrcDs):
    def __init__(
        self,
        *,
        cur: pyodbc.Cursor,
        db_name: str,
        schema_name: str | None,
        table_name: str,
        after: dict[str, datetime.date],
        wrapper: Wrapper = default_wrapper,
    ):
        self._cur: typing.Final[pyodbc.Cursor] = cur
        self._db_name: typing.Final[str] = db_name
        self._schema_name: typing.Final[str | None] = schema_name
        self._table_name: typing.Final[str] = table_name
        self._after: typing.Final[dict[str, datetime.date]] = after
        self._wrapper: typing.Final[Wrapper] = wrapper

        if self._schema_name:
            self._full_table_name = f"{wrapper(self._schema_name)}.{wrapper(table_name)}"
        else:
            self._full_table_name = wrapper(table_name)

        self._table: data.Table | None = None

    def fetch_rows(
        self, *, col_names: set[str] | None, after: dict[str, typing.Hashable] | None
    ) -> list[data.Row]:
        if col_names:
            cols = sorted(col_names)
        else:
            cols = sorted({c.name for c in self.get_table().columns})

        full_after = shared.combine_filters(ds_filter=self._after, query_filter=after)

        sorted_after: list[tuple[str, typing.Hashable]] = []
        params: list[typing.Hashable] | None = None
        if full_after:
            sorted_after = sorted((key, val) for key, val in full_after.items() if val is not None)
            params = [itm[1] for itm in sorted_after]

        sql = "SELECT "
        sql += ", ".join(
            _wrap_col_name_w_alias(wrapper=self._wrapper, col_name=col) for col in cols
        )
        sql += f" FROM {self._full_table_name}"
        if sorted_after:
            sql += " WHERE " + " OR ".join(f"{self._wrapper(key)} > ?" for key, val in sorted_after)

        if params:
            try:
                self._cur.execute(sql, params)
            except Exception as e:
                logger.error(
                    f"An error occurred while running {sql!r}, params: {params!r}: {e!s}\n{traceback.format_exc()}"
                )
                raise
            self._cur.execute(sql, params)
        else:
            try:
                self._cur.execute(sql)
            except Exception as e:
                logger.error(
                    f"An error occurred while running {sql!r}: {e!s}\n{traceback.format_exc()}"
                )
                raise

        return [dict(zip(cols, row)) for row in self._cur.fetchall()]

    def fetch_rows_by_key(
        self, *, col_names: set[str] | None, keys: set[data.RowKey]
    ) -> list[data.Row]:
        if keys:
            if col_names:
                cols = sorted(col_names)
            else:
                cols = sorted(c.name for c in self.get_table().columns)

            sql = "SELECT "
            sql += ", ".join(
                _wrap_col_name_w_alias(wrapper=self._wrapper, col_name=col) for col in cols
            )
            sql += f" FROM {self._full_table_name}"

            sql += " WHERE "
            key_cols = sorted(next(itertools.islice(keys, 1)).keys())
            sql += " AND ".join(f"{self._wrapper(key_col)} = ?" for key_col in key_cols)

            params = (tuple(row[key_col] for key_col in key_cols) for row in keys)

            self._cur.executemany(sql, params)
            return [dict(zip(cols, row)) for row in self._cur.fetchall()]

        return []

    def get_row_count(self) -> int:
        if self._schema_name:
            full_table_name = (
                f"{self._wrapper(self._schema_name)}.{self._wrapper(self._table_name)}"
            )
        else:
            full_table_name = self._wrapper(self._table_name)

        sql = f"SELECT count(*) AS ct FROM {full_table_name}"

        if self._after:
            sorted_after = sorted((key, val) for key, val in self._after.items() if val is not None)

            sorted_keys = [itm[0] for itm in sorted_after]

            params = [itm[1] for itm in sorted_after]

            sql += "WHERE " + " OR ".join(f"{self._wrapper(key)} > ?" for key in sorted_keys)

            result = self._cur.execute(sql, params)
        else:
            result = self._cur.execute(sql)

        return result.fetchval()

    def get_table(self) -> data.Table:
        if self._table is not None:
            return self._table

        if not self.table_exists():
            raise data.error.TableDoesntExist(
                table_name=self._table_name, schema_name=self._schema_name
            )

        cols: list[data.Column] = []
        for row in self._cur.columns(self._table_name, schema=self._schema_name):
            col_name = row.column_name.lower()

            col = data.Column(
                name=col_name,
                nullable=_get_nullable(row),
                data_type=_get_data_type(row),
                length=_get_length(row),
                precision=_get_precision(row),
                scale=_get_scale(row),
            )
            cols.append(col)

        pk = _get_key_cols(
            cur=self._cur, schema_name=self._schema_name, table_name=self._table_name
        )

        table = data.Table(
            db_name=self._db_name,
            schema_name=self._schema_name,
            table_name=self._table_name,
            columns=frozenset(cols),
            pk=pk,
        )

        self._table = table

        return table

    def table_exists(self) -> bool:
        table_exists = bool(
            self._cur.tables(table=self._table_name, schema=self._schema_name).fetchone()
        )
        return table_exists


def _get_data_type(row: pyodbc.Row, /) -> data.DataType:
    if row.type_name == "bool":
        return data.DataType.Bool
    else:
        return {
            -10: data.DataType.Text,  # 'text'
            -11: data.DataType.UUID,  # 'uuid'
            -1: data.DataType.Text,  # 'text'
            -3: data.DataType.Text,  # 'bytea'
            -4: data.DataType.Text,  # 'bytea'
            -5: data.DataType.BigInt,  # 'int8'
            -6: data.DataType.Int,  # 'int2'
            -7: data.DataType.Bool,  # 'bool'
            -8: data.DataType.Text,  # 'char'
            -9: data.DataType.Text,  # 'varchar'
            10: data.DataType.Timestamp,  # 'time'
            11: data.DataType.TimestampTZ,  # 'timestamptz'
            12: data.DataType.Text,  # 'varchar'
            1: data.DataType.Text,  # 'char'
            2: data.DataType.Decimal,  # 'numeric'
            3: data.DataType.Decimal,  # 'numeric'
            4: data.DataType.Int,  # 'int4'
            5: data.DataType.Int,  # 'int2'
            6: data.DataType.Float,  # 'float8'
            7: data.DataType.Float,  # 'float4'
            8: data.DataType.BigFloat,  # 'float8'  # Double Precision in pg
            91: data.DataType.Date,  # 'date'
            92: data.DataType.Timestamp,  # 'time'
            93: data.DataType.TimestampTZ,  # 'timestamptz'
            9: data.DataType.Date,  # 'date'
        }[row.data_type]


def _get_key_cols(
    *, cur: pyodbc.Cursor, schema_name: str | None, table_name: str
) -> tuple[str, ...]:
    return tuple(
        typing.cast(str, col.column_name.lower())
        for col in cur.primaryKeys(table_name, schema=schema_name)
    )


def _get_length(row: pyodbc.Row, /) -> int | None:
    if hasattr(row, "length"):
        return row.length
    return None


def _get_nullable(row: pyodbc.Row, /) -> bool:
    if isinstance(row.is_nullable, str):
        if row.is_nullable == "YES":
            return True
        if row.is_nullable == "NO":
            return False
        raise ValueError(
            f"Expected is_nullable to be either 'YES' or 'NO', but got {row.is_nullable!r}."
        )
    else:
        if row.is_nullable == 1:
            return True
        return False


def _get_precision(row: pyodbc.Row, /) -> int | None:
    if hasattr(row, "precision"):
        return row.precision
    return None


def _get_scale(row: pyodbc.Row, /) -> int | None:
    if hasattr(row, "scale"):
        assert row.scale is None or isinstance(
            row.scale, int
        ), f"Expected row.scale to be an integer, but got {row.scale!r}."
        return row.scale

    return None


def _render_dt(dt: datetime.datetime, /) -> str:
    return "'" + dt.isoformat(sep=" ", timespec="milliseconds") + "'"


def _wrap_col_name_w_alias(*, wrapper: typing.Callable[[str], str], col_name: str) -> str:
    if col_name.lower() == col_name:
        return wrapper(col_name)
    return f"{wrapper(col_name)} AS {wrapper(col_name).lower()}"


# if __name__ == '__main__':
# with pyodbc.connect(
#     'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=mine;UID=me;PWD=pwd') as con:
#     with con.cursor_provider() as cur:
#         tbl = inspect_table(cur=cur, table_name="mytable", schema_name="myschema")
#         print(tbl)

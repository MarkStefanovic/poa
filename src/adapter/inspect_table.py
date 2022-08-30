import typing

import pyodbc

from src import data

__all__ = ("inspect_table", "table_exists")


def inspect_table(
    cur: pyodbc.Cursor,
    table_name: str,
    schema_name: str | None = None,
) -> data.Table:
    if not table_exists(cur=cur, table_name=table_name, schema_name=schema_name):
        raise data.error.TableDoesntExist(table_name=table_name, schema_name=schema_name)

    cols: list[data.Column] = []
    for row in cur.columns(table_name, schema=schema_name):
        col = data.Column(
            name=row.column_name,
            nullable=_get_nullable(row),
            data_type=_get_data_type(row),
            length=_get_length(row),
            precision=_get_precision(row),
            scale=_get_scale(row),
        )
        cols.append(col)

    pk = _get_primary_key_cols_for_table(
        cur=cur,
        table_name=table_name,
        schema_name=schema_name,
    )

    return data.Table(
        schema_name=schema_name,
        table_name=table_name,
        columns=frozenset(cols),
        pk=pk,
    )

    
def table_exists(
    cur: pyodbc.Cursor,
    table_name: str,
    schema_name: str | None,
) -> bool:
    return bool(cur.tables(table=table_name, schema=schema_name).fetchone())


def _get_data_type(row: pyodbc.Row, /) -> data.DataType:
    if row.type_name == "bool":
        return data.DataType.Bool
    else:
        return {
            -10: lambda: data.DataType.Text,  # 'text'
            -11: lambda: data.DataType.UUID,  # 'uuid'
            -1: lambda: data.DataType.Text,  # 'text'
            -3: lambda: data.DataType.Text,  # 'bytea'
            -4: lambda: data.DataType.Text,  # 'bytea'
            -5: lambda: data.DataType.BigInt,  # 'int8'
            -6: lambda: data.DataType.Int,  # 'int2'
            -7: lambda: data.DataType.Bool,  # 'bool'
            -8: lambda: data.DataType.Text,  # 'char'
            -9: lambda: data.DataType.Text,  # 'varchar'
            10: lambda: data.DataType.Timestamp,  # 'time'
            11: lambda: data.DataType.TimestampTZ,  # 'timestamptz'
            12: lambda: data.DataType.Text,  # 'varchar'
            1: lambda: data.DataType.Text,  # 'char'
            2: lambda: data.DataType.Decimal,  # 'numeric'
            3: lambda: data.DataType.Decimal,  # 'numeric'
            4: lambda: data.DataType.Int,  # 'int4'
            5: lambda: data.DataType.Int,  # 'int2'
            6: lambda: data.DataType.Float,  # 'float8'
            7: lambda: data.DataType.Float,  # 'float4'
            8: lambda: data.DataType.BigFloat,  # 'float8'  # Double Precision in pg
            91: lambda: data.DataType.Date,  # 'date'
            92: lambda: data.DataType.Timestamp,  # 'time'
            93: lambda: data.DataType.TimestampTZ,  # 'timestamptz'
            9: lambda: data.DataType.Date,  # 'date'
        }[row.data_type]


def _get_length(row: pyodbc.Row, /) -> typing.Optional[int]:
    if hasattr(row, "length"):
        return row.length
    return None


def _get_nullable(row: pyodbc.Row, /) -> bool:
    if isinstance(row.is_nullable, str):
        if row.is_nullable == "YES":
            return True
        if row.is_nullable == "NO":
            return False
        raise ValueError(f"Expected is_nullable to be either 'YES' or 'NO', but got {row.is_nullable!r}.")
    else:
        if row.is_nullable == 1:
            return True
        return False


def _get_precision(row: pyodbc.Row, /) -> typing.Optional[int]:
    if hasattr(row, "precision"):
        return row.precision
    return None


def _get_primary_key_cols_for_table(
    cur: pyodbc.Cursor, table_name: str, schema_name: typing.Optional[str]
) -> tuple[str]:
    return tuple(
        typing.cast(str, col.column_name.lower())
        for col in cur.primaryKeys(table_name, schema=schema_name)
    )


def _get_scale(row: pyodbc.Row, /) -> typing.Optional[int]:
    if hasattr(row, "scale"):
        return row.scale
    return None


if __name__ == '__main__':
    with pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=mine;UID=me;PWD=pwd') as con:
        with con.cursor() as cur:
            tbl = inspect_table(cur=cur, table_name="mytable", schema_name="myschema")
            print(tbl)

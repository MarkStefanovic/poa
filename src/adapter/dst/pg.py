import operator

import pyodbc

from src import data

__all__ = ("create_table",)


def create_table(*, con: pyodbc.Connection, table: data.Table) -> None:
    with con.cursor() as cur:
        sql = _generate_create_table_sql(table=table)
        cur.execute(sql)


def _generate_create_table_sql(*, table: data.Table) -> str:
    sql = "CREATE TABLE ("
    sql += ", ".join(
        _generate_column_definition(col=col)
        for col in sorted(table.columns, key=operator.attrgetter("name"))
    )
    if table.pk:
        sql += ", PRIMARY KEY (" + ", ".join(_wrap_name(col) for col in table.pk) + ")"
    sql += ")"
    return sql


def _generate_column_definition(*, col: data.Column) -> str:
    if col.nullable:
        nullable = "NULL"
    else:
        nullable = "NOT NULL"

    col_name = _wrap_name(col.name)

    return {
        data.DataType.BigFloat: lambda: f"{col_name} DOUBLE PRECISION {nullable}",
        data.DataType.BigInt: lambda: f"{col_name} BIGINT {nullable}",
        data.DataType.Bool: lambda: f"{col_name} BOOL {nullable}",
        data.DataType.Date: lambda: f"{col_name} DATE {nullable}",
        data.DataType.Decimal: lambda: f"{col_name} NUMERIC({18 if col.precision is None else col.precision}, {4 if col.scale is None else col.scale}) {nullable}",
        data.DataType.Float: lambda: f"{col_name} FLOAT {nullable}",
        data.DataType.Int: lambda: f"{col_name} INT {nullable}",
        data.DataType.Text: lambda: f"{col_name} TEXT {nullable}",
        data.DataType.Timestamp: lambda: f"{col_name} TIMESTAMP {nullable}",
        data.DataType.TimestampTZ: lambda: f"{col_name} TIMESTAMPTZ {nullable}",
        data.DataType.UUID: lambda: f"{col_name} UUID {nullable}",
    }[col.data_type]()


def _wrap_name(name: str, /) -> str:
    return f'"{name}"'


if __name__ == '__main__':
    tbl = data.Table(
        schema_name="dbo",
        table_name="activity",
        columns=frozenset((
            data.Column(name="activity_id", data_type=data.DataType.Int, nullable=False, length=None, precision=None, scale=None),
            data.Column(name="description", data_type=data.DataType.Text, nullable=False, length=None, precision=None, scale=None),
            data.Column(name="created_date", data_type=data.DataType.Timestamp, nullable=False, length=None, precision=None, scale=None),
            data.Column(name="changed_date", data_type=data.DataType.Timestamp, nullable=False, length=None, precision=None, scale=None),
        )),
        pk=("activity_id",),
    )
    sql = _generate_create_table_sql(table=tbl)
    print(sql)

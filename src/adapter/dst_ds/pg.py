import dataclasses
import itertools
import operator
import textwrap
import typing

from psycopg2.extras import RealDictCursor

from src import data

__all__ = ("PgDstDs",)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection,SqlResolve
class PgDstDs(data.DstDs):
    def __init__(self, *, cur: RealDictCursor, table: data.Table):
        self._cur = cur
        self._src_table = table

        if table.schema_name:
            table_name = f"{table.schema_name}_{table.table_name}"
        else:
            table_name = table.table_name

        self._dst_table = dataclasses.replace(table, schema_name="poa", table_name=table_name)

    def create(self) -> None:
        sql = _generate_create_table_sql(table=self._dst_table)
        self._cur.execute(sql)

    def delete_rows(self, /, keys: set[data.RowKey]) -> int:
        if keys:
            sql = _generate_delete_rows_sql(
                schema_name=self._dst_table.schema_name,
                table_name=self._dst_table.table_name,
                keys=keys,
            )
            self._cur.executemany(sql, keys)
            result = self._cur.fetchall()
            return sum(row[0] for row in result)

        return 0

    def fetch_rows(
        self,
        *,
        col_names: set[str] | None,
        after: dict[str, typing.Hashable] | None,
    ) -> list[data.Row]:
        if col_names:
            cols = sorted(set(col_names))
        else:
            cols = sorted({c.name for c in self._dst_table.columns})

        full_table_name = _generate_full_table_name(
            schema_name=self._dst_table.schema_name,
            table_name=self._dst_table.table_name,
        )

        sql = "SELECT\n  "
        sql += "\n, ".join(_wrap_name(col) for col in cols)
        sql += f"\nFROM {full_table_name}"

        self._cur.execute(sql)
        return self._cur.fetchall()  # type: ignore

    def get_row_count(self) -> int:
        full_table_name = _generate_full_table_name(
            schema_name=self._dst_table.schema_name,
            table_name=self._dst_table.table_name,
        )
        self._cur.execute(f"SELECT COUNT(*) AS ct FROM {full_table_name}")
        return self._cur.fetchone()["ct"]  # noqa

    def get_sync_table_spec(self) -> data.SyncTableSpec:
        raise data.error.SyncTableSpecNotFound(
            schema_name=self._src_table.schema_name,
            table_name=self._src_table.table_name,
        )

    def table_exists(self) -> bool:
        self._cur.execute(
            """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables AS t
                    WHERE
                        t.table_schema = %(schema_name)s
                        AND t.table_name = %(table_name)s
                )
            """,
            {"schema_name": self._dst_table.schema_name, "table_name": self._dst_table.table_name},
        )
        return self._cur.fetchone()[0]

    def truncate(self) -> None:
        full_table_name = _generate_full_table_name(
            schema_name=self._dst_table.schema_name,
            table_name=self._dst_table.table_name,
        )
        self._cur.execute(f"TRUNCATE {full_table_name}")

    def upsert_rows(self, /, rows: typing.Iterable[data.Row]) -> dict[typing.Literal["rows_added", "rows_updated"], int]:
        if rows:
            sql = _generate_upsert_sql(
                schema_name=self._dst_table.schema_name,
                table_name=self._dst_table.table_name,
                column_names={c.name for c in self._dst_table.columns},
                pk_cols=self._dst_table.pk,
            )
            self._cur.executemany(sql, rows)
            result = self._cur.fetchall()
            ops = {row["poa_op"] for row in result}  # noqa
            rows_added = sum(1 for op in ops if op == "a")
            rows_updated = sum(1 for op in ops if op == "u")
            return {"rows_added": rows_added, "rows_updated": rows_updated}


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
def _generate_create_table_sql(*, table: data.Table) -> str:
    if table.schema_name:
        full_table_name = f"{_wrap_name(table.schema_name)}.{_wrap_name(table.table_name)}"
    else:
        full_table_name = _wrap_name(table.table_name)
    sql = f"CREATE TABLE {full_table_name} (\n  "
    sql += "\n, ".join(
        _generate_column_definition(col=col)
        for col in sorted(table.columns, key=operator.attrgetter("name"))
    ) + (
        "\n, poa_hd CHAR(32) NOT NULL"
        "\n, poa_op CHAR(1) NOT NULL CHECK (op IN ('a', 'd', 'u'))"
        "\n, poa_ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()"
    )
    if table.pk:
        sql += "\n, PRIMARY KEY (" + ", ".join(_wrap_name(col) for col in table.pk) + ")"
    sql += "\n);"
    sql += f"\nCREATE INDEX ix_{table.table_name}_poa_ts ON {full_table_name} (poa_ts DESC);"
    sql += f"\nCREATE INDEX ix_{table.table_name}_poa_op ON {full_table_name} (poa_op);"
    return sql


def _generate_delete_rows_sql(
    *,
    schema_name: str | None,
    table_name: str,
    keys: set[data.RowKey],
) -> str:
    assert len(keys) > 0, "keys cannot be empty."

    full_table_name = _generate_full_table_name(schema_name=schema_name, table_name=table_name)
    key: data.RowKey = next(itertools.islice(keys, 1))
    key_cols = sorted(key.keys())
    where_clause = " AND ".join(f"t.{_wrap_name(c)} = %({c})s" for c in key_cols)
    return textwrap.dedent(f"""
        WITH del AS (
            UPDATE {full_table_name} AS t 
            SET 
                poa_op = 'd'
            ,   poa_ts = now()
            WHERE 
                {where_clause}
                AND poa_op <> 'd'
            RETURNING 1
        )
        SELECT * FROM del;
    """).strip()


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
        data.DataType.Decimal: lambda: (
            f"{col_name} NUMERIC({18 if col.precision is None else col.precision}, {4 if col.scale is None else col.scale}) {nullable}"
        ),
        data.DataType.Float: lambda: f"{col_name} FLOAT {nullable}",
        data.DataType.Int: lambda: f"{col_name} INT {nullable}",
        data.DataType.Text: lambda: f"{col_name} TEXT {nullable}",
        data.DataType.Timestamp: lambda: f"{col_name} TIMESTAMP(3) {nullable}",
        data.DataType.TimestampTZ: lambda: f"{col_name} TIMESTAMPTZ(3) {nullable}",
        data.DataType.UUID: lambda: f"{col_name} UUID {nullable}",
    }[col.data_type]()


def _generate_full_table_name(*, schema_name: str | None, table_name: str) -> str:
    if schema_name:
        return f"{_wrap_name(schema_name)}.{_wrap_name(table_name)}"
    else:
        return _wrap_name(table_name)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
def _generate_upsert_sql(
    *,
    schema_name: str | None,
    table_name: str,
    column_names: set[str],
    pk_cols: tuple[str],
) -> str:
    col_names = sorted(column_names)

    col_name_csv = ", ".join(_wrap_name(c) for c in col_names)

    placeholders = (f"%({c})s" for c in col_names)

    hd_col_csv = ", ".join(_wrap_name(c) for c in col_names if c not in pk_cols)
    hd = f"MD5(ROW({hd_col_csv})::TEXT)"

    pk_csv = ", ".join(_wrap_name(c) for c in pk_cols)

    set_values_csv = "\n, ".join(
        f"{_wrap_name(c)} = EXCLUDED.{_wrap_name(c)}"
        for c in col_names if c not in pk_cols
    ) + "\n, poa_hd = EXCLUDED.hd, poa_op = 'u'\n, poa_ts = now()"

    if schema_name:
        full_table_name = f"{_wrap_name(schema_name)}.{_wrap_name(table_name)}"
    else:
        full_table_name = _wrap_name(table_name)

    return textwrap.dedent(f"""
        INSERT INTO {full_table_name}
          ({col_name_csv}, poa_hd, poa_op)
        VALUES
          ({placeholders}, {hd} 'a')
        ON CONFLICT ({pk_csv})
        DO UPDATE SET
          {set_values_csv}
        WHERE
            {full_table_name}.poa_hd <> EXCLUDED.poa_hd
            OR {full_table_name}.poa_op = 'd'
        RETURNING poa_op
    """).strip()


def _wrap_name(name: str, /) -> str:
    return f'"{name.lower()}"'


if __name__ == '__main__':
    tbl = data.Table(
        schema_name="gtl",
        table_name="tmp_activity",
        columns=frozenset((
            data.Column(name="activity_id", data_type=data.DataType.Int, nullable=False, length=None, precision=None, scale=None),
            data.Column(name="description", data_type=data.DataType.Text, nullable=False, length=None, precision=None, scale=None),
            data.Column(name="created_date", data_type=data.DataType.Timestamp, nullable=False, length=None, precision=None, scale=None),
            data.Column(name="changed_date", data_type=data.DataType.Timestamp, nullable=False, length=None, precision=None, scale=None),
        )),
        pk=("activity_id",),
    )
    s = _generate_create_table_sql(table=tbl)
    print(s)

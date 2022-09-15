from __future__ import annotations

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
    def __init__(self, *, cur: RealDictCursor, dst_db_name: str, dst_schema_name: str | None, src_table: data.Table):
        self._cur = cur
        self._dst_db_name = dst_db_name
        self._dst_schema_name = dst_schema_name
        self._src_table = src_table

        if src_table.schema_name:
            dst_table_name = f"{src_table.db_name}_{src_table.schema_name}_{src_table.table_name}"
        else:
            dst_table_name = f"{src_table.db_name}_{src_table.table_name}"

        self._dst_table = dataclasses.replace(
            src_table,
            db_name=self._dst_db_name,
            schema_name=self._dst_schema_name,
            table_name=dst_table_name,
        )

    def add_increasing_col_indices(self, /, increasing_cols: set[str]) -> None:
        full_table_name = _generate_full_table_name(
            schema_name=self._dst_table.schema_name,
            table_name=self._dst_table.table_name,
        )
        for col in increasing_cols:
            self._cur.execute(
                f"CREATE INDEX ix_{self._dst_table.table_name}_{col} ON {full_table_name} ({_wrap_name(col)} DESC);")

    def add_table_def(self, /, table: data.Table) -> None:
        # TODO
        pass

    def create(self) -> None:
        full_table_name = _generate_full_table_name(
            schema_name=self._dst_table.schema_name,
            table_name=self._dst_table.table_name,
        )
        sql = f"CREATE TABLE {full_table_name} (\n  "
        sql += "\n, ".join(
            _generate_column_definition(col=col)
            for col in sorted(self._dst_table.columns, key=operator.attrgetter("name"))
        )
        sql += (
            "\n, poa_hd CHAR(32) NOT NULL"
            "\n, poa_op CHAR(1) NOT NULL CHECK (poa_op IN ('a', 'd', 'u'))"
            "\n, poa_ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()"
            "\n, PRIMARY KEY (" + ", ".join(_wrap_name(col) for col in self._dst_table.pk) + ")"
            "\n);"
        )
        self._cur.execute(sql)
        self._cur.execute(f"CREATE INDEX ix_{self._dst_table.table_name}_poa_ts ON {full_table_name} (poa_ts DESC);")
        self._cur.execute(f"CREATE INDEX ix_{self._dst_table.table_name}_poa_op ON {full_table_name} (poa_op);")

    def delete_rows(self, /, keys: set[data.RowKey]) -> None:
        if keys:
            full_table_name = _generate_full_table_name(
                schema_name=self._dst_table.schema_name,
                table_name=self._dst_table.table_name,
            )
            key: data.RowKey = next(itertools.islice(keys, 1))
            key_cols = sorted(key.keys())
            where_clause = " AND ".join(f"t.{_wrap_name(c)} = %({c})s" for c in key_cols)
            sql = textwrap.dedent(f"""
                UPDATE {full_table_name} AS t 
                SET 
                    poa_op = 'd'
                ,   poa_ts = now()
                WHERE 
                    {where_clause}
                    AND poa_op <> 'd'
            """).strip()
            self._cur.executemany(sql, keys)

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
        sorted_after: list[tuple[str, typing.Hashable]] = []
        params: dict[str, typing.Hashable] | None = None
        if after:
            sorted_after = sorted((key, val) for key, val in after.items() if val is not None)
            params = dict(sorted_after)
        if sorted_after:
            sql += "\nWHERE\n  " + "\n  OR ".join(f"{_wrap_name(key)} > %({key})s" for key, val in sorted_after)
        self._cur.execute(sql, params)
        return self._cur.fetchall()  # noqa

    def get_max_values(self, /, cols: set[str]) -> dict[str, typing.Hashable] | None:
        full_table_name = _generate_full_table_name(
            schema_name=self._dst_table.schema_name,
            table_name=self._dst_table.table_name,
        )
        max_values = {}
        for col in sorted(cols):
            self._cur.execute(f"SELECT MAX({_wrap_name(col)}) AS v FROM {full_table_name}")
            value = self._cur.fetchone()["v"]  # noqa
            if value is not None:
                max_values[col] = value
        if max_values:
            return max_values
        return None

    def get_row_count(self) -> int:
        full_table_name = _generate_full_table_name(
            schema_name=self._dst_table.schema_name,
            table_name=self._dst_table.table_name,
        )
        self._cur.execute(f"SELECT COUNT(*) AS ct FROM {full_table_name}")
        return self._cur.fetchone()["ct"]  # noqa

    def get_table_def(self) -> data.Table:
        self._cur.execute(
            """
            SELECT col_name, col_data_type, col_length, col_precision, col_scale, col_nullable
            FROM poa.get_table_cols(p_src_db_name := %(src_db_name)s, p_src_schema_name := %(src_schema_name)s, p_src_table_name := %(src_table_name)s)
            """,
            {"src_db_name": self._src_table.db_name, "src_schema_name": self._src_table.schema_name, " src_table_name": self._src_table.table_name},
        )
        # result = self._cur.fetchall()
        # col_defs = [
        #     data.Column(name=row["col_name"], data_type=)
        #     for row in result
        # ]
        # TODO

    def table_exists(self) -> bool:
        self._cur.execute(
            """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables AS t
                    WHERE
                        t.table_schema = %(schema_name)s
                        AND t.table_name = %(table_name)s
                ) AS tbl_exists
            """,
            {"schema_name": self._dst_table.schema_name, "table_name": self._dst_table.table_name},
        )
        return self._cur.fetchone()["tbl_exists"]  # noqa

    def truncate(self) -> None:
        full_table_name = _generate_full_table_name(
            schema_name=self._dst_table.schema_name,
            table_name=self._dst_table.table_name,
        )
        self._cur.execute(f"TRUNCATE {full_table_name}")

    def upsert_rows(self, /, rows: typing.Iterable[data.Row]) -> None:
        if rows:
            col_names = sorted({c.name for c in self._dst_table.columns})

            col_name_csv = ", ".join(_wrap_name(c) for c in col_names)

            placeholders = ", ".join(f"%({c})s" for c in col_names)

            hd_col_csv = ", ".join(f"%({c})s" for c in col_names if c not in self._dst_table.pk)
            hd = f"MD5(ROW({hd_col_csv})::TEXT)"

            pk_csv = ", ".join(_wrap_name(c) for c in self._dst_table.pk)

            set_values_csv = "\n, ".join(
                f"{_wrap_name(c)} = EXCLUDED.{_wrap_name(c)}"
                for c in col_names if c not in self._dst_table.pk
            ) + "\n, poa_hd = EXCLUDED.poa_hd, poa_op = 'u'\n, poa_ts = now()"

            if self._dst_table.schema_name:
                full_table_name = f"{_wrap_name(self._dst_table.schema_name)}.{_wrap_name(self._dst_table.table_name)}"
            else:
                full_table_name = _wrap_name(self._dst_table.table_name)

            sql = textwrap.dedent(f"""
                INSERT INTO {full_table_name}
                  ({col_name_csv}, poa_hd, poa_op)
                VALUES
                  ({placeholders}, {hd}, 'a')
                ON CONFLICT ({pk_csv})
                DO UPDATE SET
                  {set_values_csv}
                WHERE
                    {full_table_name}.poa_hd <> EXCLUDED.poa_hd
                    OR {full_table_name}.poa_op = 'd'
                RETURNING poa_op
            """).strip()
            self._cur.executemany(sql, rows)


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
        data.DataType.Timestamp: lambda: f"{col_name} TIMESTAMP {nullable}",
        data.DataType.TimestampTZ: lambda: f"{col_name} TIMESTAMPTZ {nullable}",
        data.DataType.UUID: lambda: f"{col_name} UUID {nullable}",
    }[col.data_type]()


def _generate_full_table_name(*, schema_name: str | None, table_name: str) -> str:
    if schema_name:
        return f"{_wrap_name(schema_name)}.{_wrap_name(table_name)}"
    else:
        return _wrap_name(table_name)


def _wrap_name(name: str, /) -> str:
    return f'"{name.lower()}"'

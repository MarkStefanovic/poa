from __future__ import annotations

import itertools
import textwrap
import typing

from psycopg2.extras import RealDictCursor

from src import data

__all__ = ("PgSrcDs",)


class PgSrcDs(data.SrcDs):
    def __init__(
        self,
        *,
        cur: RealDictCursor,
        db_name: str,
        schema_name: str,
        table_name: str,
    ):
        self._cur = cur
        self._db_name = db_name
        self._schema_name = schema_name
        self._table_name = table_name

        if self._schema_name:
            self._full_table_name = f"{_wrapper(self._schema_name)}.{_wrapper(table_name)}"
        else:
            self._full_table_name = _wrapper(table_name)

        self._table: data.Table | None = None

    def fetch_rows(self, *, col_names: set[str] | None, after: dict[str, typing.Hashable] | None) -> list[data.Row]:
        if col_names:
            cols = sorted(col_names)
        else:
            cols = sorted({c.name for c in self.get_table().columns})

        sql = "SELECT\n  "
        sql += "\n, ".join(_wrap_col_name_w_alias(col_name=col) for col in cols)
        sql += f"\nFROM {self._full_table_name}"
        if after:
            sql += "\nWHERE\n  " + "\n  OR ".join(f"{_wrapper(key)} > %(key)s" for key in after.keys())
            self._cur.execute(sql, after)
        else:
            self._cur.execute(sql)

        return [dict(zip(cols, row)) for row in self._cur.fetchall()]

    def fetch_rows_by_key(self, *, col_names: set[str] | None, keys: set[data.RowKey]) -> list[data.Row]:
        if keys:
            if col_names:
                cols = sorted(col_names)
            else:
                cols = sorted({c.name for c in self.get_table().columns})

            sql = "SELECT\n  "
            sql += "\n, ".join(_wrap_col_name_w_alias(col_name=col) for col in cols)
            sql += f"\nFROM {self._full_table_name}"

            sql += "\nWHERE\n  "
            key_cols = sorted(next(itertools.islice(keys, 1)).keys())
            sql += "\n  AND ".join(f"{_wrapper(key_col)} = %(key_col)s" for key_col in key_cols)

            params = [
                {
                    key_col: row[key_col]
                    for key_col in key_cols
                }
                for row in keys
            ]

            self._cur.executemany(sql, params)
            return [dict(row) for row in self._cur.fetchall()]

        return []

    def get_row_count(self) -> int:
        if self._schema_name:
            full_table_name = f"{_wrapper(self._schema_name)}.{_wrapper(self._table_name)}"
        else:
            full_table_name = _wrapper(self._table_name)

        self._cur.execute(f"SELECT COUNT(*) AS ct FROM {full_table_name};")
        return self._cur.fetchone()["ct"]  # noqa

    def get_table(self) -> data.Table:
        if self._table is not None:
            return self._table

        if not self.table_exists():
            raise data.error.TableDoesntExist(table_name=self._table_name, schema_name=self._schema_name)

        sql = textwrap.dedent(
            """
            SELECT
                c.column_name
            ,   c.is_nullable = 'YES' AS nullable
            ,   c.data_type
            ,   c.character_maximum_length AS max_length
            ,   c.numeric_precision AS precision
            ,   c.numeric_scale AS scale
            FROM information_schema.columns AS c
            WHERE
                c.table_schema = %(schema_name)s
                AND c.table_name = %(table_name)s
            ;
            """
        ).strip()
        self._cur.execute(sql, {"schema_name": self._schema_name, "table_name": self._table_name})

        cols: list[data.Column] = []
        if result := self._cur.fetchall():
            result = typing.cast(list[dict[str, typing.Any]], result)
            for row in result:
                col = data.Column(
                    name=row["column_name"],
                    data_type=_lookup_data_type(row["data_type"]),
                    nullable=row["nullable"],
                    length=row["max_length"],
                    precision=row["precision"],
                    scale=row["scale"],
                )
                cols.append(col)
        else:
            raise data.error.TableDoesntExist(schema_name=self._schema_name, table_name=self._table_name)

        pk = _get_pk_for_table(
            cur=self._cur,
            schema_name=self._schema_name,
            table_name=self._table_name,
        )

        assert pk is not None, f"No primary key was found for the table, {self._schema_name}.{self._table_name}."

        return data.Table(
            db_name=self._db_name,
            schema_name=self._schema_name,
            table_name=self._table_name,
            pk=pk,
            columns=frozenset(cols),
        )

    def table_exists(self) -> bool:
        self._cur.execute(
            """
            SELECT 
                COUNT(*) AS ct 
            FROM information_schema.tables AS t
            WHERE
                t.table_schema = %(schema_name)s
                AND t.table_name = %(table_name)s
            ;
            """,
            {"schema_name": self._schema_name, "table_name": self._table_name}
        )
        return self._cur.fetchone()["ct"] > 0  # noqa


def _get_pk_for_table(*, cur: RealDictCursor, schema_name: str, table_name: str) -> tuple[str, ...] | None:
    sql = textwrap.dedent("""
        SELECT
            c.column_name
        ,   c.data_type
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.constraint_column_usage AS ccu
            ON tc.constraint_schema = ccu.constraint_schema
            AND tc.constraint_name = ccu.constraint_name
        JOIN information_schema.columns AS c
            ON c.table_schema = tc.constraint_schema
            AND tc.table_name = c.table_name
            AND ccu.column_name = c.column_name
        WHERE
            constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = %(schema_name)s
            AND tc.table_name = %(table_name)s
        ;
    """).strip()
    cur.execute(sql, {"schema_name": schema_name, "table_name": table_name})
    if result := cur.fetchall():
        result = typing.cast(list[dict[str, typing.Any]], result)
        return tuple(row["column_name"] for row in result)
    return None


def _lookup_data_type(type_name: str, /) -> data.DataType:
    if type_name in {
        "anyarray", "ARRAY", "bytea", "inet", "interval", "jsonb", "name", "pg_dependencies",
        "pg_lsn", "pg_mcv_list", "pg_ndistinct", "pg_node_tree", "regproc", "regtype",
        "USER-DEFINED", "xid",
    }:
        raise NotImplementedError(f"The data type {type_name} is not supported.")

    return {
        '"char"': data.DataType.Text,
        'bigint': data.DataType.BigInt,
        'boolean': data.DataType.Bool,
        'character': data.DataType.Text,
        'character varying': data.DataType.Text,
        'date': data.DataType.Date,
        'double precision': data.DataType.BigInt,
        'integer': data.DataType.Int,
        'numeric': data.DataType.Decimal,
        'oid': data.DataType.Int,
        'real': data.DataType.Float,
        'smallint': data.DataType.Int,
        'text': data.DataType.Text,
        'timestamp with time zone': data.DataType.TimestampTZ,
        'timestamp without time zone': data.DataType.Timestamp,
    }[type_name]


def _wrapper(name: str, /) -> str:
    return f'"{name}"'


def _wrap_col_name_w_alias(*, col_name: str) -> str:
    if col_name.lower() == col_name:
        return _wrapper(col_name)
    return f"{_wrapper(col_name)} AS {_wrapper(col_name).lower()}"

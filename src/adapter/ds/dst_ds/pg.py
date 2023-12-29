from __future__ import annotations

import dataclasses
import datetime
import itertools
import operator
import typing

from src import data
from src.adapter.ds import shared

__all__ = ("PgDstDs",)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection,SqlResolve
class PgDstDs(data.DstDs):
    def __init__(
        self,
        *,
        cur: data.Cursor,
        dst_db_name: str,
        dst_schema_name: str | None,
        dst_table_name: str,
        src_table: data.Table,
        after: dict[str, datetime.date],
    ):
        self._cur: typing.Final[data.Cursor] = cur
        self._src_table: typing.Final[data.Table] = src_table
        self._after: typing.Final[dict[str, datetime.date]] = after

        self._dst_table: typing.Final[data.Table] = dataclasses.replace(
            src_table,
            db_name=dst_db_name,
            schema_name=dst_schema_name,
            table_name=dst_table_name,
        )

        self._full_table_name: typing.Final[str] = _generate_full_table_name(
            schema_name=self._dst_table.schema_name,
            table_name=self._dst_table.table_name,
        )
        self._history_table_name: typing.Final[str] = _generate_full_table_name(
            schema_name=self._dst_table.schema_name,
            table_name=self._dst_table.table_name + "_history",
        )
        self._staging_table_name: typing.Final[str] = _generate_full_table_name(
            schema_name=self._dst_table.schema_name,
            table_name=self._dst_table.table_name + "_staging",
        )

    def add_check_result(self, /, result: data.CheckResult) -> None | data.Error:
        try:
            sql = """
                CALL poa.add_check_result (
                    p_src_db_name := %s
                ,   p_src_schema_name := %s
                ,   p_src_table_name := %s
                ,   p_dst_db_name := %s
                ,   p_dst_schema_name := %s
                ,   p_dst_table_name := %s
                ,   p_src_rows := %s 
                ,   p_dst_rows := %s
                ,   p_extra_keys := %s 
                ,   p_missing_keys := %s
                ,   p_execution_millis := %s
                )
            """

            return self._cur.execute(
                sql=sql,
                params=(
                    result.src_db_name,
                    result.src_schema_name,
                    result.src_table_name,
                    result.dst_db_name,
                    result.dst_schema_name,
                    result.dst_table_name,
                    result.src_rows,
                    result.dst_rows,
                    tuple(result.extra_keys or ()),
                    tuple(result.missing_keys or ()),
                    result.execution_millis,
                ),
            )
        except Exception as e:
            return data.Error.new(
                str(e),
                table_name=self._full_table_name,
                result=result,
            )

    def add_rows_to_staging(self, /, rows: typing.Iterable[data.Row]) -> None | data.Error:
        try:
            truncate_result = self._cur.execute(
                sql=f"TRUNCATE {self._staging_table_name}",
                params=None,
            )
            if isinstance(truncate_result, data.Error):
                return truncate_result

            col_names = sorted({c.name for c in self._dst_table.columns})
            col_name_csv = ", ".join(_wrap_name(c) for c in col_names)
            col_placeholders = ", ".join("%s" for _ in col_names)

            hd_cols = [c for c in col_names if c not in self._dst_table.pk]
            hd_col_csv = ", ".join("%s" for _ in hd_cols)
            hd_placeholder = f"md5(row({hd_col_csv})::TEXT)"

            params: list[list[typing.Hashable]] = []
            for row in rows:
                row_params = [row[col] for col in col_names]
                row_params += [row[col] for col in hd_cols]
                params.append(row_params)

            sql = f"""
                INSERT INTO {self._staging_table_name} ({col_name_csv}, poa_op, poa_hd)
                VALUES ({col_placeholders}, 'a', '{hd_placeholder}')
                ON CONFLICT DO NOTHING
            """

            return self._cur.execute_many(sql=sql, params=params)
        except Exception as e:
            return data.Error.new(str(e), table_name=self._full_table_name)

    def add_increasing_col_indices(
        self, /, increasing_cols: typing.Iterable[str]
    ) -> None | data.Error:
        try:
            for col in increasing_cols:
                add_index_result = self._cur.execute(
                    sql=(
                        f"CREATE INDEX IF NOT EXISTS ix_{self._dst_table.table_name}_{col} "
                        f"ON {self._full_table_name} ({_wrap_name(col)} DESC)"
                    ),
                    params=None,
                )
                if isinstance(add_index_result, data.Error):
                    return add_index_result

            return None
        except Exception as e:
            return data.Error.new(
                str(e),
                table_name=self._full_table_name,
                increasing_cols=tuple(increasing_cols),
            )

    def create(self) -> None | data.Error:
        try:
            sql = f"CREATE TABLE {self._full_table_name} (\n  "
            sql += "\n, ".join(
                _generate_column_definition(col=col)
                for col in sorted(self._dst_table.columns, key=operator.attrgetter("name"))
            )
            sql += (
                "\n, poa_hd CHAR(32) NOT NULL"
                "\n, poa_op CHAR(1) NOT NULL CHECK (poa_op IN ('a', 'd', 'u'))"
                "\n, poa_ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()"
                "\n, PRIMARY KEY (" + ", ".join(_wrap_name(col) for col in self._dst_table.pk) + ")"
                "\n)"
            )
            create_table_result = self._cur.execute(sql=sql, params=None)
            if isinstance(create_table_result, data.Error):
                return create_table_result

            poa_ts_index_result = self._cur.execute(
                sql=f"CREATE INDEX ix_{self._dst_table.table_name}_poa_ts ON {self._full_table_name} (poa_ts DESC)",
                params=None,
            )
            if isinstance(poa_ts_index_result, data.Error):
                return poa_ts_index_result

            poa_op_index_result = self._cur.execute(
                sql=f"CREATE INDEX ix_{self._dst_table.table_name}_poa_op ON {self._full_table_name} (poa_op)",
                params=None,
            )
            if isinstance(poa_op_index_result, data.Error):
                return poa_ts_index_result

            return None
        except Exception as e:
            return data.Error.new(str(e), table_name=self._full_table_name)

    def create_history_table(self) -> None | data.Error:
        try:
            history_table_name = _generate_full_table_name(
                schema_name=self._dst_table.schema_name,
                table_name=self._dst_table.table_name + "_history",
            )

            sql = f"CREATE TABLE IF NOT EXISTS {history_table_name} (\n  "
            sql += "\n, ".join(
                _generate_column_definition(col=col)
                for col in sorted(self._dst_table.columns, key=operator.attrgetter("name"))
            )
            sql += (
                "\n, poa_hd CHAR(32) NOT NULL"
                "\n, poa_op CHAR(1) NOT NULL CHECK (poa_op IN ('a', 'd', 'u'))"
                "\n, poa_ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()"
                "\n, PRIMARY KEY ("
                + ", ".join(_wrap_name(col) for col in self._dst_table.pk)
                + ", poa_ts)"
                "\n)"
            )

            create_table_result = self._cur.execute(sql=sql, params=None)
            if isinstance(create_table_result, data.Error):
                return create_table_result

            poa_ts_index_result = self._cur.execute(
                sql=(
                    f"CREATE INDEX IF NOT EXISTS ix_{self._dst_table.table_name}_history_poa_ts "
                    f"ON {history_table_name} (poa_ts DESC)"
                ),
                params=None,
            )
            if isinstance(poa_ts_index_result, data.Error):
                return poa_ts_index_result

            poa_op_result = self._cur.execute(
                sql=(
                    f"CREATE INDEX IF NOT EXISTS ix_{self._dst_table.table_name}_history_poa_op "
                    f"ON {history_table_name} (poa_op)"
                ),
                params=None,
            )
            if isinstance(poa_op_result, data.Error):
                return poa_op_result

            return None
        except Exception as e:
            return data.Error.new(str(e), table_name=self._full_table_name)

    def create_staging_table(self) -> None | data.Error:
        try:
            staging_table_name = _generate_full_table_name(
                schema_name=self._dst_table.schema_name,
                table_name=self._dst_table.table_name + "_staging",
            )

            sql = f"CREATE TABLE IF NOT EXISTS {staging_table_name} (\n  "
            sql += "\n, ".join(
                _generate_column_definition(col=col)
                for col in sorted(self._dst_table.columns, key=operator.attrgetter("name"))
            )
            sql += (
                "\n, poa_hd CHAR(32) NOT NULL"
                "\n, poa_op CHAR(1) NOT NULL CHECK (poa_op IN ('a', 'd', 'u'))"
                "\n, poa_ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()"
                "\n, PRIMARY KEY ("
                + ", ".join(_wrap_name(col) for col in self._dst_table.pk)
                + ", poa_ts)"
                "\n)"
            )

            create_table_result = self._cur.execute(sql=sql, params=None)
            if isinstance(create_table_result, data.Error):
                return create_table_result

            poa_ts_index_result = self._cur.execute(
                sql=(
                    f"CREATE INDEX IF NOT EXISTS ix_{self._dst_table.table_name}_staging_poa_ts "
                    f"ON {staging_table_name} (poa_ts DESC)"
                ),
                params=None,
            )
            if isinstance(poa_ts_index_result, data.Error):
                return poa_ts_index_result

            poa_op_result = self._cur.execute(
                sql=(
                    f"CREATE INDEX IF NOT EXISTS ix_{self._dst_table.table_name}_staging_poa_op "
                    f"ON {staging_table_name} (poa_op)"
                ),
                params=None,
            )
            if isinstance(poa_op_result, data.Error):
                return poa_op_result

            return None
        except Exception as e:
            return data.Error.new(str(e), table_name=self._full_table_name)

    def delete_rows(self, /, keys: typing.Iterable[data.RowKey]) -> None | data.Error:
        try:
            if keys:
                first_key: data.RowKey = next(itertools.islice(keys, 1))

                key_cols = sorted(first_key.keys())
                where_clause = " AND ".join(f"{_wrap_name(c)} = %s" for c in key_cols)
                sql = f"""
                    UPDATE {self._full_table_name}
                    SET 
                        poa_op = 'd'
                    ,   poa_ts = now()
                    WHERE 
                        {where_clause}
                        AND poa_op <> 'd'
                """

                return self._cur.execute_many(
                    sql=sql,
                    params=[list(key.values()) for key in keys],
                )
        except Exception as e:
            return data.Error.new(
                str(e),
                table_name=self._full_table_name,
                key=tuple(keys),
            )

    def drop_table(self) -> None | data.Error:
        return self._cur.execute(
            sql=f"DROP TABLE IF EXISTS {self._full_table_name}",
            params=None,
        )

    def fetch_rows(
        self,
        *,
        col_names: set[str] | None,
        after: dict[str, typing.Hashable] | None,
    ) -> tuple[data.Row, ...] | data.Error:
        try:
            if col_names:
                cols = sorted(set(col_names))
            else:
                cols = sorted({c.name for c in self._dst_table.columns})

            full_table_name = _generate_full_table_name(
                schema_name=self._dst_table.schema_name,
                table_name=self._dst_table.table_name,
            )

            sql = "SELECT "
            sql += ", ".join(_wrap_name(col) for col in cols)
            sql += f"FROM {full_table_name}"

            full_after = shared.combine_filters(ds_filter=self._after, query_filter=after)

            sql += "WHERE poa_op <> 'd'"

            if full_after:
                sql += (
                    " AND ("
                    + " OR ".join(f"{_wrap_name(key)} > %s" for key, val in full_after.items())
                    + ")"
                )

            return self._cur.execute(
                sql=sql,
                params=full_after.values(),
            )
        except Exception as e:
            return data.Error.new(
                str(e),
                table_name=self._full_table_name,
                col_names=tuple(col_names),
                after=tuple((after or {}).items()),
            )

    def get_max_values(
        self, /, col_names: typing.Iterable[str]
    ) -> dict[str, typing.Hashable] | None | data.Error:
        try:
            max_values: dict[str, typing.Hashable] = {}
            for col in sorted(col_names):
                sql = f"SELECT max({_wrap_name(col)}) AS v FROM {self._full_table_name}"

                row = self._cur.fetch_one(sql=sql, params=None)
                if isinstance(row, data.Error):
                    return row

                if row is not None:
                    max_values[col] = row["v"]

            if max_values:
                return max_values

            return None
        except Exception as e:
            return data.Error.new(
                str(e),
                table_name=self._full_table_name,
                col_names=tuple(col_names),
            )

    def get_row_count(self) -> int | data.Error:
        try:
            sql = f"SELECT count(*) AS ct FROM {self._full_table_name} WHERE poa_op <> 'd'"

            if self._after:
                sql += (
                    "AND ("
                    + " OR ".join(f"{_wrap_name(key)} > %s" for key in self._after.keys())
                    + ")"
                )
                row = self._cur.fetch_one(sql=sql, params=self._after.values())
            else:
                row = self._cur.fetch_one(sql=sql, params=None)

            if isinstance(row, data.Error):
                return row

            return typing.cast(int, row["ct"])
        except Exception as e:
            return data.Error.new(str(e), table_name=self._full_table_name)

    def table_exists(self) -> bool | data.Error:
        try:
            row = self._cur.fetch_one(
                sql="""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables AS t
                        WHERE
                            t.table_schema = %s
                            AND t.table_name = %s
                    ) AS tbl_exists
                """,
                params=(self._dst_table.schema_name, self._dst_table.table_name),
            )
            if isinstance(row, data.Error):
                return row

            if row is None:
                return data.Error.new(
                    "Somehow the table_exists() query returned None.",
                    table_name=self._full_table_name,
                )

            return typing.cast(bool, row["tbl_exists"])
        except Exception as e:
            return data.Error.new(str(e), table_name=self._full_table_name)

    def truncate(self) -> None | data.Error:
        return self._cur.execute(sql=f"TRUNCATE {self._full_table_name}", params=None)

    def update_history_table(self) -> None | data.Error:
        try:
            col_names = sorted({c.name for c in self._dst_table.columns}) + [
                "poa_hd",
                "poa_op",
                "poa_ts",
            ]

            col_name_csv = ", ".join(_wrap_name(c) for c in col_names)

            pks_match = " AND ".join(
                f"d.{col} = h.{col}" for col in self._dst_table.pk + ("poa_ts",)
            )

            sql = f"""
                INSERT INTO {self._history_table_name} (
                    {col_name_csv}
                )
                SELECT
                    {col_name_csv}
                FROM {self._full_table_name} AS d
                WHERE
                    NOT EXISTS (
                        SELECT 1 
                        FROM {self._history_table_name} AS h
                        WHERE
                            {pks_match}
                    )
            """

            return self._cur.execute(sql=sql, params=None)
        except Exception as e:
            return data.Error.new(str(e), table_name=self._full_table_name)

    def upsert_rows_from_staging(self, /, rows: typing.Iterable[data.Row]) -> None | data.Error:
        try:
            if not rows:
                return None

            col_names = sorted({c.name for c in self._dst_table.columns})

            def col_name_csv(prefix: str, /) -> str:
                return ", ".join(prefix + _wrap_name(c) for c in col_names)

            pk_csv = ", ".join(_wrap_name(c) for c in self._dst_table.pk)

            set_values_csv = (
                ", ".join(
                    f"{_wrap_name(c)} = EXCLUDED.{_wrap_name(c)}"
                    for c in col_names
                    if c not in self._dst_table.pk
                )
                + ", poa_hd = EXCLUDED.poa_hd, poa_op = 'u', poa_ts = now()"
            )

            sql = f"""
                INSERT INTO {self._full_table_name} (
                    {col_name_csv('')}
                )
                SELECT 
                    {col_name_csv('stg.')}
                FROM {self._staging_table_name} AS stg
                ON CONFLICT ({pk_csv})
                DO UPDATE SET 
                    {set_values_csv}
                WHERE
                    {self._full_table_name}.poa_hd <> EXCLUDED.poa_hd
                    OR {self._full_table_name}.poa_op = 'd'
                RETURNING poa_op
            """

            return self._cur.execute(sql=sql, params=None)
        except Exception as e:
            return data.Error.new(str(e), table_name=self._full_table_name)


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
            f"{col_name} NUMERIC({18 if col.precision is None else col.precision}, "
            f"{4 if col.scale is None else col.scale}) {nullable}"
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

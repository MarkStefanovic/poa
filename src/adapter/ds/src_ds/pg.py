import datetime
import pathlib
import textwrap
import typing

from src import data
from src.adapter.ds import shared

__all__ = ("PgSrcDs",)


class PgSrcDs(data.SrcDs):
    def __init__(
        self,
        *,
        cur: data.Cursor,
        db_name: str,
        schema_name: str,
        table_name: str,
        after: dict[str, datetime.date],
    ):
        self._cur: typing.Final[data.Cursor] = cur
        self._db_name: typing.Final[str] = db_name
        self._schema_name: typing.Final[str] = schema_name
        self._table_name: typing.Final[str] = table_name
        self._after: typing.Final[dict[str, datetime.date]] = after

        if self._schema_name:
            self._full_table_name = f"{_wrapper(self._schema_name)}.{_wrapper(table_name)}"
        else:
            self._full_table_name = _wrapper(table_name)

        self._table: data.Table | None = None

    def fetch_rows(
        self,
        *,
        col_names: set[str] | None,
        after: dict[str, typing.Hashable] | None,
    ) -> tuple[data.Row, ...] | data.Error:
        if col_names:
            cols = sorted(col_names)
        else:
            cols = sorted({c.name for c in self.get_table().columns})

        sql = "SELECT\n  "
        sql += "\n, ".join(_wrap_col_name_w_alias(col_name=col) for col in cols)
        sql += f"\nFROM {self._full_table_name}"

        full_after = shared.combine_filters(ds_filter=self._after, query_filter=after)

        if full_after:
            sql += "\nWHERE\n  " + "\n  OR ".join(
                f"{_wrapper(key)} > %s" for key in full_after.keys()
            )
            return self._cur.fetch_all(sql=sql, params=full_after.values())
        else:
            return self._cur.fetch_all(sql=sql, params=None)

    def fetch_rows_by_key(
        self,
        *,
        col_names: typing.Iterable[str] | None,
        keys: typing.Iterable[data.RowKey],
    ) -> tuple[data.Row, ...] | data.Error:
        if keys:
            if col_names:
                cols = sorted(col_names)
            else:
                table = self.get_table()
                if isinstance(table, data.Error):
                    return table

                cols = sorted({c.name for c in table.columns})

            sql = _compose_fetch_rows_by_key_sql(
                full_table_name=self._full_table_name,
                cols=cols,
                keys=tuple(keys),
            )
            if isinstance(sql, data.Error):
                return sql

            return self._cur.fetch_all(
                sql=sql,
                params=[val for key in keys for val in key.values()],
            )

        return tuple()

    def get_row_count(self) -> int | data.Error:
        try:
            sql = f"SELECT count(*) AS ct FROM {self._full_table_name}"

            if self._after:
                sql += "WHERE " + " OR ".join(f"{_wrapper(key)} > %s" for key in self._after.keys())

                row = self._cur.fetch_one(
                    sql=sql,
                    params=self._after.values(),
                )
            else:
                row = self._cur.fetch_one(sql=sql, params=None)

            if isinstance(row, data.Error):
                return row

            if row is None:
                return data.Error.new(
                    "Somehow get_row_count query returned None.  That should be impossible.",
                    schema_name=self._schema_name,
                    table_name=self._table_name,
                )

            return typing.cast(int, row["ct"])
        except Exception as e:
            return data.Error.new(
                str(e),
                schema_name=self._schema_name,
                table_name=self._table_name,
            )

    def get_table(self) -> data.Table | data.Error:
        if self._table is not None:
            return self._table

        if not self.table_exists():
            raise data.Error.new(
                f"The table, {self._full_table_name}, does not exist.",
                schema=self._schema_name,
                table=self._table_name,
            )

        sql = """
            SELECT
                c.column_name
            ,   c.is_nullable = 'YES' AS nullable
            ,   c.data_type
            ,   c.character_maximum_length AS max_length
            ,   c.numeric_precision AS precision
            ,   c.numeric_scale AS scale
            FROM information_schema.columns AS c
            WHERE
                c.table_schema = %s
                AND c.table_name = %s
        """

        information_schema_columns = self._cur.fetch_all(
            sql=sql,
            params=(self._schema_name, self._table_name),
        )
        if isinstance(information_schema_columns, data.Error):
            return information_schema_columns

        cols: list[data.Column] = []
        if information_schema_columns:
            for row in information_schema_columns:
                col = data.Column(
                    name=typing.cast(str, row["column_name"]),
                    data_type=_lookup_data_type(typing.cast(str, row["data_type"])),
                    nullable=typing.cast(bool, row["nullable"]),
                    length=typing.cast(int, row["max_length"]),
                    precision=typing.cast(int, row["precision"]),
                    scale=typing.cast(int, row["scale"]),
                )
                cols.append(col)
        else:
            raise data.Error.new(
                f"{self._full_table_name} does not exist.",
                schema_name=self._schema_name,
                table_name=self._table_name,
            )

        pk = _get_pk_for_table(
            cur=self._cur,
            schema_name=self._schema_name,
            table_name=self._table_name,
        )
        if isinstance(pk, data.Error):
            return pk

        return data.Table(
            db_name=self._db_name,
            schema_name=self._schema_name,
            table_name=self._table_name,
            pk=pk,
            columns=frozenset(cols),
        )

    def table_exists(self) -> bool | data.Error:
        try:
            result = self._cur.fetch_one(
                sql="""
                    SELECT 
                        count(*) AS ct 
                    FROM information_schema.tables AS t
                    WHERE
                        t.table_schema = %s
                        AND t.table_name = %s
                """,
                params=(self._schema_name, self._table_name),
            )
            if isinstance(result, data.Error):
                return result

            if result is None:
                return data.Error.new(
                    (
                        f"Somehow the result of table_exists is None for table, {self._full_table_name}.  "
                        f"That should be impossible."
                    ),
                    schema_name=self._schema_name,
                    table_name=self._table_name,
                )

            return typing.cast(int, result["ct"]) > 0
        except Exception as e:
            return data.Error.new(str(e))


def _get_pk_for_table(
    *,
    cur: data.Cursor,
    schema_name: str,
    table_name: str,
) -> tuple[str, ...] | data.Error:
    sql = """
        SELECT
            c.column_name
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
            AND tc.table_schema = %s
            AND tc.table_name = %s
    """

    rows = cur.fetch_all(
        sql=sql,
        params=(schema_name, table_name),
    )
    if isinstance(rows, data.Error):
        return rows

    if rows:
        return tuple(typing.cast(str, row["column_name"]) for row in rows)

    return data.Error.new(
        f"The table, {schema_name}.{table_name} was not found.",
        schema_name=schema_name,
        table_name=table_name,
    )


def _lookup_data_type(type_name: str, /) -> data.DataType:
    if type_name in {
        "anyarray",
        "ARRAY",
        "bytea",
        "inet",
        "interval",
        "jsonb",
        "name",
        "pg_dependencies",
        "pg_lsn",
        "pg_mcv_list",
        "pg_ndistinct",
        "pg_node_tree",
        "regproc",
        "regtype",
        "USER-DEFINED",
        "xid",
    }:
        raise NotImplementedError(f"The data type {type_name} is not supported.")

    return {
        "char": data.DataType.Text,
        "bigint": data.DataType.BigInt,
        "boolean": data.DataType.Bool,
        "character": data.DataType.Text,
        "character varying": data.DataType.Text,
        "date": data.DataType.Date,
        "double precision": data.DataType.BigInt,
        "integer": data.DataType.Int,
        "numeric": data.DataType.Decimal,
        "oid": data.DataType.Int,
        "real": data.DataType.Float,
        "smallint": data.DataType.Int,
        "text": data.DataType.Text,
        "timestamp with time zone": data.DataType.TimestampTZ,
        "timestamp without time zone": data.DataType.Timestamp,
    }[type_name]


def _wrapper(name: str, /) -> str:
    return f'"{name}"'


def _wrap_col_name_w_alias(*, col_name: str) -> str:
    if col_name.lower() == col_name:
        return _wrapper(col_name)
    return f"{_wrapper(col_name)} AS {_wrapper(col_name).lower()}"


def _compose_fetch_rows_by_key_sql(
    *,
    full_table_name: str,
    cols: typing.Iterable[str],
    keys: typing.Sequence[data.RowKey],
) -> str | data.Error:
    try:
        if not keys:
            return data.Error.new(
                "No keys were provided.",
                full_table_name=full_table_name,
                cols=tuple(cols),
            )

        key_fields = keys[0].keys()

        def key_fields_csv(prefix: str, /) -> str:
            return ", ".join(prefix + _wrap_col_name_w_alias(col_name=col) for col in key_fields)

        key_value_csv = "(" + ", ".join("%s" for _ in range(len(key_fields))) + ")"
        key_values_csv = ", ".join(key_value_csv for _ in range(len(keys)))

        col_name_csv = ", ".join("t." + _wrap_col_name_w_alias(col_name=col) for col in cols)

        return f"""
            WITH values ({key_fields_csv('')}) AS (
                VALUES {key_values_csv}
            )
            SELECT {col_name_csv}
            FROM {full_table_name} AS t
            JOIN values AS v 
              ON ({key_fields_csv('t.')}) 
                 IS NOT DISTINCT FROM 
                 ({key_fields_csv('v.')})
        """
    except Exception as e:
        return data.Error.new(
            str(e),
            full_table_name=full_table_name,
            cols=tuple(cols),
            keys=keys,
        )


# def _compose_select_rows_query(
#     *,
#     schema_name: str | None,
#     table_name: str,
#     after: dict[str, datetime.date],
# ) -> tuple[sql.Composed, tuple[datetime.date, ...] | None]:
#     if after:
#         sorted_after = sorted((key, val) for key, val in after.items() if val is not None)
#
#         sorted_keys = [itm[0] for itm in sorted_after]
#
#         criteria: sql.Composed = sql.SQL(" OR ").join(
#             [sql.SQL("{key} > ?").format(key=sql.Identifier(key)) for key in sorted_keys]
#         )
#
#         params: tuple[datetime.date, ...] | None = tuple(itm[1] for itm in sorted_after)
#
#         if schema_name:
#             # noinspection SqlDialectInspection,SqlNoDataSourceInspection
#             qry = sql.SQL("SELECT count(*) AS ct FROM {schema}.{table} WHERE {criteria};").format(
#                 schema=sql.Identifier(schema_name),
#                 table=sql.Identifier(table_name),
#                 criteria=criteria,
#             )
#         else:
#             # noinspection SqlDialectInspection,SqlNoDataSourceInspection
#             qry = sql.SQL("SELECT count(*) AS ct FROM {table} WHERE {criteria};").format(
#                 table=sql.Identifier(table_name),
#                 criteria=criteria,
#             )
#
#         return qry, params
#     else:
#         if schema_name:
#             # noinspection SqlDialectInspection,SqlNoDataSourceInspection
#             qry = sql.SQL("SELECT count(*) AS ct FROM {schema}.{table};").format(
#                 schema=sql.Identifier(schema_name),
#                 table=sql.Identifier(table_name),
#             )
#
#             return qry, None
#         else:
#             # noinspection SqlDialectInspection,SqlNoDataSourceInspection
#             qry = sql.SQL("SELECT count(*) AS ct FROM {table};").format(
#                 table=sql.Identifier(table_name),
#             )
#
#         return qry, None


# if __name__ == "__main__":
#     with psycopg.connect(database="")
#     q, p = _compose_select_rows_query(
#         schema_name="hhr",
#         table_name="activity",
#         after={"created_date": datetime.date(2023, 11, 1)},
#     )
#     q_str = q.as_string(cn)
#     print(f"query: {q}")
#     print(f"params: {p}")


if __name__ == "__main__":
    from src.adapter.cursor_provider.pg import PgCursorProvider
    from src.adapter.config import load

    cfg = load(config_file=pathlib.Path(r"C:\bu\py\poa\assets\config.json"))
    if isinstance(cfg, data.Error):
        raise Exception(str(cfg))

    db_cfg = cfg.db("dw")
    if isinstance(db_cfg, data.Error):
        raise Exception(str(db_cfg))

    cp = PgCursorProvider(db_config=db_cfg)
    with cp.open() as cr:
        if isinstance(cr, data.Error):
            raise Exception(str(cr))

        ds = PgSrcDs(
            cur=cr,
            db_name=db_cfg.db_name,
            schema_name="hhr",
            table_name="activity",
            after={},
        )

        # res = ds.fetch_rows_by_key(
        #     col_names=["activity_id", "description"],
        #     keys=[
        #         data.FrozenDict({"activity_id": 1138}),
        #         data.FrozenDict({"activity_id": 1143}),
        #     ],
        # )

        # res = ds.get_row_count()

        # res = _get_pk_for_table(
        #     cur=cr,
        #     schema_name="hhr",
        #     table_name="activity",
        # )

        res = ds.get_table()

        print(res)

    # q = _compose_fetch_rows_by_key_sql(
    #     full_table_name="dbo.test",
    #     cols=(
    #         "id",
    #         "first_name",
    #         "middle_name",
    #         "last_name",
    #     ),
    #     keys=[
    #         data.FrozenDict(
    #             {
    #                 "id": i,
    #                 "first_name": f"first_name_{i}",
    #                 "middle_name": f"middle_name_{i}",
    #                 "last_name": f"last_name_{i}",
    #             }
    #         )
    #         for i in range(100)
    #     ],
    # )
    # print(q)

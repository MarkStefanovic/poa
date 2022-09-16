from psycopg2.extras import RealDictCursor

from src import data

__all__ = ("PgCache",)


class PgCache(data.Cache):
    def __init__(self, *, cur: RealDictCursor):
        self._cur = cur

    def add_table_def(self, /, table: data.Table) -> None:
        self._cur.execute(
            """
            SELECT * 
            FROM poa.add_table_def (
                p_db_name := %(db_name)s
            ,   p_schema_name := %(schema_name)s
            ,   p_table_name := %(table_name)s
            ,   p_pk_cols := %(pk)s
            );
            """,
            {
                "db_name": table.db_name,
                "schema_name": table.schema_name,
                "table_name": table.table_name,
                "pk": table.pk,
            }
        )
        table_def_id = self._cur.fetchone()["add_table_def"]  # noqa
        self._cur.executemany(
            """
            PERFORM poa.add_col_def(
                p_table_def_id := %(table_def_id)s
            ,   p_col_name := %(name)s
            ,   p_col_data_type := %(data_type)s
            ,   p_col_length := %(length)s
            ,   p_col_precision := %(precision)s
            ,   p_col_scale := %(scale)s
            ,   p_col_nullable := %(nullable)s
            );
            """,
            [
                {
                    "table_def_id": table_def_id,
                    "name": col.name,
                    "data_type": {
                        data.DataType.BigFloat: "big_float",
                        data.DataType.BigInt: "big_int",
                        data.DataType.Bool: "bool",
                        data.DataType.Date: "date",
                        data.DataType.Decimal: "decimal",
                        data.DataType.Float: "float",
                        data.DataType.Int: "int",
                        data.DataType.Text: "text",
                        data.DataType.Timestamp: "timestamp",
                        data.DataType.TimestampTZ: "timestamptz",
                        data.DataType.UUID: "uuid",
                    }[col.data_type],
                    "length": col.length,
                    "precision": col.precision,
                    "scale": col.scale,
                    "nullable": col.nullable,
                }
                for col in table.columns
            ]
        )

    def get_table_def(self, *, db_name: str, schema_name: str | None, table_name: str) -> data.Table:
        self._cur.execute(
            """
            SELECT 
                col_name
            ,   col_data_type
            ,   col_length
            ,   col_precision
            ,   col_scale
            ,   col_nullable
            FROM poa.get_table_cols(
                p_db_name := %(db_name)s
            ,   p_schema_name := %(schema_name)s
            ,   p_table_name := %(table_name)s
            )
            """,
            {
                "db_name": db_name,
                "schema_name": schema_name,
                "table_name": table_name,
            },
        )
        result = self._cur.fetchall()
        col_defs: list[data.Column] = []
        for row in result:
            data_type = {
                "big_float": data.DataType.BigFloat,
                "big_int": data.DataType.BigInt,
                "bool": data.DataType.Bool,
                "date": data.DataType.Date,
                "decimal": data.DataType.Decimal,
                "float": data.DataType.Float,
                "int": data.DataType.Int,
                "text": data.DataType.Text,
                "timestamp": data.DataType.Timestamp,
                "timestamptz": data.DataType.TimestampTZ,
                "uuid": data.DataType.UUID,
            }[row["col_data_type"]]  # noqa
            col_def = data.Column(
                name=row["col_name"],  # noqa
                data_type=data_type,  # noqa
                length=row["col_length"],  # noqa
                precision=row["col_precision"],  # noqa
                scale=row["col_scale"],  # noqa
                nullable=row["col_nullable"],  # noqa
            )
            col_defs.append(col_def)

        self._cur.execute(
            """
            SELECT *
            FROM poa.get_pk(
                p_db_name := %(db_name)s
            ,   p_schema_name := %(schema_name)s
            ,   p_table_name := %(table_name)s
            );
            """
        )
        pk = self._cur.fetchone()["pk"]  # noqa

        return data.Table(
            db_name=db_name,
            schema_name=schema_name,
            table_name=table_name,
            columns=frozenset(col_defs),
            pk=tuple(pk)
        )

import typing

import psycopg

from src import data

__all__ = ("PgCache",)


_DATA_TYPE_DB_NAMES: typing.Final[tuple[tuple[data.DataType, str], ...]] = (
    (data.DataType.BigFloat, "big_float"),
    (data.DataType.BigInt, "big_int"),
    (data.DataType.Bool, "bool"),
    (data.DataType.Date, "date"),
    (data.DataType.Decimal, "decimal"),
    (data.DataType.Float, "float"),
    (data.DataType.Int, "int"),
    (data.DataType.Text, "text"),
    (data.DataType.Timestamp, "timestamp"),
    (data.DataType.TimestampTZ, "timestamptz"),
    (data.DataType.UUID, "uuid"),
)


# noinspection SqlDialectInspection,SqlNoDataSourceInspection
class PgCache(data.Cache):
    def __init__(self, cur: psycopg.Cursor) -> None:
        self._cur: typing.Final[psycopg.Cursor] = cur

    def add_table(self, /, table: data.Table) -> None | data.Error:
        try:
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
                    "pk": list(table.pk),
                },
            )

            table_def_id = self._cur.fetchone()["add_table_def"]  # noqa

            self._cur.executemany(
                """
                SELECT * 
                FROM poa.add_col_def(
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
                ],
            )

            return None
        except Exception as e:
            return data.Error.new(str(e), table=table)

    def get_table_def(
        self,
        *,
        db_name: str,
        schema_name: str | None,
        table_name: str,
    ) -> data.Table | None | data.Error:
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
        if result:
            col_defs: list[data.Column] = []
            for row in result:
                (
                    col_name,
                    col_data_type,
                    col_length,
                    col_precision,
                    col_scale,
                    col_nullable,
                ) = row

                data_type = _get_data_type_for_data_type_db_name(col_data_type)
                if isinstance(data_type, data.Error):
                    return data_type

                col_def = data.Column(
                    name=col_name,
                    data_type=data_type,
                    length=col_length,
                    precision=col_precision,
                    scale=col_scale,
                    nullable=col_nullable,
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
                """,
                {"db_name": db_name, "schema_name": schema_name, "table_name": table_name},
            )

            pk = self._cur.fetchone()[0]

            return data.Table(
                db_name=db_name,
                schema_name=schema_name,
                table_name=table_name,
                columns=frozenset(col_defs),
                pk=tuple(pk),
            )

        return None


def _get_db_name_for_data_type(data_type: data.DataType, /) -> str | data.Error:
    try:
        return next(m[1] for m in _DATA_TYPE_DB_NAMES if m[0] == data_type)
    except StopIteration:
        return data.Error.new(
            f"Could not find db_name for DataType, {data_type!r}.", data_type=data_type
        )
    except Exception as e:
        return data.Error.new(str(e), data_type=data_type)


def _get_data_type_for_data_type_db_name(data_type_db_name: str, /) -> data.DataType | data.Error:
    try:
        return next(m[0] for m in _DATA_TYPE_DB_NAMES if m[1] == data_type_db_name)
    except StopIteration:
        return data.Error.new(
            f"Could not find DataType for db_name, {data_type_db_name!r}.",
            data_type_db_name=data_type_db_name,
        )
    except Exception as e:
        return data.Error.new(str(e), data_type=data_type_db_name)

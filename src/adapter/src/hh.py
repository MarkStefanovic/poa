import datetime
import typing

import pyodbc

__all__ = ("fetch",)


def fetch(
    *,
    con: pyodbc.Connection,
    schema_name: str,
    table_name: str,
    column_names: list[str],
    ts_after: dict[str, datetime.datetime | None],
) -> list[dict[str, typing.Any]]:
    with con.cursor() as cur:
        sql = generate_select_sql(
            schema_name=schema_name,
            table_name=table_name,
            column_names=column_names,
            ts_after=ts_after,
        )
        cur.execute(sql)
        cols = [col[0] for col in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def generate_select_sql(
    *,
    schema_name: str,
    table_name: str,
    column_names: list[str],
    ts_after: dict[str, datetime.datetime | None],
) -> str:
    sql = "SELECT "
    sql += ", ".join(_wrap_name(col) for col in column_names)
    sql += f" FROM {_wrap_name(schema_name)}.{_wrap_name(table_name)}"
    if ts_after:
        valid_ts_criteria = {col: ts for col, ts in ts_after.items() if ts}
        if valid_ts_criteria:
            sql += " WHERE " + " OR ".join(
                f"{_wrap_name(col)} > {_render_dt(dt)}"
                for col, dt in valid_ts_criteria.items()
            )
    return sql


def _wrap_name(name: str, /) -> str:
    return f'"{name}"'


def _render_dt(dt: datetime.datetime, /) -> str:
    return "'" + dt.strftime("%Y-%m-%d %H:%M:%S") + "'"


if __name__ == '__main__':
    sql = generate_select_sql(
        schema_name="dbo",
        table_name="activity",
        column_names=["activity_id_", "changed_date", "code", "name", "created_date"],
        ts_after={
            "created_date": datetime.datetime(2011, 12, 13, 1, 2, 3),
            "changed_date": None,
        }
    )
    print(sql)

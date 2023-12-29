import typing

import psycopg
import pyodbc

from src import data

__all__ = (
    "execute",
    "execute_many",
    "fetch_all",
    "fetch_one",
)


def execute(
    cur: pyodbc.Cursor | psycopg.Cursor,
    sql: str,
    params: typing.Iterable[typing.Hashable] | None,
) -> None | data.Error:
    try:
        if errors := query_errors(sql=sql, params=params):
            return data.Error.new(
                "\n".join(errors),
                sql=sql,
                params=None if params is None else tuple(params),
            )

        if params:
            cur.execute(sql, list(params))
        else:
            cur.execute(sql)

        return None
    except Exception as e:
        return data.Error.new(
            str(e),
            sql=sql,
            params=None if params is None else tuple(params),
        )


def execute_many(
    *,
    cur: pyodbc.Cursor | psycopg.Cursor,
    sql: str,
    params: typing.Iterable[typing.Iterable[typing.Hashable]] | None,
) -> None | data.Error:
    try:
        if errors := query_errors(sql=sql, params=params):
            return data.Error.new(
                "\n".join(errors),
                sql=sql,
                params=None if params is None else tuple(tuple(p) for p in params),
            )

        if params:
            cur.executemany(sql, list(list(p) for p in params))
        else:
            return data.Error.new("executemany was called with no parameters.")
    except Exception as e:
        return data.Error.new(
            str(e),
            sql=sql,
            params=None if params is None else tuple(tuple(p) for p in params),
        )


def fetch_one(
    *,
    cur: pyodbc.Cursor | psycopg.Cursor,
    sql: str,
    params: typing.Iterable[typing.Hashable] | None,
) -> data.Row | None | data.Error:
    try:
        if errors := query_errors(sql=sql, params=params):
            return data.Error.new(
                "\n".join(errors),
                sql=sql,
                params=None if params is None else tuple(params),
            )

        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)

        if result := cur.fetchone():
            return data.Row(result)

        return None
    except Exception as e:
        return data.Error.new(
            str(e),
            sql=sql,
            params=None if params is None else tuple(params),
        )


def fetch_all(
    *,
    cur: pyodbc.Cursor | psycopg.Cursor,
    sql: str,
    params: typing.Iterable[typing.Hashable] | None,
) -> tuple[data.Row, ...] | data.Error:
    try:
        if errors := query_errors(sql=sql, params=params):
            return data.Error.new(
                "\n".join(errors),
                sql=sql,
                params=None if params is None else tuple(params),
            )

        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)

        result = cur.fetchall()

        return tuple(data.Row(row) for row in result)
    except Exception as e:
        return data.Error.new(
            str(e),
            sql=sql,
            params=None if params is None else tuple(params),
        )


def query_errors(
    *,
    sql: str,
    params: typing.Iterable[typing.Hashable] | None,
) -> list[str]:
    errors: list[str] = []

    if ";" in sql:
        errors.append("; is not allowed in sql queries.")

    if "--" in sql:
        errors.append("-- is not allowed in sql queries.")

    if "/*" in sql:
        errors.append("/* is not allowed in sql queries.")

    if "*/" in sql:
        errors.append("*/ is not allowed in sql queries.")

    if params:
        for param in params:
            if isinstance(param, str):
                if ";" in param:
                    errors.append("; is not allowed in parameters.")

                if "--" in param:
                    errors.append("-- is not allowed in parameters.")

                if "/*" in param:
                    errors.append("/* is not allowed in parameters.")

                if "*/" in param:
                    errors.append("*/ is not allowed in parameters.")

    return errors

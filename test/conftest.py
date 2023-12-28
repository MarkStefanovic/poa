import json
import pathlib
import typing

import psycopg
import pyodbc
import pytest
from psycopg import sql
from psycopg.rows import dict_row


@pytest.fixture(scope="function")
def _root_dir_fixture(request: typing.Any) -> pathlib.Path:
    return next(p for p in pathlib.Path(request.fspath).parents if p.name == "test")


@pytest.fixture(scope="function")
def _config_fixture(_root_dir_fixture: pathlib.Path) -> dict[str, typing.Any]:
    with (_root_dir_fixture / "test-config.json").open("r") as fh:
        return typing.cast(dict[str, typing.Hashable], json.load(fh))


@pytest.fixture(scope="function")
def pg_connection_str_fixture(_config_fixture: dict[str, typing.Any]) -> str:
    return typing.cast(str, _config_fixture["ds"]["pg"]["connection-string"])


@pytest.fixture(scope="function")
def pg_connection_fixture(
    _root_dir_fixture: pathlib.Path, pg_connection_str_fixture: str
) -> typing.Generator[psycopg.Connection, None, None]:
    with psycopg.connect(pg_connection_str_fixture) as con:
        with con.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS poa CASCADE;")
            sql_path = _root_dir_fixture.parent / "setup.sql"
            with sql_path.open("r") as fh:
                qry = "\n".join(fh.readlines())
                for fragment in qry.split(";"):
                    cur.execute(typing.cast(sql.LiteralString, fragment))
        yield con


@pytest.fixture(scope="function")
def pg_cursor_fixture(
    pg_connection_fixture: psycopg.Connection,
) -> typing.Generator[dict[str, typing.Any], None, None]:
    with pg_connection_fixture.cursor(row_factory=dict_row) as cur:
        yield cur


@pytest.fixture(scope="function")
def hh_connection_str_fixture(_config_fixture: dict[str, typing.Any]) -> str:
    return typing.cast(str, _config_fixture["ds"]["hh"]["connection-string"])


@pytest.fixture(scope="function")
def hh_schema_name(_config_fixture: dict[str, typing.Any]) -> str:
    return typing.cast(str, _config_fixture["hh-schema-name"])


@pytest.fixture(scope="function")
def hh_connection_fixture(
    hh_connection_str_fixture: str,
) -> typing.Generator[pyodbc.Connection, None, None]:
    with pyodbc.connect(hh_connection_str_fixture, autocommit=True) as con:
        yield con

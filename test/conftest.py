import json
import pathlib
import typing

import psycopg2
import pytest
from psycopg2._psycopg import connection
from psycopg2.extras import RealDictCursor


@pytest.fixture(scope="function")
def _root_dir_fixture(request) -> pathlib.Path:
    return next(p for p in pathlib.Path(request.fspath).parents if p.name == "test")


@pytest.fixture(scope="function")
def _config_fixture(_root_dir_fixture: pathlib.Path) -> dict[str, typing.Hashable]:
    with (_root_dir_fixture / "test-config.json").open("r") as fh:
        return json.load(fh)


@pytest.fixture(scope="function")
def connection_str_fixture(_config_fixture: dict[str, typing.Hashable]) -> str:
    return typing.cast(str, _config_fixture["test-db-connection-str"])


@pytest.fixture(scope="function")
def pg_connection_fixture(_root_dir_fixture: pathlib.Path, connection_str_fixture: str) -> typing.Generator[connection, None, None]:
    with psycopg2.connect(connection_str_fixture) as con:
        with con.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS poa CASCADE;")
            sql_path = _root_dir_fixture.parent / "setup.sql"
            with sql_path.open("r") as fh:
                sql = "\n".join(fh.readlines())
            cur.execute(sql)
        con.commit()
        yield con


@pytest.fixture(scope="function")
def pg_cursor_fixture(pg_connection_fixture: connection) -> typing.Generator[RealDictCursor, None, None]:
    with pg_connection_fixture.cursor(cursor_factory=RealDictCursor) as cur:
        yield cur

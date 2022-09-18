import contextlib
import typing

import pytest
from psycopg2._psycopg import cursor
from psycopg2.extras import RealDictCursor

from src import data
from src.adapter.log.pg import PgLog


class CursorProvider(data.CursorProvider):
    def __init__(self, *, cur: cursor):
        self._cur = cur

    @contextlib.contextmanager
    def open(self) -> typing.Generator[typing.Any, None, None]:
        yield self._cur


@pytest.fixture(scope="function")
def log_fixture(pg_cursor_fixture: RealDictCursor) -> data.Log:
    cursor_provider = CursorProvider(cur=pg_cursor_fixture)
    return PgLog(cursor_provider=cursor_provider)


def test_error(pg_cursor_fixture: cursor, log_fixture: data.Log) -> None:
    log_fixture.error("Test")
    pg_cursor_fixture.execute("SELECT message FROM poa.error;")
    messages = [row["message"] for row in pg_cursor_fixture.fetchall()]  # type: ignore
    assert messages == ["Test"]


def test_sync_failed(pg_cursor_fixture: cursor, log_fixture: data.Log) -> None:
    pg_cursor_fixture.execute("""
        INSERT INTO poa.sync (sync_id, src_db_name, src_schema_name, src_table_name, incremental, ts) 
        OVERRIDING SYSTEM VALUE VALUES (1, 'src-db', 'src-schema', 'src-table', TRUE, now());
    """)
    log_fixture.sync_failed(sync_id=1, reason="Test")
    pg_cursor_fixture.execute("SELECT se.error_message FROM poa.sync_error AS se;")
    messages = [row["error_message"] for row in pg_cursor_fixture.fetchall()]  # type: ignore
    assert messages == ["Test"]


def test_sync_skipped(pg_cursor_fixture: cursor, log_fixture: data.Log) -> None:
    pg_cursor_fixture.execute("""
        INSERT INTO poa.sync (sync_id, src_db_name, src_schema_name, src_table_name, incremental, ts) 
        OVERRIDING SYSTEM VALUE VALUES (1, 'src-db', 'src-schema', 'src-table', TRUE, now());
    """)
    log_fixture.sync_skipped(sync_id=1, reason="Test")
    pg_cursor_fixture.execute("SELECT ss.reason FROM poa.sync_skip AS ss;")
    messages = [row["reason"] for row in pg_cursor_fixture.fetchall()]  # type: ignore
    assert messages == ["Test"]


def test_sync_started(pg_cursor_fixture: cursor, log_fixture: data.Log) -> None:
    log_fixture.sync_started(src_db_name="src-db", src_schema_name="src-schema", src_table_name="src-table", incremental=True)
    pg_cursor_fixture.execute("""
        SELECT src_db_name, src_schema_name, src_table_name, incremental
        FROM poa.sync AS s;
    """)
    sync_rows = [
        (row["src_db_name"], row["src_schema_name"], row["src_table_name"], row["incremental"])  # type: ignore
        for row in pg_cursor_fixture.fetchall()
    ]
    assert sync_rows == [("src-db", "src-schema", "src-table", True)]


def test_sync_succeeded(pg_cursor_fixture: cursor, log_fixture: data.Log) -> None:
    pg_cursor_fixture.execute("""
        INSERT INTO poa.sync (sync_id, src_db_name, src_schema_name, src_table_name, incremental, ts) 
        OVERRIDING SYSTEM VALUE VALUES (1, 'src-db', 'src-schema', 'src-table', TRUE, now());
    """)
    log_fixture.sync_succeeded(sync_id=1, execution_millis=100)
    pg_cursor_fixture.execute("SELECT ss.execution_millis FROM poa.sync_success AS ss;")
    execution_millis = [row["execution_millis"] for row in pg_cursor_fixture.fetchall()]  # type: ignore
    assert execution_millis == [100]

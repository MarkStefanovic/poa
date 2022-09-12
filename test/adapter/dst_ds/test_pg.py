import datetime

import pytest
from frozendict import frozendict
from psycopg2._psycopg import connection, cursor
from psycopg2.extras import RealDictCursor

from src import data
from src.adapter.dst_ds.pg import PgDstDs


@pytest.fixture(scope="session")
def customer_table_fixture() -> data.Table:
    return data.Table(
        db_name="src",
        schema_name="sales",
        table_name="customer",
        pk=("customer_id",),
        columns=frozenset({
            data.Column(name="customer_id", data_type=data.DataType.Int, nullable=False, length=None, precision=None, scale=None),
            data.Column(name="first_name", data_type=data.DataType.Text, nullable=False, length=None, precision=None, scale=None),
            data.Column(name="middle_name", data_type=data.DataType.Text, nullable=True, length=None, precision=None, scale=None),
            data.Column(name="last_name", data_type=data.DataType.Text, nullable=False, length=None, precision=None, scale=None),
            data.Column(name="birth_date", data_type=data.DataType.Date, nullable=False, length=None, precision=None, scale=None),
            data.Column(name="purchases", data_type=data.DataType.Decimal, nullable=False, length=None, precision=18, scale=2),
            data.Column(name="date_added", data_type=data.DataType.TimestampTZ, nullable=False, length=None, precision=None, scale=None),
            data.Column(name="date_deleted", data_type=data.DataType.TimestampTZ, nullable=True, length=None, precision=None, scale=None),
        })
    )


def test_create(pg_cursor_fixture: RealDictCursor, customer_table_fixture: data.Table):
    pg_cursor_fixture.execute("""
        INSERT INTO poa.sync_table_spec (sync_table_spec_id, src_db_name, src_schema_name, src_table_name, compare_cols, increasing_cols, skip_if_row_counts_match)
        OVERRIDING SYSTEM VALUE VALUES  (1, 'src', 'sales', 'customer', '{first_name,last_name}', '{date_added,date_deleted}', true);
    """)
    assert not _customer_table_exists(cur=pg_cursor_fixture), "The table should not exist before create."
    ds = PgDstDs(cur=pg_cursor_fixture, db_name="dst", table=customer_table_fixture)
    ds.create()
    assert _customer_table_exists(cur=pg_cursor_fixture), "The table was not created after create()."


def test_delete_rows(pg_cursor_fixture: RealDictCursor, customer_table_fixture: data.Table):
    _create_customer_table(cur=pg_cursor_fixture)
    pg_cursor_fixture.execute("""
        INSERT INTO poa.src_sales_customer 
            (birth_date, customer_id, date_added, date_deleted, first_name, last_name, middle_name, purchases, poa_hd, poa_op, poa_ts)
        VALUES  
            ('2022-09-01', 1, '2022-09-10 +0', null, 'Steve', 'Smith', 'S', 1234.56, '', 'a', '2022-09-10 +0')
        ,   ('2022-09-02', 2, '2022-09-11 +0', '2022-09-10 +0', 'Mandie', 'Mandlebrot', 'M', 2345.56, '', 'u', '2022-09-10 +0')
        ,   ('2022-09-02', 3, '2022-08-09 +0', null, 'Bill', 'Button', 'B', 345.67, '', 'a', '2022-09-10 +0')
    """)
    ds = PgDstDs(cur=pg_cursor_fixture, db_name="dst", table=customer_table_fixture)
    ds.delete_rows(keys={frozendict({"customer_id": 2})})
    pg_cursor_fixture.execute("SELECT poa_op FROM poa.src_sales_customer WHERE customer_id = 2")
    assert pg_cursor_fixture.fetchone()["poa_op"] == "d", "customer_id = 2 should have been deleted, but it wasn't."  # noqa


def test_fetch_rows_when_after_is_none(pg_cursor_fixture: RealDictCursor, customer_table_fixture: data.Table):
    _create_customer_table(cur=pg_cursor_fixture)
    pg_cursor_fixture.execute("""
        INSERT INTO poa.src_sales_customer 
            (birth_date, customer_id, date_added, date_deleted, first_name, last_name, middle_name, purchases, poa_hd, poa_op, poa_ts)
        VALUES  
            ('2022-09-01', 1, '2022-09-10 +0', null, 'Steve', 'Smith', 'S', 1234.56, '', 'a', '2022-09-10 +0')
        ,   ('2022-09-02', 2, '2022-09-11 +0', '2022-09-10 +0', 'Mandie', 'Mandlebrot', 'M', 2345.56, '', 'u', '2022-09-10 +0')
        ,   ('2022-09-02', 3, '2022-08-09 +0', null, 'Bill', 'Button', 'B', 345.67, '', 'a', '2022-09-10 +0')
    """)
    ds = PgDstDs(cur=pg_cursor_fixture, db_name="dst", table=customer_table_fixture)
    rows = ds.fetch_rows(col_names={"first_name", "last_name"}, after=None)
    assert rows == [
        {"first_name": "Steve", "last_name": "Smith"},
        {"first_name": "Mandie", "last_name": "Mandlebrot"},
        {"first_name": "Bill", "last_name": "Button"},
    ]


def test_fetch_rows_when_after_is_not_none(pg_cursor_fixture: RealDictCursor, customer_table_fixture: data.Table):
    _create_customer_table(cur=pg_cursor_fixture)
    pg_cursor_fixture.execute("""
        INSERT INTO poa.src_sales_customer 
            (birth_date, customer_id, date_added, date_deleted, first_name, last_name, middle_name, purchases, poa_hd, poa_op, poa_ts)
        VALUES  
            ('2022-09-01', 1, '2022-09-10 +0', null, 'Steve', 'Smith', 'S', 1234.56, '', 'a', '2022-09-10 +0')
        ,   ('2022-09-02', 2, '2022-09-11 +0', '2022-09-10 +0', 'Mandie', 'Mandlebrot', 'M', 2345.56, '', 'u', '2022-09-10 +0')
        ,   ('2022-09-02', 3, '2022-09-12 +0', null, 'Bill', 'Button', 'B', 345.67, '', 'a', '2022-09-10 +0')
    """)
    ds = PgDstDs(cur=pg_cursor_fixture, db_name="dst", table=customer_table_fixture)
    rows = ds.fetch_rows(
        col_names={"first_name", "last_name"},
        after={
            "date_added": datetime.datetime(2022, 9, 10, 1, 2, 3, tzinfo=datetime.timezone.utc),
            "date_updated": None,
        },
    )
    assert {(row["first_name"], row["last_name"]) for row in rows} == {("Mandie", "Mandlebrot"), ("Bill", "Button")}


def test_get_increasing_col_values(pg_cursor_fixture: RealDictCursor, customer_table_fixture: data.Table):
    _create_customer_table(cur=pg_cursor_fixture)
    pg_cursor_fixture.execute("""
        INSERT INTO poa.sync_table_spec (sync_table_spec_id, src_db_name, src_schema_name, src_table_name, compare_cols, increasing_cols, skip_if_row_counts_match)
        OVERRIDING SYSTEM VALUE VALUES  (1, 'src', 'sales', 'customer', '{first_name,last_name}', '{date_added,date_deleted}', true);
    """)
    pg_cursor_fixture.execute("""
        INSERT INTO poa.src_sales_customer 
            (birth_date, customer_id, date_added, date_deleted, first_name, last_name, middle_name, purchases, poa_hd, poa_op, poa_ts)
        VALUES  
            ('2022-09-01', 1, '2022-09-10 +0', null, 'Steve', 'Smith', 'S', 1234.56, '', 'a', '2022-09-10 +0')
        ,   ('2022-09-02', 2, '2022-09-11 +0', '2022-09-10 +0', 'Mandie', 'Mandlebrot', 'M', 2345.56, '', 'u', '2022-09-10 +0')
        ,   ('2022-09-02', 3, '2022-08-09 +0', null, 'Bill', 'Button', 'B', 345.67, '', 'a', '2022-09-10 +0')
    """)
    ds = PgDstDs(cur=pg_cursor_fixture, db_name="dst", table=customer_table_fixture)
    rows = ds.get_increasing_col_values()
    assert rows == {
        "date_added": datetime.datetime(2022, 9, 11, tzinfo=datetime.timezone.utc),
        "date_deleted": datetime.datetime(2022, 9, 10, tzinfo=datetime.timezone.utc),
    }


def test_get_row_count(pg_cursor_fixture: RealDictCursor, customer_table_fixture: data.Table):
    _create_customer_table(cur=pg_cursor_fixture)
    pg_cursor_fixture.execute("""
        INSERT INTO poa.src_sales_customer 
            (birth_date, customer_id, date_added, date_deleted, first_name, last_name, middle_name, purchases, poa_hd, poa_op, poa_ts)
        VALUES  
            ('2022-09-01', 1, '2022-09-10 +0', null, 'Steve', 'Smith', 'S', 1234.56, '', 'a', '2022-09-10 +0')
        ,   ('2022-09-02', 2, '2022-09-11 +0', '2022-09-10 +0', 'Mandie', 'Mandlebrot', 'M', 2345.56, '', 'u', '2022-09-10 +0')
        ,   ('2022-09-02', 3, '2022-08-09 +0', null, 'Bill', 'Button', 'B', 345.67, '', 'a', '2022-09-10 +0')
    """)
    ds = PgDstDs(cur=pg_cursor_fixture, db_name="dst", table=customer_table_fixture)
    rows = ds.get_row_count()
    assert rows == 3


def test_get_sync_table_spec(pg_cursor_fixture: RealDictCursor, customer_table_fixture: data.Table):
    pg_cursor_fixture.execute("SELECT COUNT(*) AS ct FROM poa.sync_table_spec")
    assert pg_cursor_fixture.fetchone()["ct"] == 0, "poa.sync_table_spec should be empty before test_get_sync_table_spec is run."  # noqa

    pg_cursor_fixture.execute("""
        CALL poa.add_sync_table_spec(
            p_src_db_name := 'src'
        ,   p_src_schema_name := 'sales'
        ,   p_src_table_name := 'customer'
        ,   p_compare_cols := ARRAY['description']::TEXT[]
        ,   p_increasing_cols := ARRAY['date_added', 'date_deleted']::TEXT[]
        ,   p_skip_if_row_counts_match := TRUE
        );
    """)

    ds = PgDstDs(cur=pg_cursor_fixture, db_name="src", table=customer_table_fixture)
    sync_table_spec = ds.get_sync_table_spec()
    assert sync_table_spec.table_name == "customer"
    assert sync_table_spec.increasing_cols == {"date_added", "date_deleted"}


def test_table_exists(pg_cursor_fixture: RealDictCursor, customer_table_fixture: data.Table):
    _create_customer_table(cur=pg_cursor_fixture)
    ds = PgDstDs(cur=pg_cursor_fixture, db_name="src", table=customer_table_fixture)
    assert ds.table_exists()


def test_truncate(pg_cursor_fixture: RealDictCursor, customer_table_fixture: data.Table):
    _create_customer_table(cur=pg_cursor_fixture)
    pg_cursor_fixture.execute("""
        INSERT INTO poa.src_sales_customer 
            (birth_date, customer_id, date_added, date_deleted, first_name, last_name, middle_name, purchases, poa_hd, poa_op, poa_ts)
        VALUES  
            ('2022-09-01', 1, '2022-09-10 +0', null, 'Steve', 'Smith', 'S', 1234.56, '', 'a', '2022-09-10 +0')
        ,   ('2022-09-02', 2, '2022-09-11 +0', '2022-09-10 +0', 'Mandie', 'Mandlebrot', 'M', 2345.56, '', 'u', '2022-09-10 +0')
        ,   ('2022-09-02', 3, '2022-08-09 +0', null, 'Bill', 'Button', 'B', 345.67, '', 'a', '2022-09-10 +0')
    """)
    pg_cursor_fixture.execute("SELECT COUNT(*) AS ct FROM poa.src_sales_customer")
    initial_rows = pg_cursor_fixture.fetchone()["ct"]  # noqa
    assert initial_rows == 3, f"initial rows should be 3, but there were {initial_rows} rows."
    ds = PgDstDs(cur=pg_cursor_fixture, db_name="src", table=customer_table_fixture)
    ds.truncate()
    pg_cursor_fixture.execute("SELECT COUNT(*) AS ct FROM poa.src_sales_customer")
    rows_after_truncate = pg_cursor_fixture.fetchone()["ct"]  # noqa
    assert rows_after_truncate == 0, f"rows after truncate should be 0, but there were {rows_after_truncate} rows."


def test_upsert_rows(pg_cursor_fixture: RealDictCursor, customer_table_fixture: data.Table):
    _create_customer_table(cur=pg_cursor_fixture)
    pg_cursor_fixture.execute("""
        INSERT INTO poa.src_sales_customer 
            (birth_date, customer_id, date_added, date_deleted, first_name, last_name, middle_name, purchases, poa_hd, poa_op, poa_ts)
        VALUES  
            ('2022-09-01', 1, '2022-09-10 +0', null, 'Steve', 'Smith', 'S', 1234.56, '', 'a', '2022-09-10 +0')
        ,   ('2022-09-02', 2, '2022-09-11 +0', '2022-09-10 +0', 'Mandie', 'Mandlebrot', 'M', 2345.56, '', 'u', '2022-09-10 +0')
        ,   ('2022-09-02', 3, '2022-08-09 +0', null, 'Bill', 'Button', 'B', 345.67, '', 'a', '2022-09-10 +0')
    """)
    pg_cursor_fixture.execute("SELECT COUNT(*) AS ct FROM poa.src_sales_customer")
    initial_rows = pg_cursor_fixture.fetchone()["ct"]  # noqa
    assert initial_rows == 3, f"initial rows should be 3, but there were {initial_rows} rows."
    ds = PgDstDs(cur=pg_cursor_fixture, db_name="src", table=customer_table_fixture)
    rows = [
        {"birth_date": datetime.date(1912, 3, 4), "customer_id": 1, "date_added": datetime.datetime(2010, 1, 2), "date_deleted": None, "first_name": "Steve", "last_name": "Smith", "middle_name": "S", "purchases": 2345.67},
        {"birth_date": datetime.date(2001, 2, 3), "customer_id": 4, "date_added": datetime.datetime(2011, 2, 3), "date_deleted": datetime.date(2011, 3, 4), "first_name": "Tim", "last_name": "Timely", "middle_name": "T", "purchases": 13.45},
    ]
    ds.upsert_rows(rows)
    pg_cursor_fixture.execute("SELECT COUNT(*) AS ct FROM poa.src_sales_customer")
    rows_after_upsert = pg_cursor_fixture.fetchone()["ct"]  # noqa
    assert rows_after_upsert == 4, f"rows after truncate should be 4, but there were {rows_after_upsert} rows."


def _create_customer_table(*, cur: cursor) -> None:
    cur.execute("""
        CREATE TABLE poa.src_sales_customer (
            birth_date   DATE NOT NULL
        ,   customer_id  INTEGER PRIMARY KEY
        ,   date_added   TIMESTAMP(3) WITH TIME ZONE NOT NULL
        ,   date_deleted TIMESTAMP(3) WITH TIME ZONE NULL
        ,   first_name   TEXT NOT NULL
        ,   last_name    TEXT NOT NULL
        ,   middle_name  TEXT
        ,   purchases    NUMERIC(18, 2) NOT NULL
        ,   poa_hd       CHAR(32) NOT NULL
        ,   poa_op       CHAR NOT NULL
        ,   poa_ts       TIMESTAMP(3) WITH TIME ZONE DEFAULT now() NOT NULL
        );
    """)


def _customer_table_exists(*, cur: cursor) -> bool:
    cur.execute("""
        SELECT COUNT(*) AS ct 
        FROM information_schema.tables 
        WHERE table_schema = 'poa' AND table_name = 'src_sales_customer'
    """)
    result = cur.fetchone()
    return result["ct"] > 0  # noqa

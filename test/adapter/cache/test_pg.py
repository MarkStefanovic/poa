import pytest
from psycopg2.extras import RealDictCursor

from src import data
from src.adapter.cache.pg import PgCache


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


def test_round_trip(pg_cursor_fixture: RealDictCursor, customer_table_fixture: data.Table):
    cache = PgCache(cur=pg_cursor_fixture)
    cache.add_table_def(table=customer_table_fixture)
    table_def = cache.get_table_def(db_name="src", schema_name="sales", table_name="customer")
    assert customer_table_fixture == table_def

from psycopg2._psycopg import connection, cursor

from src import data, service
from src.adapter.dst_ds.pg import PgDstDs
from src.adapter.src_ds.pg import PgSrcDs


def test_incremental_sync_using_increasing_method(pg_connection_fixture: connection, customer_table_fixture: data.Table):
    with pg_connection_fixture.cursor() as src_cur, pg_connection_fixture.cursor() as dst_cur:
        src_db_name = "pg_src"
        src_schema_name = "sales"
        src_table_name = "customer"

        src_table = _create_customer_table(
            cur=src_cur,
            db_name=src_db_name,
            schema_name=src_schema_name,
            table_name=src_table_name,
        )

        assert _table_exists(cur=src_cur, schema_name=src_schema_name, table_name=src_table_name)

        src_cur.execute("""
            INSERT INTO poa.src_sales_customer 
                (birth_date, customer_id, date_added, date_deleted, first_name, last_name, middle_name, purchases)
            VALUES  
                ('2022-09-01', 1, '2022-09-10 +0', null, 'Steve', 'Smith', 'S', 1234.56)
            ,   ('2022-09-02', 2, '2022-09-11 +0', '2022-09-10 +0', 'Mandie', 'Mandlebrot', 'M', 2345.56)
            ,   ('2022-09-02', 3, '2022-08-09 +0', null, 'Bill', 'Button', 'B', 345.67)
            ;
        """)

        src_ds = PgSrcDs(
            cur=src_cur,
            db_name=src_db_name,
            schema_name=src_schema_name,
            table_name=src_table_name,
        )

        dst_ds = PgDstDs(
            cur=dst_cur,
            dst_db_name="dw",
            dst_schema_name="sales",
            dst_table_name="customer",
            src_table=src_table,
        )

        service.sync(
            src_ds=src_ds,
            dst_ds=dst_ds,
            incremental=True,
            compare_cols=None,
            increasing_cols={"date_added", "date_updated"},
            skip_if_row_counts_match=True,
            recreate=False,
        )


def _create_customer_table(*, cur: cursor, db_name: str, schema_name: str, table_name: str) -> data.Table:
    cur.execute(f"""
        CREATE TABLE {schema_name}.{table_name} (
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

    return data.Table(
        db_name=db_name,
        schema_name=schema_name,
        table_name=table_name,
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


def _table_exists(*, cur: cursor, schema_name: str, table_name: str) -> bool:
    cur.execute(
        """
        SELECT 
            COUNT(*) AS ct 
        FROM information_schema.tables 
        WHERE 
            table_schema = %(schema_name)s 
            AND table_name = %(table_name)s
        ;
        """,
        {"schema_name": schema_name, "table_name": table_name}
    )
    result = cur.fetchone()
    return result["ct"] > 0  # noqa

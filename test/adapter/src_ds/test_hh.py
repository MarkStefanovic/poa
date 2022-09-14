from src import adapter, data


def test_fetch_rows(hh_connection_fixture, hh_schema_name):
    with hh_connection_fixture as con:
        with con.cursor() as cur:
            ds = adapter.src_ds.create(
                api=data.API.HH,
                cur=cur,
                db_name="src",
                schema_name=hh_schema_name,
                table_name="activity",
                pk_cols=("activity_id",),
            )
            rows = ds.fetch_rows(col_names={"activity_id"}, after=None)
            assert len(rows) > 0, "No rows were returned"


def test_get_row_count(hh_connection_fixture, hh_schema_name):
    with hh_connection_fixture as con:
        with con.cursor() as cur:
            ds = adapter.src_ds.create(
                api=data.API.HH,
                cur=cur,
                db_name="src",
                schema_name=hh_schema_name,
                table_name="activity",
                pk_cols=("activity_id",),
            )
            row_ct = ds.get_row_count()
            assert row_ct > 0


def test_get_table(hh_connection_fixture, hh_schema_name):
    with hh_connection_fixture as con:
        with con.cursor() as cur:
            ds = adapter.src_ds.create(
                api=data.API.HH,
                cur=cur,
                db_name="src",
                schema_name=hh_schema_name,
                table_name="activity",
                pk_cols=("activity_id",),
            )
            table = ds.get_table()
            expected_activity_id_col_def = data.Column(
                name="activity_id",
                data_type=data.DataType.BigInt,
                nullable=False,
                length=None,
                precision=None,
                scale=None,
            )
            assert next(col for col in table.columns if col.name == "activity_id") == expected_activity_id_col_def


def test_table_exists(hh_connection_fixture, hh_schema_name):
    with hh_connection_fixture as con:
        with con.cursor() as cur:
            ds = adapter.src_ds.create(
                api=data.API.HH,
                cur=cur,
                db_name="src",
                schema_name=hh_schema_name,
                table_name="activity",
                pk_cols=("activity_id",),
            )
            assert ds.table_exists() is True

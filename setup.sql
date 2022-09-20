/*
DROP SCHEMA poa CASCADE;
*/
CREATE SCHEMA poa;

CREATE TABLE poa.check_result (
    check_result_id SERIAL PRIMARY KEY
,   src_db_name TEXT NOT NULL
,   src_schema_name TEXT NULL
,   src_table_name TEXT NOT NULL
,   dst_db_name TEXT NOT NULL
,   dst_schema_name TEXT NULL
,   dst_table_name TEXT NOT NULL
,   src_rows INT NOT NULL
,   dst_rows INT NOT NULL
,   extra_keys JSONB[] NULL
,   missing_keys JSONB[] NULL
,   execution_millis INT NOT NULL CHECK (execution_millis >= 0)
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
);

CREATE TABLE poa.cleanup (
    cleanup_id SERIAL PRIMARY KEY
,   days_kept INT NOT NULL CHECK (days_kept > 0)
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);

CREATE TABLE poa.error (
    error_id SERIAL PRIMARY KEY
,   message TEXT NOT NULL
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
);

CREATE TABLE poa.sync (
    sync_id SERIAL PRIMARY KEY
,   src_db_name TEXT NOT NULL
,   src_schema_name TEXT NULL
,   src_table_name TEXT NOT NULL
,   incremental BOOL NOT NULL
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
);

CREATE TABLE poa.sync_error (
    sync_id INT PRIMARY KEY REFERENCES poa.sync (sync_id)
,   error_message TEXT NOT NULL
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
);

CREATE TABLE poa.sync_skip (
    sync_id INT PRIMARY KEY REFERENCES poa.sync (sync_id)
,   reason TEXT NOT NULL
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
);

CREATE TABLE poa.sync_success (
    sync_id INT PRIMARY KEY REFERENCES poa.sync (sync_id)
,   execution_millis INT NOT NULL
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
);

CREATE OR REPLACE PROCEDURE poa.add_check_result (
    p_src_db_name TEXT
,   p_src_schema_name TEXT
,   p_src_table_name TEXT
,   p_dst_db_name TEXT
,   p_dst_schema_name TEXT
,   p_dst_table_name TEXT
,   p_src_rows INT
,   p_dst_rows INT
,   p_extra_keys JSONB[]
,   p_missing_keys JSONB[]
,   p_execution_millis INT
)
LANGUAGE sql
AS $$
    INSERT INTO poa.check_result (
        src_db_name
    ,   src_schema_name
    ,   src_table_name
    ,   dst_db_name
    ,   dst_schema_name
    ,   dst_table_name
    ,   src_rows
    ,   dst_rows
    ,   extra_keys
    ,   missing_keys
    ,   execution_millis
    ) VALUES (
        p_src_db_name
    ,   p_src_schema_name
    ,   p_src_table_name
    ,   p_dst_db_name
    ,   p_dst_schema_name
    ,   p_dst_table_name
    ,   p_src_rows
    ,   p_dst_rows
    ,   p_extra_keys
    ,   p_missing_keys
    ,   p_execution_millis
    );
$$;

CREATE OR REPLACE PROCEDURE poa.log_error(
    p_error_message TEXT
)
LANGUAGE sql
AS $$
    INSERT INTO poa.error (message)
    VALUES (p_error_message);
$$;

CREATE OR REPLACE PROCEDURE poa.sync_failed (
    p_sync_id INT
,   p_error_message TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    ASSERT p_sync_id IS NOT NULL, 'p_sync_id is required';
    ASSERT length(trim(p_error_message)) > 0, 'p_error_message is required';

    INSERT INTO poa.sync_error (sync_id, error_message)
    VALUES (p_sync_id, trim(p_error_message));
END;
$$;

CREATE OR REPLACE PROCEDURE poa.sync_skipped (
    p_sync_id INT
,   p_skip_reason TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    ASSERT p_sync_id IS NOT NULL, 'p_sync_id is required';
    ASSERT length(trim(p_skip_reason)) > 0, 'p_skip_reason is required';

    INSERT INTO poa.sync_skip (sync_id, reason)
    VALUES (p_sync_id, trim(p_skip_reason));
END;
$$;

CREATE OR REPLACE FUNCTION poa.sync_started (
    p_src_db_name TEXT
,   p_src_schema_name TEXT
,   p_src_table_name TEXT
,   p_incremental BOOL
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_result INT;
BEGIN
    ASSERT length(p_src_db_name) > 0, 'p_src_db_name is required.';
    ASSERT p_src_schema_name IS NULL OR length(p_src_schema_name) > 0, 'If p_src_schema_name is provided, then it cannot be blank.';
    ASSERT length(p_src_table_name) > 0, 'p_table_name is required.';
    ASSERT p_incremental IS NOT NULL, 'p_incremental is required.';

    WITH ins AS (
        INSERT INTO poa.sync (src_db_name, src_schema_name, src_table_name, incremental)
        VALUES (p_src_db_name, p_src_schema_name, p_src_table_name, p_incremental)
        RETURNING sync_id
    )
    SELECT i.sync_id
    INTO v_result
    FROM ins AS i;

    RETURN v_result;
END;
$$;

CREATE OR REPLACE PROCEDURE poa.sync_succeeded (
    p_sync_id INT
,   p_execution_millis INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    ASSERT p_sync_id IS NOT NULL, 'p_sync_id is required';
    ASSERT p_execution_millis >= 0, 'p_execution_millis must be >= 0, but got %.', p_execution_millis;

    INSERT INTO poa.sync_success (sync_id, execution_millis)
    VALUES (p_sync_id, p_execution_millis);
END;
$$;

CREATE OR REPLACE PROCEDURE poa.delete_old_logs (
    p_days_to_keep INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_cutoff TIMESTAMPTZ := now() - (p_days_to_keep || ' DAYS')::INTERVAL;

BEGIN
    DELETE FROM poa.check_result AS cr
    WHERE cr.ts < v_cutoff;

    DELETE FROM poa.error AS e
    WHERE e.ts < v_cutoff;

    DELETE FROM poa.cleanup AS c
    WHERE c.ts < v_cutoff;

    DROP TABLE IF EXISTS tmp_sync_ids_to_delete;
    CREATE TEMP TABLE tmp_sync_ids_to_delete (sync_id INT PRIMARY KEY);

    INSERT INTO tmp_sync_ids_to_delete (sync_id)
    SELECT s.sync_id
    FROM poa.sync AS s
    WHERE s.ts < v_cutoff;

    DELETE FROM poa.sync_error AS s
    WHERE EXISTS (
        SELECT 1
        FROM tmp_sync_ids_to_delete AS os
        WHERE s.sync_id = os.sync_id
    );

    DELETE FROM poa.sync_skip AS s
    WHERE EXISTS (
        SELECT 1
        FROM tmp_sync_ids_to_delete AS os
        WHERE s.sync_id = os.sync_id
    );

    DELETE FROM poa.sync_success AS s
    WHERE EXISTS (
        SELECT 1
        FROM tmp_sync_ids_to_delete AS os
        WHERE s.sync_id = os.sync_id
    );

    DELETE FROM poa.sync AS s
    WHERE EXISTS (
        SELECT 1
        FROM tmp_sync_ids_to_delete AS os
        WHERE s.sync_id = os.sync_id
    );

    INSERT INTO poa.cleanup (days_kept)
    VALUES (p_days_to_keep);
END;
$$;

CREATE TABLE poa.table_def (
    table_def_id SERIAL PRIMARY KEY
,   db_name TEXT NOT NULL CHECK (length(db_name) > 0)
,   schema_name TEXT NULL CHECK (schema_name IS NULL OR length(schema_name) > 0)
,   table_name TEXT NOT NULL CHECK (length(table_name) > 0)
,   pk_cols TEXT[] NOT NULL CHECK (cardinality(pk_cols) > 0)
,   op CHAR(1) NOT NULL CHECK (op IN ('a', 'd', 'u'))
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
,   UNIQUE (db_name, schema_name, table_name)
);

CREATE TYPE poa.col_def_data_type_option AS ENUM (
    'big_float'
,   'big_int'
,   'bool'
,   'date'
,   'decimal'
,   'float'
,   'int'
,   'text'
,   'timestamp'
,   'timestamptz'
,   'uuid'
);

CREATE TABLE poa.col_def (
    col_def_id SERIAL PRIMARY KEY
,   table_def_id INT NOT NULL REFERENCES poa.table_def (table_def_id)
,   col_name TEXT NOT NULL CHECK (length(col_name) > 0)
,   col_data_type poa.col_def_data_type_option NOT NULL
,   col_length INT NULL CHECK (col_length IS NULL OR col_length > 0)
,   col_precision INT NULL CHECK (col_precision IS NULL OR col_precision > 0)
,   col_scale INT NULL CHECK (col_scale IS NULL OR col_scale >= 0)
,   col_nullable BOOL NOT NULL
,   op CHAR(1) NOT NULL CHECK (op IN ('a', 'd', 'u'))
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
,   UNIQUE (table_def_id, col_name)
);

CREATE OR REPLACE FUNCTION poa.add_col_def(
    p_table_def_id INT
,   p_col_name TEXT
,   p_col_data_type TEXT
,   p_col_length INT
,   p_col_precision INT
,   p_col_scale INT
,   p_col_nullable BOOL
)
RETURNS INT
LANGUAGE sql
AS $$
    INSERT INTO poa.col_def
        (table_def_id, col_name, col_data_type, col_length, col_precision, col_scale, col_nullable, op)
    VALUES
        (p_table_def_id, p_col_name, p_col_data_type::poa.col_def_data_type_option, p_col_length, p_col_precision, p_col_scale, p_col_nullable, 'a')
    ON CONFLICT (table_def_id, col_name)
    DO UPDATE SET
        col_data_type = EXCLUDED.col_data_type
    ,   col_length = EXCLUDED.col_length
    ,   col_precision = EXCLUDED.col_precision
    ,   col_scale = EXCLUDED.col_scale
    ,   col_nullable = EXCLUDED.col_nullable
    ,   op = 'u'
    ,   ts = now()
    WHERE
        (
            poa.col_def.col_data_type
        ,   poa.col_def.col_length
        ,   poa.col_def.col_precision
        ,   poa.col_def.col_scale
        ,   poa.col_def.col_nullable
        )
        IS DISTINCT FROM
        (
            EXCLUDED.col_data_type
        ,   EXCLUDED.col_length
        ,   EXCLUDED.col_precision
        ,   EXCLUDED.col_scale
        ,   EXCLUDED.col_nullable
        )
    RETURNING col_def_id
$$;

CREATE OR REPLACE FUNCTION poa.add_table_def(
    p_db_name TEXT
,   p_schema_name TEXT
,   p_table_name TEXT
,   p_pk_cols TEXT[]
)
RETURNS INT
LANGUAGE sql
AS $$
    INSERT INTO poa.table_def
        (db_name, schema_name, table_name, pk_cols, op)
    VALUES
        (p_db_name, p_schema_name, p_table_name, p_pk_cols, 'a')
    ON CONFLICT (db_name, schema_name, table_name)
    DO UPDATE SET
        pk_cols = p_pk_cols
    ,   op = 'u'
    ,   ts = now()
    WHERE
        poa.table_def.pk_cols IS DISTINCT FROM EXCLUDED.pk_cols
    RETURNING table_def_id
$$;

CREATE OR REPLACE FUNCTION poa.get_table_cols (
    p_db_name TEXT
,   p_schema_name TEXT
,   p_table_name TEXT
)
RETURNS TABLE (
    col_name TEXT
,   col_data_type TEXT
,   col_length INT
,   col_precision INT
,   col_scale INT
,   col_nullable BOOL
)
LANGUAGE sql
AS $$
    SELECT
        cd.col_name
    ,   cd.col_data_type::TEXT AS col_data_type
    ,   cd.col_length
    ,   cd.col_precision
    ,   cd.col_scale
    ,   cd.col_nullable
    FROM poa.col_def AS cd
    JOIN poa.table_def AS td
        ON cd.table_def_id = td.table_def_id
    WHERE
        td.db_name = p_db_name
        AND td.schema_name = p_schema_name
        AND td.table_name = p_table_name
    ORDER BY
        cd.col_name
    ;
$$;

CREATE OR REPLACE FUNCTION poa.get_pk(
    p_db_name TEXT
,   p_schema_name TEXT
,   p_table_name TEXT
)
RETURNS TEXT[]
LANGUAGE sql
AS $$
    SELECT
        td.pk_cols
    FROM poa.table_def AS td
    WHERE
        td.db_name = p_db_name
        AND td.schema_name IS NOT DISTINCT FROM p_schema_name
        AND td.table_name = p_table_name
    ;
$$;

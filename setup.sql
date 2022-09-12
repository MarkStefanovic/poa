/*
DROP SCHEMA poa CASCADE;
*/
CREATE SCHEMA poa;

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

CREATE TABLE poa.sync_table_spec (
    sync_table_spec_id SERIAL PRIMARY KEY
,   src_db_name TEXT NOT NULL
,   src_schema_name TEXT NULL
,   src_table_name TEXT NOT NULL
,   compare_cols TEXT[] NULL
,   increasing_cols TEXT[] NULL
,   skip_if_row_counts_match BOOL NOT NULL
,   UNIQUE (src_db_name, src_schema_name, src_table_name)
);

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
BEGIN
    DROP TABLE IF EXISTS tmp_sync_ids_to_delete;
    CREATE TEMP TABLE tmp_sync_ids_to_delete (sync_id INT PRIMARY KEY);

    INSERT INTO tmp_sync_ids_to_delete (sync_id)
    SELECT s.sync_id
    FROM poa.sync AS s
    WHERE s.ts < now() - (p_days_to_keep || ' DAYS')::INTERVAL;

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
END;
$$;

CREATE OR REPLACE PROCEDURE poa.add_sync_table_spec (
    p_src_db_name TEXT
,   p_src_schema_name TEXT
,   p_src_table_name TEXT
,   p_compare_cols TEXT[] = NULL
,   p_increasing_cols TEXT[] = NULL
,   p_skip_if_row_counts_match BOOL = FALSE
)
LANGUAGE plpgsql
AS $$
BEGIN
    ASSERT p_compare_cols IS NULL OR cardinality(p_compare_cols) > 0, 'If p_compare_cols is provided, then it must have at least 1 element.';
    ASSERT p_increasing_cols IS NULL OR cardinality(p_increasing_cols) > 0, 'If p_increasing_cols is provided, then it must have at least 1 element.';

    INSERT INTO poa.sync_table_spec
        (src_db_name, src_schema_name, src_table_name, compare_cols, increasing_cols, skip_if_row_counts_match)
    VALUES
        (p_src_db_name, p_src_schema_name, p_src_table_name, p_compare_cols, p_increasing_cols, p_skip_if_row_counts_match)
    ;
END;
$$;

CREATE OR REPLACE FUNCTION poa.get_sync_table_spec (
    p_src_db_name TEXT
,   p_src_schema_name TEXT
,   p_src_table_name TEXT
)
RETURNS TABLE (
    compare_cols TEXT[]
,   increasing_cols TEXT[]
,   skip_if_row_counts_match BOOL
)
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        sts.compare_cols
    ,   sts.increasing_cols
    ,   sts.skip_if_row_counts_match
    FROM poa.sync_table_spec AS sts
    WHERE
        sts.src_db_name = p_src_db_name
        AND sts.src_schema_name IS NOT DISTINCT FROM p_src_schema_name
        AND sts.src_table_name = p_src_table_name
    ;
END;$$;

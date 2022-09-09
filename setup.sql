/*
DROP SCHEMA poa;
*/
CREATE SCHEMA poa;

CREATE TABLE poa.sync (
    sync_id SERIAL PRIMARY KEY
,   src_api TEXT NOT NULL
,   dst_api TEXT NOT NULL
,   schema_name TEXT NULL
,   table_name TEXT NOT NULL
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

CREATE OR REPLACE FUNCTION poa.sync_started (
    p_src_api TEXT
,   p_dst_api TEXT
,   p_schema_name TEXT
,   p_table_name TEXT
,   p_incremental BOOL
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_result INT;
BEGIN
    ASSERT length(p_src_api) > 0, 'p_src_api is required';
    ASSERT length(p_dst_api) > 0, 'p_dst_api is required.';
    ASSERT p_schema_name IS NULL OR length(p_schema_name) > 0, 'If p_schema_name is provided, then it cannot be blank.';
    ASSERT length(p_table_name) > 0, 'p_table_name is required.';
    ASSERT p_incremental IS NOT NULL, 'p_incremental is required.';

    WITH ins AS (
        INSERT INTO poa.sync (src_api, dst_api, schema_name, table_name, incremental)
        VALUES (p_src_api, p_dst_api, p_schema_name, p_table_name, p_incremental)
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

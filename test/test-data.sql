
DO $$
DECLARE v_sync_id INT;
BEGIN
    v_sync_id = (
        SELECT * FROM poa.sync_started(
            p_src_api := 'psycopg2'
        ,   p_dst_api := 'pyodbc'
        ,   p_schema_name := 'dbo'
        ,   p_table_name := 'activity'
        ,   p_incremental := TRUE
        )
    );
    CALL poa.sync_failed(p_sync_id := v_sync_id, p_error_message := 'Test');
END;
$$;





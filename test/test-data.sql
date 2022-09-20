
DO $$
DECLARE v_sync_id INT;
BEGIN
    v_sync_id = (
        SELECT * FROM poa.sync_started(
            p_src_db_name := 'src'
        ,   p_src_schema_name := 'sales'
        ,   p_src_table_name := 'customer'
        ,   p_incremental := TRUE
        )
    );
    CALL poa.sync_failed(p_sync_id := v_sync_id, p_error_message := 'Test');
END;
$$;





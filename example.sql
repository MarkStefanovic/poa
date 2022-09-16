DO $$
DECLARE v_table_def_id INT;
BEGIN
    TRUNCATE poa.col_def, poa.table_def;

    v_table_def_id = (
        SELECT * FROM poa.add_table_def(
            p_db_name := 'src'
        ,   p_schema_name := 'sales'
        ,   p_table_name := 'customer'
        ,   p_pk_cols := ARRAY['customer_id']::TEXT[]
        )
    );
    PERFORM poa.add_col_def(
        p_table_def_id := v_table_def_id
    ,   p_col_name := 'customer_id'
    ,   p_col_data_type := 'int'
    ,   p_col_length := NULL
    ,   p_col_precision := NULL
    ,   p_col_scale := NULL
    ,   p_col_nullable := FALSE
    );
    PERFORM poa.add_col_def(
        p_table_def_id := v_table_def_id
    ,   p_col_name := 'first_name'
    ,   p_col_data_type := 'text'
    ,   p_col_length := NULL
    ,   p_col_precision := NULL
    ,   p_col_scale := NULL
    ,   p_col_nullable := FALSE
    );
    PERFORM poa.add_col_def(
        p_table_def_id := v_table_def_id
    ,   p_col_name := 'last_name'
    ,   p_col_data_type := 'text'
    ,   p_col_length := NULL
    ,   p_col_precision := NULL
    ,   p_col_scale := NULL
    ,   p_col_nullable := FALSE
    );
END;
$$;

SELECT col_name, col_data_type, col_length, col_precision, col_scale, col_nullable
FROM poa.get_table_cols(p_db_name := 'src', p_schema_name := 'sales', p_table_name := 'customer');

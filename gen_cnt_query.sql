-- This is a single SQL file that can be executed in Athena
-- Save this as: schema_table_counts.sql

-- Step 1: Create a view that shows current counts
CREATE OR REPLACE VIEW schema_table_counts AS
WITH table_list AS (
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'schema1'
    AND table_name IN (
        'table1', 'table2', 'table3'  -- Replace with your actual table names
    )
),
table_counts AS (
    SELECT 
        'schema1' as schema_name,
        table_name,
        current_date as as_of_date,
        CAST(
            CASE table_name
                WHEN 'table1' THEN (SELECT COUNT(*) FROM schema1.table1)
                WHEN 'table2' THEN (SELECT COUNT(*) FROM schema1.table2)
                WHEN 'table3' THEN (SELECT COUNT(*) FROM schema1.table3)
                -- Add more WHEN clauses for additional tables
            END AS BIGINT
        ) as number_records
    FROM table_list
)
SELECT * FROM table_counts;

-- Step 2: Query the view to get results
SELECT * FROM schema_table_counts ORDER BY table_name;


# Python script to generate Athena SQL
def generate_athena_sql(schema_name, table_list):
    sql = f"""
    CREATE OR REPLACE VIEW schema_table_counts AS
    WITH table_list AS (
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = '{schema_name}'
        AND table_name IN ({','.join([f"'{t}'" for t in table_list])})
    ),
    table_counts AS (
        SELECT 
            '{schema_name}' as schema_name,
            table_name,
            current_date as as_of_date,
            CAST(
                CASE table_name
    """
    
    for table in table_list:
        sql += f"""
                    WHEN '{table}' THEN (SELECT COUNT(*) FROM {schema_name}.{table})
        """
    
    sql += """
                END AS BIGINT
            ) as number_records
        FROM table_list
    )
    SELECT * FROM table_counts;
    """
    return sql

# Example usage
schema_name = "schema1"
table_list = ["table1", "table2", "table3"]
sql = generate_athena_sql(schema_name, table_list)
print(sql)

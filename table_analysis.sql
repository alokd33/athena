def generate_column_analysis_query(schema_name: str, table_name: str) -> str:
    """
    Generates an Athena SQL query for column analysis.
    
    Args:
        schema_name (str): The schema/database name
        table_name (str): The table name to analyze
        
    Returns:
        str: The generated Athena SQL query
    """
    query = f"""
WITH table_stats AS (
    SELECT COUNT(*) as total_count
    FROM {schema_name}.{table_name}
),
column_analysis AS (
    SELECT 
        column_name,
        data_type,
        -- Basic counts
        COUNT(*) as total_count,
        COUNT(*) - COUNT(column_name) as null_count,
        COUNT(column_name) as not_null_count,
        COUNT(DISTINCT column_name) as distinct_count,
        -- Additional useful metrics
        MIN(column_name) as min_value,
        MAX(column_name) as max_value,
        -- For numeric columns
        CASE 
            WHEN data_type IN ('integer', 'bigint', 'double', 'decimal') 
            THEN AVG(CAST(column_name AS double))
            ELSE NULL 
        END as avg_value
    FROM {schema_name}.{table_name}
    CROSS JOIN UNNEST(
        ARRAY(
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = '{schema_name}' 
            AND table_name = '{table_name}'
        )
    ) AS t(column_name)
    GROUP BY column_name, data_type
)

SELECT 
    ca.column_name,
    ca.data_type,
    ts.total_count as table_total_count,
    ca.total_count as column_total_count,
    ca.null_count,
    ca.not_null_count,
    ca.distinct_count,
    -- Calculate percentages
    ROUND((ca.null_count * 100.0) / NULLIF(ca.total_count, 0), 2) as null_percentage,
    ROUND((ca.not_null_count * 100.0) / NULLIF(ca.total_count, 0), 2) as not_null_percentage,
    ROUND((ca.distinct_count * 100.0) / NULLIF(ca.total_count, 0), 2) as distinct_percentage,
    -- Data quality indicators
    CASE 
        WHEN ca.null_count = ca.total_count THEN 'CRITICAL: All values are NULL'
        WHEN ca.null_count > ca.total_count * 0.5 THEN 'WARNING: High NULL rate'
        WHEN ca.distinct_count = 1 THEN 'WARNING: Single distinct value'
        WHEN ca.distinct_count = ca.not_null_count THEN 'INFO: All values are unique'
        WHEN ca.distinct_count < ca.not_null_count * 0.1 THEN 'WARNING: Low cardinality'
        ELSE 'OK'
    END as data_quality_status,
    -- Additional metrics
    ca.min_value,
    ca.max_value,
    ca.avg_value,
    -- Data type specific checks
    CASE 
        WHEN ca.data_type LIKE '%date%' THEN 'Date column - check for valid date ranges'
        WHEN ca.data_type LIKE '%varchar%' THEN 'String column - check for empty strings'
        WHEN ca.data_type IN ('integer', 'bigint', 'double', 'decimal') THEN 'Numeric column - check for outliers'
        ELSE 'Standard column'
    END as column_type_notes
FROM column_analysis ca
CROSS JOIN table_stats ts
ORDER BY 
    CASE 
        WHEN ca.null_count = ca.total_count THEN 1
        WHEN ca.null_count > ca.total_count * 0.5 THEN 2
        WHEN ca.distinct_count = 1 THEN 3
        ELSE 4
    END,
    ca.column_name;
"""
    return query

def save_query_to_file(query: str, filename: str = "athena_query.sql") -> None:
    """
    Saves the generated query to a file.
    
    Args:
        query (str): The SQL query to save
        filename (str): The name of the file to save to
    """
    with open(filename, 'w') as f:
        f.write(query)
    print(f"Query saved to {filename}")

# Example usage
if __name__ == "__main__":
    # Configuration
    SCHEMA_NAME = "your_database"
    TABLE_NAME = "your_table"
    
    # Generate the query
    query = generate_column_analysis_query(SCHEMA_NAME, TABLE_NAME)
    
    # Save to file
    save_query_to_file(query)
    
    # Or print to console
    print("\nGenerated Query:")
    print(query)


-- To use this code:
-- Save it in a file (e.g., generate_athena_query.py)


from generate_athena_query import generate_column_analysis_query, save_query_to_file

# Generate query
query = generate_column_analysis_query(
    schema_name="your_database",
    table_name="your_table"
)

# Save to file
save_query_to_file(query, "my_athena_query.sql")

# Or print to console
print(query)

CREATE TABLE sales_db.customer_data (
    customer_id BIGINT,
    customer_name VARCHAR,
    email VARCHAR,
    age INT,
    registration_date DATE,
    total_purchases DECIMAL(10,2),
    is_active BOOLEAN,
    last_login TIMESTAMP
);

WITH table_stats AS (
    SELECT COUNT(*) as total_count
    FROM sales_db.customer_data
),
column_analysis AS (
    SELECT 
        column_name,
        data_type,
        -- Basic counts
        COUNT(*) as total_count,
        COUNT(*) - COUNT(column_name) as null_count,
        COUNT(column_name) as not_null_count,
        COUNT(DISTINCT column_name) as distinct_count,
        -- Additional useful metrics
        MIN(column_name) as min_value,
        MAX(column_name) as max_value,
        -- For numeric columns
        CASE 
            WHEN data_type IN ('integer', 'bigint', 'double', 'decimal') 
            THEN AVG(CAST(column_name AS double))
            ELSE NULL 
        END as avg_value
    FROM sales_db.customer_data
    CROSS JOIN UNNEST(
        ARRAY(
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'sales_db' 
            AND table_name = 'customer_data'
        )
    ) AS t(column_name)
    GROUP BY column_name, data_type
)

SELECT 
    ca.column_name,
    ca.data_type,
    ts.total_count as table_total_count,
    ca.total_count as column_total_count,
    ca.null_count,
    ca.not_null_count,
    ca.distinct_count,
    -- Calculate percentages
    ROUND((ca.null_count * 100.0) / NULLIF(ca.total_count, 0), 2) as null_percentage,
    ROUND((ca.not_null_count * 100.0) / NULLIF(ca.total_count, 0), 2) as not_null_percentage,
    ROUND((ca.distinct_count * 100.0) / NULLIF(ca.total_count, 0), 2) as distinct_percentage,
    -- Data quality indicators
    CASE 
        WHEN ca.null_count = ca.total_count THEN 'CRITICAL: All values are NULL'
        WHEN ca.null_count > ca.total_count * 0.5 THEN 'WARNING: High NULL rate'
        WHEN ca.distinct_count = 1 THEN 'WARNING: Single distinct value'
        WHEN ca.distinct_count = ca.not_null_count THEN 'INFO: All values are unique'
        WHEN ca.distinct_count < ca.not_null_count * 0.1 THEN 'WARNING: Low cardinality'
        ELSE 'OK'
    END as data_quality_status,
    -- Additional metrics
    ca.min_value,
    ca.max_value,
    ca.avg_value,
    -- Data type specific checks
    CASE 
        WHEN ca.data_type LIKE '%date%' THEN 'Date column - check for valid date ranges'
        WHEN ca.data_type LIKE '%varchar%' THEN 'String column - check for empty strings'
        WHEN ca.data_type IN ('integer', 'bigint', 'double', 'decimal') THEN 'Numeric column - check for outliers'
        ELSE 'Standard column'
    END as column_type_notes
FROM column_analysis ca
CROSS JOIN table_stats ts
ORDER BY 
    CASE 
        WHEN ca.null_count = ca.total_count THEN 1
        WHEN ca.null_count > ca.total_count * 0.5 THEN 2
        WHEN ca.distinct_count = 1 THEN 3
        ELSE 4
    END,
    ca.column_name;


Sample Output (assuming the table has 1000 records):

column_name       | data_type | table_total_count | column_total_count | null_count | not_null_count | distinct_count | null_percentage | not_null_percentage | distinct_percentage | data_quality_status          | min_value    | max_value    | avg_value | column_type_notes
-----------------|-----------|-------------------|-------------------|------------|----------------|----------------|-----------------|---------------------|---------------------|-----------------------------|--------------|--------------|-----------|------------------
customer_id      | bigint    | 1000             | 1000             | 0          | 1000          | 1000          | 0.00           | 100.00             | 100.00             | INFO: All values are unique | 1            | 1000         | 500.5     | Numeric column - check for outliers
customer_name    | varchar   | 1000             | 1000             | 0          | 1000          | 950           | 0.00           | 100.00             | 95.00              | OK                         | 'A'          | 'Z'          | NULL      | String column - check for empty strings
email            | varchar   | 1000             | 1000             | 5          | 995           | 995           | 0.50           | 99.50              | 99.50              | OK                         | 'a@a.com'    | 'z@z.com'    | NULL      | String column - check for empty strings
age              | integer   | 1000             | 1000             | 0          | 1000          | 50            | 0.00           | 100.00             | 5.00               | WARNING: Low cardinality    | 18           | 65           | 35.2      | Numeric column - check for outliers
registration_date| date      | 1000             | 1000             | 0          | 1000          | 365           | 0.00           | 100.00             | 36.50              | OK                         | '2020-01-01' | '2023-12-31' | NULL      | Date column - check for valid date ranges
total_purchases  | decimal   | 1000             | 1000             | 0          | 1000          | 1000          | 0.00           | 100.00             | 100.00             | INFO: All values are unique | 0.00         | 9999.99      | 2500.50   | Numeric column - check for outliers
is_active        | boolean   | 1000             | 1000             | 0          | 1000          | 2             | 0.00           | 100.00             | 0.20               | WARNING: Low cardinality    | false        | true         | NULL      | Standard column
last_login       | timestamp | 1000             | 1000             | 50         | 950           | 900           | 5.00           | 95.00              | 90.00              | OK                         | '2023-01-01' | '202

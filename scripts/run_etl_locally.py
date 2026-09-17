import os
import duckdb

script_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(script_dir)
DATA_RAW_DIR = os.path.join(project_dir, 'data', 'raw')
DUCKDB_PATH = os.path.join(project_dir, 'data', 'warehouse.duckdb')

def transform_and_load_data():
    sales_file = os.path.join(DATA_RAW_DIR, 'sales_data.json')
    weather_file = os.path.join(DATA_RAW_DIR, 'weather_data.csv')
    
    print(f"Connecting to local DuckDB at {DUCKDB_PATH}")
    conn = duckdb.connect(DUCKDB_PATH)
    
    query = f"""
    -- 1. Create a View of the Flattened Sales Data
    CREATE OR REPLACE TEMP VIEW flattened_sales AS
    SELECT 
        order_id,
        CAST(timestamp AS DATE) AS sale_date,
        customer.customer_id AS customer_id,
        customer.profile.city AS city,
        UNNEST(items).product_category AS category,
        UNNEST(items).quantity AS quantity,
        UNNEST(items).unit_price AS price,
        UNNEST(items).quantity * UNNEST(items).unit_price AS total_revenue
    FROM read_json_auto('{sales_file}');
    
    -- 2. Create a View of the Weather Data (Handling missing/empty conditions)
    CREATE OR REPLACE TEMP VIEW clean_weather AS
    SELECT 
        CAST(date AS DATE) AS date,
        location AS city,
        TRY_CAST(temperature_celsius AS DOUBLE) AS temp_c,
        COALESCE(NULLIF(condition, ''), 'Unknown') AS condition
    FROM read_csv_auto('{weather_file}');
    
    -- 3. Join, Aggregate, and Load into Final Fact Table
    CREATE TABLE IF NOT EXISTS fact_weather_sales (
        sale_date DATE,
        city VARCHAR,
        weather_condition VARCHAR,
        avg_temp DOUBLE,
        product_category VARCHAR,
        total_items_sold INTEGER,
        total_revenue DOUBLE
    );
    
    DROP TABLE IF EXISTS fact_weather_sales;
    
    CREATE TABLE fact_weather_sales AS
    SELECT 
        s.sale_date,
        s.city,
        w.condition AS weather_condition,
        AVG(w.temp_c) AS avg_temp,
        s.category AS product_category,
        SUM(s.quantity) AS total_items_sold,
        SUM(s.total_revenue) AS total_revenue
    FROM flattened_sales s
    LEFT JOIN clean_weather w 
        ON s.sale_date = w.date AND s.city = w.city
    WHERE s.category IS NOT NULL
    GROUP BY 1, 2, 3, 5;
    """
    
    print("Executing Transformation and Load Query...")
    conn.execute(query)
    
    result = conn.execute("SELECT * FROM fact_weather_sales LIMIT 5;").fetchdf()
    print("\nSample of Data Loaded into Warehouse:")
    print(result)
    
    conn.close()
    print("\nETL Process Completed Successfully.")

if __name__ == "__main__":
    print("Starting local ETL pipeline execution...")
    transform_and_load_data()
    print("\nYou can now run the verify_results.py script!")

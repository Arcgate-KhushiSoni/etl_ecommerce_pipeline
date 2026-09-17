from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import duckdb

# Airflow standard config
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Directories mapped in Docker
DATA_RAW_DIR = '/opt/airflow/data/raw'
DATA_PROCESSED_DIR = '/opt/airflow/data/processed'
DUCKDB_PATH = '/opt/airflow/data/warehouse.duckdb'

def transform_and_load_data():
    """
    Reads the messy JSON and CSV data, cleans, joins, aggregates,
    and loads it into a DuckDB warehouse.
    """
    sales_file = os.path.join(DATA_RAW_DIR, 'sales_data.json')
    weather_file = os.path.join(DATA_RAW_DIR, 'weather_data.csv')
    
    print(f"Connecting to DuckDB at {DUCKDB_PATH}")
    conn = duckdb.connect(DUCKDB_PATH)
    
    # Using DuckDB's powerful SQL to read and flatten JSON directly
    # We will unnest the 'items' array, and join with the weather CSV
    
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
    
    -- Clear out old data if we were doing an incremental load, 
    -- but here we just drop and recreate for simplicity of the demo
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
    
    # Verify by printing a small sample
    result = conn.execute("SELECT * FROM fact_weather_sales LIMIT 5;").fetchdf()
    print("Sample of Data Loaded into Warehouse:")
    print(result)
    
    conn.close()
    print("ETL Process Completed Successfully.")

# Define the DAG
with DAG(
    'ecommerce_weather_etl',
    default_args=default_args,
    description='A complex ETL pipeline joining JSON and CSV data',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    # In a real project, Extract would be its own task.
    # We simulate it with a dummy task here since data is already generated.
    from airflow.operators.dummy import DummyOperator
    
    extract_task = DummyOperator(
        task_id='extract_data'
    )
    
    transform_load_task = PythonOperator(
        task_id='transform_and_load_duckdb',
        python_callable=transform_and_load_data,
    )
    
    # Define dependencies
    extract_task >> transform_load_task


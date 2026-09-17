import duckdb
import os
import pandas as pd

script_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(script_dir)
db_path = os.path.join(project_dir, 'data', 'warehouse.duckdb')

if not os.path.exists(db_path):
    print(f"Database not found at {db_path}. Did the Airflow DAG run successfully?")
    exit(1)

conn = duckdb.connect(db_path)

print("--------------------------------------------------")
print("1. Total Sales by Weather Condition")
print("--------------------------------------------------")
query1 = """
SELECT 
    weather_condition, 
    SUM(total_items_sold) as items_sold,
    SUM(total_revenue) as revenue
FROM fact_weather_sales
GROUP BY 1
ORDER BY revenue DESC;
"""
print(conn.execute(query1).fetchdf())
print("\n")

print("--------------------------------------------------")
print("2. Top Selling Categories on Clear Days")
print("--------------------------------------------------")
query2 = """
SELECT 
    product_category,
    SUM(total_items_sold) as items_sold,
    SUM(total_revenue) as revenue
FROM fact_weather_sales
WHERE weather_condition = 'Clear'
GROUP BY 1
ORDER BY revenue DESC;
"""
print(conn.execute(query2).fetchdf())
print("\n")

conn.close()

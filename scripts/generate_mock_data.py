import json
import csv
import random
from datetime import datetime, timedelta
import os

# Ensure we are in the correct directory (or relative to script location)
script_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(script_dir)
data_raw_dir = os.path.join(project_dir, 'data', 'raw')

# Ensure directory exists
os.makedirs(data_raw_dir, exist_ok=True)

cities = ["New York", "London", "Tokyo", "Mumbai", "Sydney"]
weather_conditions = ["Clear", "Rain", "Clouds", "Snow"]
categories = ["Electronics", "Clothing", "Home", "Books", "Umbrellas"]

# Generate Dates for the last 30 days
end_date = datetime.now()
start_date = end_date - timedelta(days=30)
date_list = [(start_date + timedelta(days=x)).strftime("%Y-%m-%d") for x in range(31)]

def generate_weather_data():
    weather_data = []
    for date in date_list:
        for city in cities:
            # Introduce some missing data to simulate real-world messiness
            if random.random() > 0.95:
                condition = ""
                temp = ""
            else:
                condition = random.choice(weather_conditions)
                # Ensure umbrellas sell more when raining (we will simulate this in sales)
                temp = round(random.uniform(-5.0, 35.0), 1)
            
            weather_data.append({
                "date": date,
                "location": city,
                "temperature_celsius": temp,
                "condition": condition
            })
    
    weather_file = os.path.join(data_raw_dir, 'weather_data.csv')
    with open(weather_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["date", "location", "temperature_celsius", "condition"])
        writer.writeheader()
        writer.writerows(weather_data)
    print(f"Generated {weather_file}")

def generate_sales_data():
    sales_data = []
    for _ in range(1000): # 1000 orders
        date = random.choice(date_list)
        city = random.choice(cities)
        
        # Simulating complex, nested JSON structure
        order = {
            "order_id": f"ORD-{random.randint(10000, 99999)}",
            "timestamp": date + f"T{random.randint(8, 23):02d}:{random.randint(0, 59):02d}:00Z",
            "customer": {
                "customer_id": f"CUST-{random.randint(100, 999)}",
                "profile": {
                    "city": city,
                    "is_premium": random.choice([True, False])
                }
            },
            "items": []
        }
        
        num_items = random.randint(1, 5)
        for _ in range(num_items):
            # Try to skew umbrella sales to a specific city conditionally, or just randomly
            category = random.choice(categories)
            quantity = random.randint(1, 3)
            price = round(random.uniform(10.0, 150.0), 2)
            
            order["items"].append({
                "product_category": category,
                "quantity": quantity,
                "unit_price": price
            })
            
        # Introduce a bad record
        if random.random() > 0.98:
            order["items"] = None # Will cause issues during flattening if not handled
            
        sales_data.append(order)

    sales_file = os.path.join(data_raw_dir, 'sales_data.json')
    with open(sales_file, 'w') as f:
        json.dump(sales_data, f, indent=4)
    print(f"Generated {sales_file}")

if __name__ == "__main__":
    generate_weather_data()
    generate_sales_data()

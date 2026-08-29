import mysql.connector
import os
import random
import time
from datetime import datetime


# --------------------------------------------
# 1. Connect to MySQL
# --------------------------------------------
connection = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = os.getenv("MYSQL_PASSWORD"),
    database = "buyonline"
)

cursor = connection.cursor()
print("Connected to database buyonline!")

# ---------------------------------------------------------
# 2. Product data used by the generator
# ---------------------------------------------------------

product_names = [
    "Bluetooth Mouse",
    "USB-C Hub",
    "Phone Stand",
    "Laptop Sleeve",
    "Fitness Band",
    "Water Bottle",
    "Desk Organizer",
    "Wireless Charger",
    "LED Desk Lamp",
    "Travel Backpack",
    "Gym Gloves",
    "Notebook",
    "Coffee Tumbler",
    "Keyboard Wrist Rest",
    "Cable Organizer"
]

categories = [
    "Electronics",
    "Fitness",
    "Home",
    "Accessories",
    "Travel",
    "Stationery"
]

# ---------------------------------------------------------
# 3. Find the next Product ID
# ---------------------------------------------------------

cursor.execute("SELECT MAX(id) FROM products")
result = cursor.fetchone()

if result[0] is None:
    next_id = 1
else:
    next_id = result[0] + 1

print(f"Starting Product ID: {next_id}")

# ---------------------------------------------------------
# 4. Insert query
# ---------------------------------------------------------

insert_query = """
    INSERT INTO products (id, name, category, price, last_updated)
    VALUES (%s, %s, %s, %s, %s)
"""

# ---------------------------------------------------------
# 5. Continuous Data generation
# ---------------------------------------------------------
try:
    while True:
        name = random.choice(product_names)
        category = random.choice(categories)
        price = round(random.uniform(199,9999),2)
        last_updated = datetime.now()
        product = (
            next_id,
            name,
            category,
            price,
            last_updated
        )
            
        cursor.execute(insert_query,product)

        connection.commit()
        print(
            f"Product inserted | "
            f"ID: {next_id} | "
            f"Name: {name} | "
            f"Category: {category} | "
            f"Price: {price} | "
            f"Timestamp: {last_updated}"
        )

        next_id += 1

        # 5 second wait before generating next record
        time.sleep(1)

except KeyboardInterrupt:
    print("\nData generator stopped by user")

finally:
    cursor.close()
    connection.close()
    print("MySQL connection closed")
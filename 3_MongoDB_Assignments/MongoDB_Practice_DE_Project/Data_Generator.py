import mysql.connector
import os
import random
import time
from datetime import datetime, timedelta


# --------------------------------------------
# 1. Connect to MySQL
# --------------------------------------------
connection = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = os.getenv("MYSQL_PASSWORD"),
    database = "ecommerce_source"
)

cursor = connection.cursor()
print("Connected to MySQL Successfully.")

# ---------------------------------------------------------
# 2. Product and customers data used by the generator
# ---------------------------------------------------------

products = [
    # -------------------------------------------------------------------------
    # Electronics
    # -------------------------------------------------------------------------
    {
        "product_id": 101,
        "product_name": "Laptop",
        "category": "Electronics",
        "unit_price": 65000.00
    },
    {
        "product_id": 102,
        "product_name": "Smartphone",
        "category": "Electronics",
        "unit_price": 35000.00
    },
    {
        "product_id": 103,
        "product_name": "Tablet",
        "category": "Electronics",
        "unit_price": 28000.00
    },
    {
        "product_id": 104,
        "product_name": "Smart TV",
        "category": "Electronics",
        "unit_price": 55000.00
    },
    {
        "product_id": 105,
        "product_name": "Monitor",
        "category": "Electronics",
        "unit_price": 15000.00
    },
    {
        "product_id": 106,
        "product_name": "Digital Camera",
        "category": "Electronics",
        "unit_price": 48000.00
    },
    {
        "product_id": 107,
        "product_name": "Bluetooth Speaker",
        "category": "Electronics",
        "unit_price": 4500.00
    },
    {
        "product_id": 108,
        "product_name": "Gaming Console",
        "category": "Electronics",
        "unit_price": 52000.00
    },

    # -------------------------------------------------------------------------
    # Accessories
    # -------------------------------------------------------------------------
    {
        "product_id": 109,
        "product_name": "Wireless Headphones",
        "category": "Accessories",
        "unit_price": 2500.00
    },
    {
        "product_id": 110,
        "product_name": "Mechanical Keyboard",
        "category": "Accessories",
        "unit_price": 4500.00
    },
    {
        "product_id": 111,
        "product_name": "Wireless Mouse",
        "category": "Accessories",
        "unit_price": 1800.00
    },
    {
        "product_id": 112,
        "product_name": "USB-C Hub",
        "category": "Accessories",
        "unit_price": 2200.00
    },
    {
        "product_id": 113,
        "product_name": "Webcam",
        "category": "Accessories",
        "unit_price": 3500.00
    },
    {
        "product_id": 114,
        "product_name": "Laptop Stand",
        "category": "Accessories",
        "unit_price": 1800.00
    },
    {
        "product_id": 115,
        "product_name": "Wireless Charger",
        "category": "Accessories",
        "unit_price": 1600.00
    },
    {
        "product_id": 116,
        "product_name": "Power Bank",
        "category": "Accessories",
        "unit_price": 2500.00
    },

    # -------------------------------------------------------------------------
    # Storage
    # -------------------------------------------------------------------------
    {
        "product_id": 117,
        "product_name": "External SSD",
        "category": "Storage",
        "unit_price": 7000.00
    },
    {
        "product_id": 118,
        "product_name": "External Hard Drive",
        "category": "Storage",
        "unit_price": 5500.00
    },
    {
        "product_id": 119,
        "product_name": "USB Flash Drive",
        "category": "Storage",
        "unit_price": 900.00
    },
    {
        "product_id": 120,
        "product_name": "Memory Card",
        "category": "Storage",
        "unit_price": 1200.00
    },

    # -------------------------------------------------------------------------
    # Furniture
    # -------------------------------------------------------------------------
    {
        "product_id": 121,
        "product_name": "Office Chair",
        "category": "Furniture",
        "unit_price": 12000.00
    },
    {
        "product_id": 122,
        "product_name": "Standing Desk",
        "category": "Furniture",
        "unit_price": 18000.00
    },
    {
        "product_id": 123,
        "product_name": "Bookshelf",
        "category": "Furniture",
        "unit_price": 7500.00
    },
    {
        "product_id": 124,
        "product_name": "Study Table",
        "category": "Furniture",
        "unit_price": 9000.00
    },

    # -------------------------------------------------------------------------
    # Home Appliances
    # -------------------------------------------------------------------------
    {
        "product_id": 125,
        "product_name": "Air Conditioner",
        "category": "Home Appliances",
        "unit_price": 42000.00
    },
    {
        "product_id": 126,
        "product_name": "Microwave Oven",
        "category": "Home Appliances",
        "unit_price": 11000.00
    },
    {
        "product_id": 127,
        "product_name": "Air Purifier",
        "category": "Home Appliances",
        "unit_price": 14000.00
    },
    {
        "product_id": 128,
        "product_name": "Coffee Maker",
        "category": "Home Appliances",
        "unit_price": 6500.00
    },
    {
        "product_id": 129,
        "product_name": "Electric Kettle",
        "category": "Home Appliances",
        "unit_price": 1800.00
    },

    # -------------------------------------------------------------------------
    # Books & Learning
    # -------------------------------------------------------------------------
    {
        "product_id": 130,
        "product_name": "Programming Book",
        "category": "Books",
        "unit_price": 850.00
    },
    {
        "product_id": 131,
        "product_name": "Data Science Book",
        "category": "Books",
        "unit_price": 1200.00
    },
    {
        "product_id": 132,
        "product_name": "Business Book",
        "category": "Books",
        "unit_price": 700.00
    },
    {
        "product_id": 133,
        "product_name": "Notebook",
        "category": "Stationery",
        "unit_price": 250.00
    },
    {
        "product_id": 134,
        "product_name": "Premium Pen Set",
        "category": "Stationery",
        "unit_price": 600.00
    },

    # -------------------------------------------------------------------------
    # Fitness
    # -------------------------------------------------------------------------
    {
        "product_id": 135,
        "product_name": "Yoga Mat",
        "category": "Fitness",
        "unit_price": 1200.00
    },
    {
        "product_id": 136,
        "product_name": "Dumbbell Set",
        "category": "Fitness",
        "unit_price": 4500.00
    },
    {
        "product_id": 137,
        "product_name": "Resistance Bands",
        "category": "Fitness",
        "unit_price": 900.00
    },
    {
        "product_id": 138,
        "product_name": "Treadmill",
        "category": "Fitness",
        "unit_price": 55000.00
    },

    # -------------------------------------------------------------------------
    # Clothing
    # -------------------------------------------------------------------------
    {
        "product_id": 139,
        "product_name": "Running Shoes",
        "category": "Clothing",
        "unit_price": 4500.00
    },
    {
        "product_id": 140,
        "product_name": "Hoodie",
        "category": "Clothing",
        "unit_price": 2200.00
    },
    {
        "product_id": 141,
        "product_name": "Jeans",
        "category": "Clothing",
        "unit_price": 2800.00
    },
    {
        "product_id": 142,
        "product_name": "Backpack",
        "category": "Clothing",
        "unit_price": 2500.00
    }
]


customers = [
    {
        "customer_id": 1001,
        "customer_name": "Aarav Sharma"
    },
    {
        "customer_id": 1002,
        "customer_name": "Priya Mehta"
    },
    {
        "customer_id": 1003,
        "customer_name": "Rohan Verma"
    },
    {
        "customer_id": 1004,
        "customer_name": "Ananya Kapoor"
    },
    {
        "customer_id": 1005,
        "customer_name": "Vikram Singh"
    },
    {
        "customer_id": 1006,
        "customer_name": "Sneha Iyer"
    },
    {
        "customer_id": 1007,
        "customer_name": "Aditya Rao"
    },
    {
        "customer_id": 1008,
        "customer_name": "Neha Gupta"
    },
    {
        "customer_id": 1009,
        "customer_name": "Karan Malhotra"
    },
    {
        "customer_id": 1010,
        "customer_name": "Ishita Nair"
    },
    {
        "customer_id": 1011,
        "customer_name": "Rahul Deshmukh"
    },
    {
        "customer_id": 1012,
        "customer_name": "Meera Joshi"
    },
    {
        "customer_id": 1013,
        "customer_name": "Arjun Patel"
    },
    {
        "customer_id": 1014,
        "customer_name": "Kavya Menon"
    },
    {
        "customer_id": 1015,
        "customer_name": "Siddharth Agarwal"
    },
    {
        "customer_id": 1016,
        "customer_name": "Pooja Choudhary"
    },
    {
        "customer_id": 1017,
        "customer_name": "Nikhil Bansal"
    },
    {
        "customer_id": 1018,
        "customer_name": "Riya Saxena"
    },
    {
        "customer_id": 1019,
        "customer_name": "Manish Tiwari"
    },
    {
        "customer_id": 1020,
        "customer_name": "Divya Kulkarni"
    },
    {
        "customer_id": 1021,
        "customer_name": "Saurabh Mishra"
    },
    {
        "customer_id": 1022,
        "customer_name": "Tanvi Shah"
    },
    {
        "customer_id": 1023,
        "customer_name": "Varun Khanna"
    },
    {
        "customer_id": 1024,
        "customer_name": "Simran Kaur"
    },
    {
        "customer_id": 1025,
        "customer_name": "Akash Yadav"
    },
    {
        "customer_id": 1026,
        "customer_name": "Shreya Das"
    },
    {
        "customer_id": 1027,
        "customer_name": "Mohit Jain"
    },
    {
        "customer_id": 1028,
        "customer_name": "Nandini Roy"
    },
    {
        "customer_id": 1029,
        "customer_name": "Harsh Vardhan"
    },
    {
        "customer_id": 1030,
        "customer_name": "Aditi Srivastava"
    }
]

payment_methods = [
    "Credit Card",
    "Debit Card",
    "UPI",
    "Net Banking",
    "Cash on Delivery",
    "Wallet"
]

order_statuses = [
    "Delivered",
    "Shipped",
    "Processing",
    "Cancelled",
    "Returned"
]

cities = [
    "Mumbai",
    "Delhi",
    "Bengaluru",
    "Hyderabad",
    "Chennai",
    "Kolkata",
    "Pune",
    "Ahmedabad",
    "Jaipur",
    "Lucknow",
    "Kanpur",
    "Chandigarh",
    "Indore",
    "Bhopal",
    "Patna",
    "Nagpur",
    "Surat",
    "Noida",
    "Gurugram",
    "Kochi"
]

# ---------------------------------------------------------
# Historical date range for generated orders
# ---------------------------------------------------------

start_date = datetime(2025, 1, 1)
end_date = datetime(2026, 9, 20)

date_range = (end_date - start_date).days

# ---------------------------------------------------------
# 3. Fetch the latest order_id
# ---------------------------------------------------------

cursor.execute("SELECT MAX(order_id) FROM orders")
result = cursor.fetchone()

if result[0] is None:
    next_order_id = 1
else:
    next_order_id = result[0] + 1

print(f"Starting Order ID: {next_order_id}")

# ---------------------------------------------------------
# 4. Insert Query
# ---------------------------------------------------------

insert_query = """
    INSERT INTO orders (order_id, customer_id, customer_name, product_id, product_name, category, quantity, unit_price, order_date, payment_method, order_status, city)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

# ---------------------------------------------------------
# 5. Continuous Data generation and insertion
# ---------------------------------------------------------
try:
    while True:
        customer = random.choice(customers)
        customer_id = customer["customer_id"]
        customer_name = customer["customer_name"]
        product = random.choice(products)
        product_id = product["product_id"]
        product_name = product["product_name"]
        category = product["category"]
        quantity = random.choices([1, 2, 3, 4, 5],weights=[45, 30, 15, 7, 3])[0]
        unit_price = product["unit_price"]
        random_days = random.randint(0, date_range)
        order_date = (start_date + timedelta(days=random_days)).date()
        payment_method = random.choice(payment_methods)
        order_status = random.choice(order_statuses)
        city = random.choice(cities)
        order_data = (
            next_order_id,
            customer_id,
            customer_name,
            product_id,
            product_name,
            category,
            quantity,
            unit_price,
            order_date,
            payment_method,
            order_status,
            city
        )

        cursor.execute(insert_query,order_data)

        connection.commit()

        print(
            f"Order inserted | "
            f"ID: {next_order_id} | "
            f"Customer: {customer_name} | "
            f"Product: {product_name} | "
            f"Category: {category} | "
            f"Quantity: {quantity} | "
            f"Price: {unit_price} | "
            f"Payment: {payment_method} | "
            f"Status: {order_status} | "
            f"City: {city} | "
            f"Date: {order_date}"
        )

        next_order_id +=1

        time.sleep(1)

except KeyboardInterrupt:
     print("\nData generator stopped by user")

finally:
    cursor.close()
    connection.close()
    print("MySQL connection closed")
import mysql.connector
import os
import random
import time
from datetime import datetime, timedelta
import json

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

# --------------------------------------------
# 2. Reading the checkpoint
# --------------------------------------------

try:
    with open("/Users/vvsvivek/Library/CloudStorage/OneDrive-Personal/Skills & Career/3. Data Engineering/Data_Engineering_Assignments/3_MongoDB_Assignments/MongoDB_Practice_DE_Project/checkpoint.json","r") as file:
        checkpoint = json.load(file)
        last_order_id = checkpoint["last_order_id"]
        if not last_order_id:
            last_order_id = 0
except (FileNotFoundError, KeyError, json.JSONDecodeError):
    last_order_id = 0

# --------------------------------------------
# 3. Fetching Data from mysql 
# --------------------------------------------
fetch_query = """
    SELECT
        order_id,
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
    FROM orders
    WHERE order_id > %s
"""

cursor.execute(fetch_query,(last_order_id,))
rows = cursor.fetchall()
last_order_id = rows[-1][0]


# -------------------------------------------------------------
# 4. Creating a python dictionary for inserting data in MongoDB
# -------------------------------------------------------------

documents = []

for row in rows:

    document = {}

    document["_id"] = row[0]
    document["order_id"] = row[0]

    document["customer"] = {}
    document["customer"]["customer_id"] = row[1]
    document["customer"]["customer_name"] = row[2]

    document["product"] = {}
    document["product"]["product_id"] = row[3]
    document["product"]["product_name"] = row[4]
    document["product"]["category"] = row[5]
    document["product"]["unit_price"] = row[7]

    document["order_details"] = {}
    document["order_details"]["quantity"] = row[6]
    document["order_details"]["order_date"] = row[8]
    document["order_details"]["payment_method"] = row[9]
    document["order_details"]["order_status"] = row[10]

    document["shipping"] = {}
    document["shipping"]["city"] = row[11]

    documents.append(document)

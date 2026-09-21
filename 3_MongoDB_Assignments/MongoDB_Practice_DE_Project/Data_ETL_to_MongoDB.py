import mysql.connector
import os
import json
from pymongo import MongoClient


# =============================================================================
# 1. Connect to MySQL
# =============================================================================

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password=os.getenv("MYSQL_PASSWORD"),
    database="ecommerce_source"
)

cursor = connection.cursor()

print("MySQL connection established successfully.")


# =============================================================================
# 2. Read the Last Processed Order ID from Checkpoint
# =============================================================================
# The checkpoint keeps track of the last order that was successfully
# processed and loaded into MongoDB.
#
# On the next ETL run, only orders with order_id greater than this value
# will be fetched from MySQL.

try:
    with open(
        "/Users/vvsvivek/Library/CloudStorage/OneDrive-Personal/Skills & Career/3. Data Engineering/Data_Engineering_Assignments/3_MongoDB_Assignments/MongoDB_Practice_DE_Project/checkpoint.json",
        "r"
    ) as file:

        checkpoint = json.load(file)
        last_order_id = checkpoint["last_order_id"]

        if not last_order_id:
            last_order_id = 0

except (FileNotFoundError, KeyError, json.JSONDecodeError):
    # If the checkpoint file doesn't exist or is invalid,
    # start processing from the beginning.
    last_order_id = 0

print(f"Last processed order ID: {last_order_id}")


# =============================================================================
# 3. Fetch New Orders from MySQL
# =============================================================================
# Only fetch orders that were created after the last successfully
# processed order. This makes the ETL incremental instead of processing
# the entire source table on every run.

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

cursor.execute(fetch_query, (last_order_id,))
rows = cursor.fetchall()

print(f"New orders fetched from MySQL: {len(rows)}")


# =============================================================================
# 4. Transform MySQL Rows into MongoDB Documents
# =============================================================================
# MySQL stores the order information in a flat relational structure.
# Here, the data is transformed into nested Python dictionaries that
# represent the document structure used in MongoDB.
#
# Related fields are grouped into:
#   - customer
#   - product
#   - order_details
#   - shipping

documents = []

for row in rows:

    document = {}

    # Use MySQL order_id as MongoDB _id to uniquely identify each order
    # and prevent duplicate documents during reprocessing.
    document["_id"] = row[0]
    document["order_id"] = row[0]

    # Customer information
    document["customer"] = {}
    document["customer"]["customer_id"] = row[1]
    document["customer"]["customer_name"] = row[2]

    # Product information
    document["product"] = {}
    document["product"]["product_id"] = row[3]
    document["product"]["product_name"] = row[4]
    document["product"]["category"] = row[5]

    # MySQL DECIMAL values are returned as Decimal objects.
    # Convert to float so PyMongo can encode the value as BSON.
    document["product"]["unit_price"] = float(row[7])

    # Order-specific information
    document["order_details"] = {}
    document["order_details"]["quantity"] = row[6]

    # Convert Python date object to ISO-format string.
    document["order_details"]["order_date"] = row[8].isoformat()

    document["order_details"]["payment_method"] = row[9]
    document["order_details"]["order_status"] = row[10]

    # Shipping information
    document["shipping"] = {}
    document["shipping"]["city"] = row[11]

    documents.append(document)

print(f"Orders transformed into MongoDB documents: {len(documents)}")


# =============================================================================
# 5. Connect to MongoDB
# =============================================================================

# Read the MongoDB Atlas connection string from the environment variable.
mongodb_uri = os.getenv("MONGODB_URI")

# Create a MongoDB client using the connection string.
client = MongoClient(mongodb_uri)

# Select the target database and collection.
db = client["de_practice"]
collection = db["orders"]

print("MongoDB connection established successfully.")


# =============================================================================
# 6. Insert Documents into MongoDB and Update Checkpoint
# =============================================================================
# Documents are inserted one at a time so that we can track the last
# successfully inserted order.
#
# The checkpoint is updated only after at least one document has been
# successfully inserted.

if rows:

    total_orders = len(documents)
    total_orders_inserted = 0
    last_successful_order_id = last_order_id

    for document in documents:

        try:
            result = collection.insert_one(document)

            print(f"Inserted Order ID: {result.inserted_id}")

            total_orders_inserted += 1
            last_successful_order_id = document["order_id"]

        except Exception as e:

            print(
                f"Failed to insert Order ID "
                f"{document['order_id']}: {e}"
            )

            # Stop processing so that the checkpoint remains at the
            # last successfully inserted order.
            break

    # Report the result of the MongoDB load.
    if total_orders_inserted == total_orders:

        print(
            f"MongoDB load completed successfully: "
            f"{total_orders_inserted}/{total_orders} records inserted."
        )

    else:

        print(
            f"Partial MongoDB load: "
            f"{total_orders_inserted}/{total_orders} records inserted."
        )

    # Update the checkpoint only if at least one record was inserted.
    if total_orders_inserted > 0:

        new_checkpoint = {
            "last_order_id": last_successful_order_id
        }

        print("Updating checkpoint...")

        try:

            with open(
                "/Users/vvsvivek/Library/CloudStorage/OneDrive-Personal/Skills & Career/3. Data Engineering/Data_Engineering_Assignments/3_MongoDB_Assignments/MongoDB_Practice_DE_Project/checkpoint.json",
                "w"
            ) as file:

                json.dump(new_checkpoint, file)

            print(
                f"Checkpoint updated successfully to "
                f"order_id: {new_checkpoint['last_order_id']}"
            )

        except OSError as e:

            print(f"Failed to update checkpoint: {e}")

    else:

        print(
            "Checkpoint was not updated because "
            "no records were inserted."
        )

else:

    print("No new orders found in MySQL.")
    print("Checkpoint was not updated.")
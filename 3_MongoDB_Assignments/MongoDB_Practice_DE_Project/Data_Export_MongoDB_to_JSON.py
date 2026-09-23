import os
import json
from pymongo import MongoClient

# =============================================================================
# 1. MongoDB Connection
# =============================================================================

mongodb_uri = os.getenv("MONGODB_URI")
client = MongoClient(mongodb_uri)

client.admin.command("ping")
print("MongoDB connection successful!")

db = client["de_practice"]
collection = db["orders"]

# =============================================================================
# 2. Fetching the checkpoint
# =============================================================================
try:
    with open(
        "/Users/vvsvivek/Library/CloudStorage/OneDrive-Personal/Skills & Career/3. Data Engineering/Data_Engineering_Assignments/3_MongoDB_Assignments/MongoDB_Practice_DE_Project/mongodb_checkpoint.json",
        "r"
    ) as file:
        checkpoint = json.load(file)
        last_order_id_fetched = checkpoint["last_order_id_fetched"]

        if not last_order_id_fetched:
            last_order_id_fetched = 0

except (FileNotFoundError, KeyError, json.JSONDecodeError):
    # If the checkpoint file doesn't exist or is invalid,
    # start processing from the beginning.
    last_order_id_fetched = 0

print(f"Last fetched order ID: {last_order_id_fetched}")

# =============================================================================
# 3. Fetching the latest data from MongoDB
# =============================================================================

print("Checking MongoDB for new orders...")

documents = list(collection.find(
    {"_id": {"$gt": last_order_id_fetched}}
    ))

if documents:
    first_order_id = documents[0]["_id"]
    last_order_id = documents[-1]["_id"]

    print(f"New orders found: {len(documents)}")
    print(f"Order ID range: {first_order_id} - {last_order_id}")

    # =============================================================================
    # 4. Exporting Documents to JSON
    # =============================================================================

    file_name = f"orders_{first_order_id}_{last_order_id}.json"
    export_path = f"/Users/vvsvivek/Library/CloudStorage/OneDrive-Personal/Skills & Career/3. Data Engineering/Data_Engineering_Assignments/3_MongoDB_Assignments/MongoDB_Practice_DE_Project/exports/{file_name}"

    print(f"Exporting documents to: {file_name}")

    try:
        with open(export_path,"w") as file:
            json.dump(documents,file,indent=4)

        print(f"Successfully exported {len(documents)} orders to JSON.")

        # =============================================================================
        # 5. Updating the MongoDB Checkpoint
        # =============================================================================
        last_order_id_fetched = documents[-1]["_id"]

        with open("/Users/vvsvivek/Library/CloudStorage/OneDrive-Personal/Skills & Career/3. Data Engineering/Data_Engineering_Assignments/3_MongoDB_Assignments/MongoDB_Practice_DE_Project/mongodb_checkpoint.json",
                "w") as file:
            checkpoint = {"last_order_id_fetched": last_order_id_fetched}
            json.dump(checkpoint,file)

        print(
            f"Checkpoint updated successfully to order ID: "
            f"{last_order_id_fetched}"
        )
        print("MongoDB → JSON export completed successfully.")
    except OSError as e:
        print(f"JSON export/checkpoint update failed: {e}")
        print("Checkpoint was not advanced.")
else: 
    print("No new orders found in MongoDB.")
    print("No JSON file was created.")
    print("Checkpoint was not updated.")




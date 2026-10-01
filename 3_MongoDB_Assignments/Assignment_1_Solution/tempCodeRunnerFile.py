from pymongo import MongoClient
from pprint import pprint
import os
# Read the MongoDB Atlas connection string from the environment variable.
mongodb_uri = os.getenv("MONGODB_URI")


# Create a MongoDB client using the connection string.
client = MongoClient(mongodb_uri)

# Select the target database and collection.
db = client["de_practice"]
collection = db["logistics"]

print("MongoDB connection established successfully.")

document = collection.find_one(
    {"_id": "VCV00014872/082021"}
)

pprint(document)
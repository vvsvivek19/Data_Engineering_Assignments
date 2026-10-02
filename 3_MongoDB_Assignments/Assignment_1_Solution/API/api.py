import os

from fastapi import FastAPI
from pymongo import MongoClient


app = FastAPI()


# -------------------------------------------
# Connecting to MongoDB
# -------------------------------------------

mongodb_uri = os.getenv("MONGODB_URI")

client = MongoClient(mongodb_uri)

db = client["de_practice"]

collection = db["logistics"]

# Verify that the MongoDB connection is working
client.admin.command("ping")

print("MongoDB connection established successfully.")


# -------------------------------------------
# Root Endpoint
# -------------------------------------------

@app.get("/")
def home():
    """
    Test endpoint to verify that the API is running.
    """

    return {"message": "Logistics API is running"}


# -------------------------------------------
# Logistics Filtering Endpoint
# -------------------------------------------

@app.get("/logistics")
def get_logistics(
    customerID: str | None = None,
    supplierID: str | None = None,
    ontime: str | None = None
):
    """
    Return logistics documents based on optional filters.

    Multiple filters are combined using AND logic.
    If no filters are provided, all documents are returned.
    """

    # Build the MongoDB query dynamically based on
    # the query parameters supplied by the API caller.
    query = {}

    if customerID:
        query["customerID"] = customerID

    if supplierID:
        query["supplierID"] = supplierID

    if ontime:
        query["ontime"] = ontime

    # Execute the query and convert the MongoDB cursor
    # into a Python list so FastAPI can return it as JSON.
    result = collection.find(query)

    return list(result)


# -------------------------------------------
# Logistics Summary / Aggregation Endpoint
# -------------------------------------------

@app.get("/logistics/summary")
def logistics_summary(customerID: str | None = None):
    """
    Return trip count and average transportation distance
    for each customer.

    An optional customerID can be supplied to generate the
    summary for a specific customer.
    """

    # Start with an empty aggregation pipeline so that
    # additional stages can be added dynamically.
    pipeline = []

    # If a customerID is provided, filter the documents
    # before performing the aggregation.
    if customerID:
        pipeline.append({
            "$match": {
                "customerID": customerID
            }
        })

    # Group the filtered documents by customer and calculate:
    # - total number of trips
    # - average transportation distance
    #
    # The project stage then converts MongoDB's generated
    # _id object back into regular response fields.
    pipeline.extend([
        {
            "$group": {
                "_id": {
                    "customerID": "$customerID",
                    "customerNameCode": "$customerNameCode"
                },
                "tripCount": {
                    "$sum": 1
                },
                "avgTransportationDistance": {
                    "$avg": "$TRANSPORTATION_DISTANCE_IN_KM"
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "customerID": "$_id.customerID",
                "customerNameCode": "$_id.customerNameCode",
                "tripCount": 1,
                "avgTransportationDistance": 1
            }
        }
    ])

    # Execute the aggregation pipeline and convert the
    # resulting MongoDB cursor into a Python list.
    result = collection.aggregate(pipeline)

    return list(result)
# Logistics API Documentation

## 1. Objective

The purpose of this API is to provide access to logistics data stored in the MongoDB `logistics` collection.

The API provides two main capabilities:

1. Filtering logistics documents using specific fields.
2. Aggregating logistics data to generate customer-level trip summaries.

The API was developed as part of the MongoDB + Kafka assignment, which requires an API to interact with the MongoDB collection and provide filtering and aggregation functionality.

---

## 2. Technologies Used

- Python
- FastAPI
- PyMongo
- MongoDB Atlas
- Uvicorn

---

## 3. MongoDB Configuration

The API connects to MongoDB using the `MONGODB_URI` environment variable.

### Database

```text
de_practice
```

### Collection

```text
logistics
```

The API uses PyMongo to communicate with the MongoDB collection.

---

# 4. API Endpoints

## 4.1 Root Endpoint

### Endpoint

```text
GET /
```

### Purpose

Used to verify that the API is running successfully.

### Example Request

```text
http://127.0.0.1:8000/
```

### Example Response

```json
{
    "message": "Logistics API is running"
}
```

---

# 4.2 Logistics Filtering Endpoint

### Endpoint

```text
GET /logistics
```

### Purpose

Returns logistics documents from MongoDB based on optional filters.

The endpoint supports the following query parameters:

| Parameter | Type | Optional | Description |
|---|---|---|---|
| `customerID` | string | Yes | Filters documents by customer ID |
| `supplierID` | string | Yes | Filters documents by supplier ID |
| `ontime` | string | Yes | Filters documents by the value stored in the `ontime` field |

If multiple parameters are provided, they are combined using MongoDB's AND matching behavior.

If no parameters are provided, the endpoint returns all documents.

### Example 1 — Filter by customer

```text
GET /logistics?customerID=DMREXCHEUX
```

### Example 2 — Filter by supplier

```text
GET /logistics?supplierID=SUSEXMADL5
```

### Example 3 — Filter by ontime value

```text
GET /logistics?ontime=G
```

### Example 4 — Multiple filters

```text
GET /logistics?customerID=DMREXCHEUX&supplierID=SUSEXMADL5&ontime=G
```

The API dynamically builds a MongoDB query based on the parameters provided.

For example:

```python
query = {
    "customerID": "DMREXCHEUX",
    "supplierID": "SUSEXMADL5"
}
```

The query is then executed using:

```python
collection.find(query)
```

---

# 4.3 Logistics Summary Endpoint

### Endpoint

```text
GET /logistics/summary
```

### Purpose

Provides an aggregated summary of logistics trips grouped by customer.

The endpoint calculates:

- Total number of trips
- Average transportation distance

The aggregation groups records using:

```text
customerID
customerNameCode
```

### Example Request

```text
GET /logistics/summary
```

### Example Response Structure

```json
[
    {
        "customerID": "DMREXCHEUX",
        "customerNameCode": "XXXXX",
        "tripCount": 25,
        "avgTransportationDistance": 73.4
    }
]
```

The actual values depend on the data stored in MongoDB.

---

## 4.4 Customer-Specific Summary

The summary endpoint also supports an optional `customerID` filter.

### Endpoint

```text
GET /logistics/summary?customerID=DMREXCHEUX
```

When a customer ID is provided, the API first filters the documents using a MongoDB `$match` stage.

The aggregation pipeline then performs the summary on the filtered documents.

The pipeline therefore becomes:

```text
$match
   ↓
$group
   ↓
$project
```

When no customer ID is provided:

```text
$group
   ↓
$project
```

---

# 5. Aggregation Logic

The summary endpoint uses a MongoDB aggregation pipeline.

### Stage 1 — `$match`

The `$match` stage is added only when a `customerID` is provided.

```python
{
    "$match": {
        "customerID": customerID
    }
}
```

This reduces the documents being processed by the subsequent aggregation stages.

### Stage 2 — `$group`

The `$group` stage groups documents by customer:

```python
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
}
```

`$sum: 1` is used to count the number of trips in each group.

`$avg` calculates the average value of:

```text
TRANSPORTATION_DISTANCE_IN_KM
```

### Stage 3 — `$project`

The `$project` stage removes the generated MongoDB `_id` field and exposes the customer fields directly in the API response.

```python
{
    "$project": {
        "_id": 0,
        "customerID": "$_id.customerID",
        "customerNameCode": "$_id.customerNameCode",
        "tripCount": 1,
        "avgTransportationDistance": 1
    }
}
```

---

# 6. API Assumptions

The following assumptions were made while developing the API.

### Filtering

1. `customerID`, `supplierID`, and `ontime` are treated as exact-match filters.
2. Multiple filters supplied in the same request are combined using AND logic.
3. If no filter is provided to `/logistics`, all available documents are returned.
4. The `ontime` parameter is treated as an exact value stored in the dataset. The API does not assign its own business meaning to values such as `G`.

### Aggregation

5. A customer is grouped using the combination of `customerID` and `customerNameCode`.
6. `tripCount` represents the number of logistics documents belonging to each group.
7. `avgTransportationDistance` is calculated using the MongoDB `$avg` aggregation operator.
8. The aggregation operates on the data currently available in the MongoDB `logistics` collection.

### API Scope

9. The API is intended to demonstrate interaction with the MongoDB collection rather than provide a complete production-ready service.
10. Authentication and authorization are not implemented in this assignment.
11. Pagination is not implemented for the filtering endpoint.
12. The API currently exposes only the filtering and aggregation functionality required for this assignment.

---

# 7. API Use Cases

### Use Case 1 — Retrieve customer-specific logistics records

A downstream application can request:

```text
/logistics?customerID=DMREXCHEUX
```

to retrieve logistics records associated with a particular customer.

### Use Case 2 — Retrieve supplier-specific records

A downstream application can request:

```text
/logistics?supplierID=SUSEXMADL5
```

to retrieve records associated with a supplier.

### Use Case 3 — Combine multiple filters

A downstream application can combine filters:

```text
/logistics?customerID=DMREXCHEUX&supplierID=SUSEXMADL5
```

to retrieve documents matching both conditions.

### Use Case 4 — Customer-level logistics summary

A downstream application can request:

```text
/logistics/summary
```

to obtain customer-level trip counts and average transportation distances.

### Use Case 5 — Customer-specific summary

A downstream application can request:

```text
/logistics/summary?customerID=DMREXCHEUX
```

to obtain an aggregated summary for a specific customer.

---

# 8. Running the API

Navigate to the API directory:

```bash
cd API
```

Install the required dependencies:

```bash
pip install -r requirements.txt
```

Start the FastAPI application using Uvicorn:

```bash
uvicorn api:app --reload
```

The API will be available at:

```text
http://127.0.0.1:8000
```

---

# 9. Interactive API Documentation

FastAPI automatically generates interactive API documentation.

Swagger UI:

```text
http://127.0.0.1:8000/docs
```

The `/docs` interface can be used to test the available endpoints and query parameters directly from the browser.

---

# 10. API Flow

The API is the serving layer of the overall data pipeline:

```text
CSV
 ↓
Kafka Producer
 ↓
Kafka Topic
 ↓
Kafka Consumer
 ↓
Data Validation
 ↓
MongoDB
 ↓
FastAPI
 ↓
Downstream Applications / Consumers
```

The API therefore provides a way for downstream consumers to access and aggregate the logistics data stored in MongoDB.

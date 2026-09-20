# MongoDB + PyMongo — Mini ETL Project

## 🎯 Project Goal

Build a small ETL pipeline that extracts data from MySQL,
transforms it using Python, loads it into MongoDB, and finally
extracts the MongoDB data into a JSON file.

---

## 🗺️ Pipeline

```text
MySQL
  ↓
Extract rows
  ↓
Python
  ↓
Convert rows → dictionaries
  ↓
Basic transformation / cleaning
  ↓
PyMongo
  ↓
MongoDB Collection
  ↓
Query / Aggregate
  ↓
Python
  ↓
JSON File
```


## 1. MySQL — Source

- Create a small MySQL table
- Insert realistic sample data
- Use Python to connect to MySQL
- Execute a SELECT query
- Fetch the records


## 2. Python — Transformation

- Convert MySQL rows into dictionaries
- Create a list of dictionaries
- Perform a few basic transformations/cleaning operations
- Prepare documents for MongoDB


## 3. MongoDB — Load

- Connect to MongoDB using PyMongo
- Create/select the target collection
- Insert the transformed documents
- Verify the inserted data


## 4. MongoDB — Processing

- Query the loaded documents
- Apply filters and projections
- Run a small aggregation pipeline
- Generate useful derived information


## 5. MongoDB → JSON — Extract

- Retrieve the processed documents using PyMongo
- Convert MongoDB documents into JSON-compatible data
- Handle MongoDB-specific fields such as `_id`


## 6. JSON — Output

- Write the final data to a `.json` file
- Validate the output
- Organize the project files cleanly


## 🏁 Final Outcome

```text
MySQL
  ↓
Python / MySQL Connector
  ↓
Python Transformation
  ↓
PyMongo
  ↓
MongoDB
  ↓
Query + Aggregation
  ↓
Python
  ↓
JSON
```

## Skills Demonstrated

- SQL
- MySQL
- Python
- Dictionaries / Lists
- PyMySQL / MySQL Connector
- PyMongo
- MongoDB
- Aggregation
- JSON
- ETL Pipeline
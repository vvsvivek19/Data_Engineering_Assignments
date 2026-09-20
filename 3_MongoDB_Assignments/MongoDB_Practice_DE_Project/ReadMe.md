# MongoDB + PyMongo — Mini ETL Project

## 🎯 Project Goal

Build a small ETL pipeline consisting of three Python applications:

1. A **data generator** that periodically generates realistic order data and loads it into MySQL.
2. An **ETL application** that extracts data from MySQL, transforms it into Python dictionaries, and loads it into MongoDB.
3. An **export application** that extracts data from MongoDB and dumps it into a JSON file.

The goal is to practice moving data across different systems using Python.

---

## 🗺️ Project Architecture

```text
                    DATA GENERATOR
                         │
                         ▼
                    MySQL Table
                         │
                         │ Extract
                         ▼
              Python ETL Application
                         │
                         │ Transform
                         │ Rows → Dictionaries
                         ▼
                  MongoDB Collection
                         │
                         │ Extract
                         ▼
             Python Export Application
                         │
                         │ Serialize
                         ▼
                     JSON File
````

---

## 1. MySQL — Source Database

Create a MySQL database and `orders` table containing realistic e-commerce order data.

### Data Generator

Create a Python application that:

* Generates realistic order data
* Randomly selects customers and products
* Generates quantity, payment method, order status, city, and order date
* Inserts generated records into the MySQL `orders` table
* Runs periodically to continuously add new records

```text
Python Data Generator
        ↓
MySQL
        ↓
orders table
```

---

## 2. MySQL → MongoDB — ETL Application

Create a separate Python application that:

* Connects to MySQL
* Executes a `SELECT` query on the `orders` table
* Extracts the records
* Converts MySQL rows into Python dictionaries
* Performs basic transformation / cleaning if required
* Connects to MongoDB using PyMongo
* Inserts the transformed records into a MongoDB collection

```text
MySQL
   ↓
Extract
   ↓
Python
   ↓
Transform
   ↓
PyMongo
   ↓
MongoDB
```

---

## 3. MongoDB — Processing

Use the MongoDB collection created by the ETL application to:

* Query documents
* Apply filters and projections
* Perform basic transformations
* Run aggregation pipelines
* Generate useful derived information

---

## 4. MongoDB → JSON — Export Application

Create another Python application that:

* Connects to MongoDB using PyMongo
* Extracts documents from the collection
* Converts MongoDB documents into JSON-compatible data
* Handles MongoDB-specific fields such as `_id`
* Writes the extracted data to a JSON file

```text
MongoDB
   ↓
PyMongo
   ↓
Python
   ↓
JSON
```

---

## 5. Final Pipeline

```text
┌──────────────────────┐
│  Python Data         │
│  Generator           │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│       MySQL          │
│     orders table     │
└──────────┬───────────┘
           │
           │ Extract
           ▼
┌──────────────────────┐
│  Python ETL          │
│  Application         │
└──────────┬───────────┘
           │
           │ Transform
           ▼
┌──────────────────────┐
│      MongoDB         │
│     Collection       │
└──────────┬───────────┘
           │
           │ Extract
           ▼
┌──────────────────────┐
│  Python Export       │
│  Application         │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│      JSON File       │
└──────────────────────┘
```

---

## 🏁 Final Outcome

The project demonstrates a complete small-scale data pipeline:

**Generate → Extract → Transform → Load → Process → Export**

---

## Skills Demonstrated

* SQL
* MySQL
* Python
* Python Dictionaries / Lists
* MySQL Connector
* PyMongo
* MongoDB
* MongoDB Aggregation
* JSON
* ETL Pipeline Design

```

I also deliberately removed **“PyMySQL / MySQL Connector”** and kept **MySQL Connector**, since you're actually using `mysql.connector` in your generator. Keeping the README aligned with the implementation will make the project cleaner.
```

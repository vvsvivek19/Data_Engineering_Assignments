# MongoDB Practice DE Project

A small end-to-end Data Engineering project demonstrating how Python can be used to generate, extract, transform, and move e-commerce order data across MySQL, MongoDB, and JSON files.

---

## Project Goal

The project implements an incremental data pipeline using three separate Python applications:

1. **Data Generator** — Generates realistic e-commerce orders and inserts them into MySQL.
2. **ETL Application** — Extracts new orders from MySQL, transforms the relational data into MongoDB documents, and loads them into MongoDB.
3. **Export Application** — Extracts new MongoDB documents and exports them as partitioned JSON files.

The project uses checkpoint-based incremental processing to avoid reprocessing previously handled records.

---

## Architecture

```text
Python Data Generator
        ↓
MySQL: ecommerce_source.orders
        ↓
Python ETL Application
        ↓
MongoDB: de_practice.orders
        ↓
Python Export Application
        ↓
Partitioned JSON Files
````

---

## Project Structure

```text
MongoDB_Practice_DE_Project/
│
├── Data_generator.py
├── Data_ETL_to_MongoDB.py
├── Data_Export_MongoDB_to_JSON.py
│
├── checkpoint.json
├── mongodb_checkpoint.json
│
├── exports/
│   ├── orders_1_50.json
│   ├── orders_51_60.json
│   └── orders_61_80.json
│
└── README.md
```

---

# 1. Data Generator

### File

```text
Data_generator.py
```

The Data Generator creates realistic e-commerce order records and continuously inserts them into MySQL.

### Source Database

```text
Database: ecommerce_source
Table: orders
```

### Generated Data

Each order contains:

* Order ID
* Customer ID
* Customer Name
* Product ID
* Product Name
* Category
* Quantity
* Unit Price
* Order Date
* Payment Method
* Order Status
* City

The generator uses randomly selected customers, products, payment methods, statuses, cities, quantities, and historical order dates.

### Incremental Order IDs

Before generating new orders, the application checks the current maximum `order_id` in MySQL and continues from the next ID.

For example:

```text
Existing orders → 1–60
New generation → starts from 61
```

---

# 2. MySQL → MongoDB ETL

### File

```text
Data_ETL_to_MongoDB.py
```

This application incrementally extracts new orders from MySQL, transforms the relational records into nested MongoDB documents, and loads them into MongoDB.

---

## Checkpoint-Based Extraction

The ETL maintains the last successfully processed MySQL `order_id` in:

```text
checkpoint.json
```

Example:

```json
{
    "last_order_id": 60
}
```

The next ETL run fetches only:

```sql
WHERE order_id > 60
```

This prevents previously processed records from being fetched again.

---

## Data Transformation

The flat MySQL structure is transformed into a nested MongoDB document.

Example:

```json
{
    "_id": 61,
    "order_id": 61,

    "customer": {
        "customer_id": 1005,
        "customer_name": "Example Customer"
    },

    "product": {
        "product_id": 101,
        "product_name": "Laptop",
        "category": "Electronics",
        "unit_price": 65000.0
    },

    "order_details": {
        "quantity": 2,
        "order_date": "2026-05-14",
        "payment_method": "UPI",
        "order_status": "Delivered"
    },

    "shipping": {
        "city": "Mumbai"
    }
}
```

### Document Design

Related fields are grouped into embedded documents:

```text
customer
product
order_details
shipping
```

The MySQL `order_id` is also used as the MongoDB `_id`.

This provides a stable unique identifier for each order and helps prevent duplicate documents during reprocessing.

---

## Data Type Transformation

Some MySQL data types require conversion before being inserted into MongoDB.

### DECIMAL

MySQL `DECIMAL` values are returned to Python as `Decimal` objects.

They are converted to:

```python
float(row[7])
```

### DATE

MySQL `DATE` values are returned as Python `date` objects.

They are converted to ISO-format strings:

```python
row[8].isoformat()
```

---

## Checkpoint Update

The checkpoint is updated only after successful MongoDB insertion.

Example:

```text
MySQL
  ↓
Orders 61–80
  ↓
Transform
  ↓
MongoDB
  ↓
All 20 successfully inserted
  ↓
checkpoint.json → 80
```

If an insertion fails, the checkpoint remains at the last successfully inserted order.

---

# 3. MongoDB → JSON Export

### File

```text
Data_Export_MongoDB_to_JSON.py
```

This application incrementally extracts new documents from MongoDB and exports them as separate JSON files.

---

## MongoDB Checkpoint

A separate checkpoint is maintained for the MongoDB → JSON process:

```text
mongodb_checkpoint.json
```

Example:

```json
{
    "last_order_id_fetched": 80
}
```

The application fetches only documents where:

```python
{"_id": {"$gt": last_order_id_fetched}}
```

This allows the MongoDB export process to operate independently from the MySQL → MongoDB ETL.

---

## Partitioned JSON Export

Instead of continuously appending to a single large JSON file, each export run creates a new JSON file containing that batch of documents.

For example:

```text
exports/
├── orders_1_50.json
├── orders_51_60.json
└── orders_61_80.json
```

The filename represents the range of order IDs contained in that file.

For example:

```text
orders_61_80.json
```

contains orders:

```text
61 → 80
```

This avoids repeatedly loading and rewriting the entire historical JSON dataset.

---

## Export Checkpoint

The MongoDB checkpoint is updated only after the JSON export succeeds.

Example:

```text
MongoDB checkpoint → 60
        ↓
Fetch orders 61–80
        ↓
Create orders_61_80.json
        ↓
JSON export successful
        ↓
mongodb_checkpoint.json → 80
```

If the JSON export fails, the checkpoint is not advanced, allowing the same batch to be retried during the next run.

---

# 4. Incremental Pipeline

The complete pipeline works as follows:

```text
┌─────────────────────────┐
│   Python Data Generator │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ MySQL                   │
│ ecommerce_source.orders │
└────────────┬────────────┘
             │
             │ checkpoint.json
             ▼
┌─────────────────────────┐
│ Python ETL Application  │
│ Extract → Transform     │
│ → Load                  │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ MongoDB                 │
│ de_practice.orders      │
└────────────┬────────────┘
             │
             │ mongodb_checkpoint.json
             ▼
┌─────────────────────────┐
│ Python Export           │
│ Application             │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐
│ exports/                │
│ orders_*.json           │
└─────────────────────────┘
```

---

# 5. Technologies Used

* Python
* MySQL
* MySQL Connector/Python
* MongoDB
* PyMongo
* JSON
* Python dictionaries and lists
* File handling
* Environment variables
* Checkpoint-based incremental processing

---

# 6. Data Engineering Concepts Demonstrated

* Incremental data extraction
* Checkpoint-based processing
* ETL pipeline design
* Relational → document data transformation
* Nested document modeling
* MongoDB document storage
* Batch processing
* Idempotent processing using stable document IDs
* Error handling
* JSON serialization
* Partitioned file exports
* Separate checkpoints for independent pipeline stages

---

# 7. Pipeline Execution

The applications are executed in the following order:

### Step 1 — Generate source data

```bash
python3 Data_generator.py
```

Generates orders and inserts them into MySQL.

### Step 2 — Run MySQL → MongoDB ETL

```bash
python3 Data_ETL_to_MongoDB.py
```

Fetches new MySQL orders, transforms them, and loads them into MongoDB.

### Step 3 — Export MongoDB → JSON

```bash
python3 Data_Export_MongoDB_to_JSON.py
```

Fetches new MongoDB documents and creates a new JSON export file.

---

## Final Result

The project demonstrates an incremental pipeline:

```text
MySQL
  ↓
Python ETL
  ↓
MongoDB
  ↓
Python Export
  ↓
Partitioned JSON
```

with independent checkpoints controlling each stage of the pipeline.


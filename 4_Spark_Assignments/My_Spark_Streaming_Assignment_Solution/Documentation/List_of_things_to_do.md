# Project Steps

## 1. Creating a Mock Data Producer

- Create Kafka topic: `ads_data`
- Create Avro schema
- Register Avro schema in Schema Registry
- Set up Kafka configuration in Python
- Set up Schema Registry configuration in Python
- Create mock data producer
- Verify messages are arriving in Kafka via Confluent UI

---

## 2. Creating a Spark Structured Streaming Application

### Aim

The Spark application will continuously consume messages from Kafka, process the incoming advertisement data, and perform window-based aggregations.

The aggregated results will then be written to a MySQL table. If a record for the corresponding key already exists, its aggregated values will be updated; otherwise, a new record will be inserted.

### Steps

#### Spark & Kafka Setup

- Connect to the Managed Apache Spark Cluster in GCP and test connectivity
- Configure the Spark application to read data from Kafka using Structured Streaming
- Verify that Spark is successfully receiving messages from the `ads_data` Kafka topic

#### Data Deserialization & Preparation

- Deserialize the Avro messages received from Kafka
- Convert the Kafka data into a structured Spark DataFrame
- Convert the `timestamp` field into a Spark timestamp
- Verify the incoming DataFrame schema and sample records

#### Window-Based Aggregation

- Define the event-time window
- Configure a **1-minute window with a 30-second slide**
- Group the data by `ad_id` and window
- Calculate total clicks for each `ad_id`
- Calculate total views for each `ad_id`
- Calculate average cost per view for each `ad_id`
- Verify the aggregated results in the Spark DataFrame

#### MySQL Setup

- Create the MySQL database
- Create the aggregation table
- Define the appropriate primary/unique key for identifying an aggregated record
- Test connectivity from the Spark environment to MySQL

#### Writing Aggregated Data to MySQL

- Configure Spark to write each micro-batch to MySQL
- Implement `INSERT` logic for new records
- Implement `UPDATE`/`UPSERT` logic for existing records
- Verify that aggregated values are correctly updated in MySQL
- Verify that new `ad_id`/window records are correctly inserted

#### End-to-End Testing

- Start the mock Kafka producer
- Start the Spark Structured Streaming application
- Verify Kafka → Spark data flow
- Verify window aggregation results
- Verify Spark → MySQL data flow
- Verify insert/update behavior across multiple micro-batches
- Verify that the complete pipeline works continuously

---

## 3. Documentation & Cleanup

- Add project architecture diagram
- Clean up and organize Python code
- Add comments/documentation where necessary
- Document the Kafka → Spark → MySQL data flow
- Add sample Kafka input
- Add sample aggregated output
- Document how to run the project
- Add project requirements/dependencies
- Perform final end-to-end test
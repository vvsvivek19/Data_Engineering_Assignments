### 1. Project Overview

This mini pipeline project contains three main components:

- A MySQL data generator
- A Kafka producer
- A Kafka consumer

The main aim is to take data from a MySQL table, pass that data through a Kafka producer, which converts the data into Avro-serialized messages and sends them to a Kafka topic. We then create a consumer group with 5 consumer instances. Kafka distributes the topic partitions among these consumers, and each consumer deserializes the Avro messages and writes the consumed records to its own JSON file.

### Steps to Run the System

#### 1. Start the MySQL Data Generator

Before starting, make sure the database and the required table structure have been created. Also, make sure the correct MySQL credentials are configured in the script.

Run the data generator script. It will generate random product records and continuously insert them into the MySQL table at a frequency of approximately 1 record per second.

It is recommended to let the generator run for at least 2 minutes so that there are 100+ records available to work with.

#### 2. Start the Kafka Producer

Before running the producer, make sure:

- A Kafka cluster has been created.
- The required Kafka topic has been created.
- A Schema Registry is available.
- An appropriate Avro schema/data contract has been registered for the topic.

Once everything is ready, run the Kafka producer.

The producer performs the following steps:

1. Reads the last processed timestamp from the checkpoint file.
2. Queries MySQL for records whose `last_updated` timestamp is greater than the checkpoint.
3. Converts each MySQL row into a Python dictionary.
4. Converts the Python dictionary into an Avro-serialized message using the registered Avro schema.
5. Sends each record to the Kafka topic.
6. Waits for Kafka to confirm message delivery.
7. Updates the checkpoint with the latest successfully processed timestamp.

The checkpoint allows the producer to perform **incremental extraction** on subsequent runs instead of processing the same records again.

#### 3. Start the Kafka Consumer

Once the producer has started sending messages to the Kafka topic, the Kafka consumer can be started.

The consumer creates a consumer group containing 5 consumer instances. All consumer instances use the same consumer group ID, allowing Kafka to distribute the topic partitions among them.

Each consumer performs the following steps:

1. Connects to the Kafka cluster.
2. Joins the consumer group.
3. Receives one or more partitions from Kafka.
4. Polls messages from its assigned partitions.
5. Deserializes the Avro message back into a Python dictionary.
6. Performs the required data transformations.
7. Writes the resulting record to its own JSON file.

For example, the output directory will contain files such as:

```text
Consumer_files/
├── consumer_number_1.json
├── consumer_number_2.json
├── consumer_number_3.json
├── consumer_number_4.json
└── consumer_number_5.json
```
#### 4. Complete Pipeline Cycle

The above three steps complete one cycle of the pipeline:

```text
MySQL Data Generator
        ↓
   MySQL Table
        ↓
 Kafka Producer
        ↓
Avro Serialization
        ↓
   Kafka Topic
        ↓
 Consumer Group
   ┌────┼────┬────┬────┐
   ↓    ↓    ↓    ↓    ↓
  C1    C2   C3   C4   C5
   ↓    ↓    ↓    ↓    ↓
 JSON  JSON JSON JSON JSON
 ```
 

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import os
from pymongo import MongoClient
from datetime import datetime
import time
from pymongo.errors import DuplicateKeyError

# ---------------------------------------------------------
# 0. Intial configurations
# ---------------------------------------------------------

TOPIC_NAME = "Fedex_logistics"
CONSUMER_GROUP = "fedex-consumer"

# ---------------------------------------------------------
# 1. Creating connection with MongoDB
# ---------------------------------------------------------

# Read the MongoDB Atlas connection string from the environment variable.
mongodb_uri = os.getenv("MONGODB_URI")


# Create a MongoDB client using the connection string.
client = MongoClient(mongodb_uri)

# Select the target database and collection.
db = client["de_practice"]
collection = db["fedex_logistics"]

print("MongoDB connection established successfully.")

# ---------------------------------------------------------
# 2. Setting up kafka config
# ---------------------------------------------------------

kafka_config = {
    "bootstrap.servers": os.getenv("KAFKA_URL"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.getenv("KAFKA_USERNAME"),
    "sasl.password": os.getenv("KAFKA_PASSWORD")
}

# ---------------------------------------------------------
# 3. Setting up connection to Schema Registry
# ---------------------------------------------------------

schema_registry_config = {
    # URL of the Confluent Cloud Schema Registry
        "url": os.getenv("SCHEMA_REGISTRY_URL"),
    
    # API key and secret used to authenticate with Schema Registry
    "basic.auth.user.info": (
        f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:"
        f"{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
    )
}

# creating schema registry client so that we can work with it
schema_registry_client = SchemaRegistryClient(schema_registry_config)

# -----------------------------------------------------
# 4. Define deserializers and create a Consumer config
# -----------------------------------------------------

key_deserializer = StringDeserializer('utf-8')
value_deserializer = AvroDeserializer(schema_registry_client)

consumer_config = {
        "bootstrap.servers": kafka_config["bootstrap.servers"],
        "security.protocol": kafka_config["security.protocol"],
        "sasl.mechanism": kafka_config["sasl.mechanism"],
        "sasl.username": kafka_config["sasl.username"],
        "sasl.password": kafka_config["sasl.password"],
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "key.deserializer": key_deserializer,
        "value.deserializer":value_deserializer,
    }

# ---------------------------------------------------------
# 5. Consuming Messages & Validation & insertion
# ---------------------------------------------------------

consumer = DeserializingConsumer(consumer_config)
consumer.subscribe([TOPIC_NAME])

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        elif msg.error():
            print(f"Consumer Error: {msg.error()}")
        else:
            record = msg.value()
            timestamp = datetime.fromisoformat(
                record["timestamp"].replace("Z", "+00:00")
            )
            mongo_document = {
                "shipment_id": record["shipment_id"],
                "origin": record["origin"],
                "destination": record["destination"],
                "status": record["status"],
                "timestamp": timestamp
            }
            try:
                insert_result = collection.insert_one(mongo_document)
                print(
                f"Shipment {record['shipment_id']} "
                f"successfully inserted into MongoDB "
                f"with _id: {insert_result.inserted_id}")
            except DuplicateKeyError:
                print(
                    f"Shipment {record['shipment_id']} "
                    f"already exists in MongoDB. Skipping."
                )
            time.sleep(1)
except KeyboardInterrupt as e:
    print("Consumer Stopped by user....")
    

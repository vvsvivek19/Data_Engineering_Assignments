from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

import os
from datetime import datetime, timedelta
import random
import time

from pymongo import MongoClient


# =========================================================
# 0. INITIAL CONFIGURATION
# =========================================================

# Name of the Kafka topic where shipment records will be published.
TOPIC_NAME = "Fedex_logistics"

# Read the MongoDB Atlas connection string from the environment variable.
mongodb_uri = os.getenv("MONGODB_URI")

# Create a MongoDB client using the Atlas connection string.
client = MongoClient(mongodb_uri)

# Select the database and collection where shipment IDs
# will be checked for uniqueness.
db = client["de_practice"]
collection = db["fedex_logistics"]

print("MongoDB connection established successfully.")


# =========================================================
# 1. KAFKA CONFIGURATION
# =========================================================

# Kafka connection and authentication configuration.
#
# These values are read from environment variables so that
# sensitive credentials are not hard-coded in the Python script.
kafka_config = {
    "bootstrap.servers": os.getenv("KAFKA_URL"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.getenv("KAFKA_USERNAME"),
    "sasl.password": os.getenv("KAFKA_PASSWORD")
}


# =========================================================
# 2. SCHEMA REGISTRY CONFIGURATION
# =========================================================

# Configuration required to connect to Confluent Cloud
# Schema Registry.
#
# The API key and secret are used to authenticate with
# Schema Registry.
schema_registry_config = {
    "url": os.getenv("SCHEMA_REGISTRY_URL"),

    "basic.auth.user.info": (
        f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:"
        f"{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
    )
}


# =========================================================
# 3. FETCH AVRO SCHEMA FROM SCHEMA REGISTRY
# =========================================================

# Create a Schema Registry client.
schema_registry_client = SchemaRegistryClient(
    schema_registry_config
)

# Schema Registry stores schemas under subjects.
#
# For a topic using the default TopicNameStrategy,
# the value schema subject follows:
#
# <topic-name>-value
subject_name = f"{TOPIC_NAME}-value"

# Fetch the latest version of the schema registered
# for this Kafka topic's message value.
latest_schema = schema_registry_client.get_latest_version(
    subject_name
)

# Extract the actual Avro schema definition as a string.
schema_str = latest_schema.schema.schema_str


# =========================================================
# 4. CREATE SERIALIZERS AND KAFKA PRODUCER
# =========================================================

# Serialize Kafka message keys as UTF-8 strings.
key_serializer = StringSerializer("utf-8")

# Serialize Python dictionaries into Avro format using
# the schema retrieved from Schema Registry.
value_serializer = AvroSerializer(
    schema_registry_client,
    schema_str
)

# Create a SerializingProducer.
#
# The producer automatically:
# 1. Serializes the message key using StringSerializer.
# 2. Serializes the message value using AvroSerializer.
# 3. Publishes the serialized message to Kafka.
producer = SerializingProducer(
    {
        "bootstrap.servers": kafka_config["bootstrap.servers"],
        "security.protocol": kafka_config["security.protocol"],
        "sasl.mechanism": kafka_config["sasl.mechanism"],
        "sasl.username": kafka_config["sasl.username"],
        "sasl.password": kafka_config["sasl.password"],
        "key.serializer": key_serializer,
        "value.serializer": value_serializer
    }
)


# =========================================================
# 5. MOCK DATA CONFIGURATION
# =========================================================

# Possible shipment origin locations.
origin_cities = [
    "New York, NY",
    "Chicago, IL",
    "Los Angeles, CA",
    "Houston, TX",
    "Phoenix, AZ",
    "Philadelphia, PA",
    "San Antonio, TX",
    "San Diego, CA",
    "Dallas, TX",
    "Austin, TX",
    "Seattle, WA",
    "Denver, CO",
    "Boston, MA",
    "Atlanta, GA",
    "Miami, FL",
    "Portland, OR",
    "Las Vegas, NV",
    "Detroit, MI",
    "Minneapolis, MN",
    "Charlotte, NC"
]

# Possible shipment destination locations.
destination_cities = [
    "Boston, MA",
    "Detroit, MI",
    "San Francisco, CA",
    "Dallas, TX",
    "Denver, CO",
    "Atlanta, GA",
    "Austin, TX",
    "Seattle, WA",
    "Houston, TX",
    "Phoenix, AZ",
    "Chicago, IL",
    "Las Vegas, NV",
    "Miami, FL",
    "Charlotte, NC",
    "Los Angeles, CA",
    "Portland, OR",
    "Philadelphia, PA",
    "Minneapolis, MN",
    "San Diego, CA",
    "New York, NY"
]

# Possible shipment statuses.
status_list = [
    "pending",
    "confirmed",
    "assigned",
    "in-transit",
    "delivered",
    "delayed",
    "cancelled",
    "returned"
]


# =========================================================
# 6. HELPER FUNCTIONS
# =========================================================

def generate_random_timestamp():
    """
    Generate a random historical UTC timestamp.

    A random timestamp is generated between
    January 1, 2022 and December 31, 2025.
    """

    start_date = datetime(2022, 1, 1)
    end_date = datetime(2025, 12, 31)

    # Calculate the total time difference between
    # the start and end dates.
    time_difference = end_date - start_date

    # Generate a random number of seconds within
    # the available date range.
    random_seconds = random.randint(0,int(time_difference.total_seconds()))

    # Add the random number of seconds to the start date.
    random_datetime = start_date + timedelta(seconds=random_seconds)

    # Convert the datetime object into an ISO-8601 UTC string.
    return random_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_unique_shipment_id(collection):
    """
    Generate a unique shipment ID in the format SH123456.

    The generated ID is checked against MongoDB before
    returning it. This prevents the producer from intentionally
    generating an ID that already exists in the collection.
    """

    while True:

        # Generate a random six-digit shipment ID.
        shipment_id = f"SH{random.randint(100000, 999999)}"

        # Check whether this shipment ID already exists
        # in the MongoDB collection.
        existing_shipment = collection.find_one({
            "shipment_id": shipment_id
        })

        # Return the ID only if it does not already exist.
        if existing_shipment is None:
            return shipment_id


def deliver_report(err, msg):
    """
    Kafka delivery callback.

    This function is called after Kafka attempts to deliver
    a message and reports whether the delivery succeeded
    or failed.
    """

    if err is not None:

        # Delivery failed.
        print(f"Message Delivery Failed: {err}")

    else:

        # Delivery succeeded.
        print(
            f"Message Delivered - "
            f"Topic: {msg.topic()} | "
            f"Key: {msg.key()} | "
            f"Partition: {msg.partition()}"
        )


# =========================================================
# 7. GENERATE AND PRODUCE MOCK SHIPMENT DATA
# =========================================================

print(
    "Producing Mock Records and pushing them to Kafka topic..."
)

try:

    # Continuously generate and publish shipment records.
    #
    # The producer is intentionally designed to run continuously
    # and produce approximately one message every second.
    while True:

        # Create an empty dictionary for the shipment record.
        record = {}

        # Generate a shipment ID that does not currently
        # exist in MongoDB.
        shipment_id = generate_unique_shipment_id(collection)

        # Randomly select shipment attributes.
        origin = random.choice(origin_cities)
        destination = random.choice(destination_cities)
        status = random.choice(status_list)
        timestamp = generate_random_timestamp()

        # Build the shipment record.
        record["shipment_id"] = shipment_id
        record["origin"] = origin
        record["destination"] = destination
        record["status"] = status
        record["timestamp"] = timestamp

        # Asynchronously publish the record to Kafka.
        #
        # The SerializingProducer automatically serializes:
        # - shipment_id -> String
        # - record -> Avro
        producer.produce(
            topic=TOPIC_NAME,
            key=record["shipment_id"],
            value=record,
            on_delivery=deliver_report
        )

        # Process any delivery callbacks that are ready.
        #
        # poll(0) does not wait for new events.
        producer.poll(0)

        # Wait approximately one second before generating
        # and publishing the next shipment record.
        time.sleep(0.26)


except KeyboardInterrupt:

    # Gracefully stop the continuous producer when
    # Ctrl+C is pressed.
    print("Producer stopped by user.")


finally:

    # Wait for any messages still queued in the producer
    # to be delivered before shutting down.
    producer.flush()
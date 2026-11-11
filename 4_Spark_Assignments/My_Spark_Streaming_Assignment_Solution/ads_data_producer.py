# ---------------------------------------------------------
# Importing required libraries
# ---------------------------------------------------------

import time
import os

from datetime import datetime, timezone
from random import randint, uniform

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

# ---------------------------------------------------------
# 1. Intial configurations
# ---------------------------------------------------------
TOPIC_NAME = "ads_data"

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
    "basic.auth.user.info": f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
}

# ---------------------------------------------------------
# 4. Fetching latest schema from Schema Registry
# ---------------------------------------------------------
schema_registry_client = SchemaRegistryClient(schema_registry_config)
subject_name = f"{TOPIC_NAME}-value"
schema = schema_registry_client.get_latest_version(subject_name).schema.schema_str

# ---------------------------------------------------------
# 5. Defining Serializers and creating SerializingProducer
# ---------------------------------------------------------
key_serializer = StringSerializer('utf_8')
value_serializer = AvroSerializer(schema_registry_client, schema)
producer = SerializingProducer({
    **kafka_config,
    "key.serializer": key_serializer,
    "value.serializer": value_serializer
})

# ---------------------------------------------------------
# 6. Delivery callback function to handle delivery reports
# ---------------------------------------------------------
def delivery_report(err,msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

#---------------------------------------------------------
# 7. Generating mock advertisement data and sending to Kafka topic
#---------------------------------------------------------
try:
    while True:
        record = {}
        ad_id = f"{randint(1000, 9999)}"
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        views = randint(0, 100)
        clicks = randint(0, views)  # clicks should not exceed views
        cost = uniform(0,100)
        record["ad_id"] = ad_id
        record["timestamp"] = timestamp
        record["clicks"] = clicks
        record["views"] = views
        record["cost"] = cost
        print(f"Producing record: {record}")
        producer.produce(topic=TOPIC_NAME, key=ad_id, value=record, on_delivery=delivery_report)

        time.sleep(1)  # Sleep for 1 second before producing the next record

except KeyboardInterrupt:
    print("Production interrupted by user.")

finally:
    producer.flush()  # Ensure all messages are sent before exiting
    print("Producer closed.")
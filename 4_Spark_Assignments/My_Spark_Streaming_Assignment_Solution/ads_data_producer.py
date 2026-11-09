from confluent_kafka import Producer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import os
import pandas as pd
from pprint import pprint

# ---------------------------------------------------------
# 0. Intial configurations
# ---------------------------------------------------------
TOPIC_NAME = "ads_data"

# ---------------------------------------------------------
# 1. Setting up kafka config
# ---------------------------------------------------------

kafka_config = {
    "bootstrap.servers": os.getenv("KAFKA_URL"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": os.getenv("KAFKA_USERNAME"),
    "sasl.password": os.getenv("KAFKA_PASSWORD")
}

# ---------------------------------------------------------
# 2. Setting up connection to Schema Registry
# ---------------------------------------------------------

schema_registry_config = {
    # URL of the Confluent Cloud Schema Registry
    "url": os.getenv("SCHEMA_REGISTRY_URL"),
    # API key and secret used to authenticate with Schema Registry
    "basic.auth.user.info": f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
}


from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import os
import pandas as pd

# ---------------------------------------------------------
# 0. Intial configurations
# ---------------------------------------------------------

TOPIC_NAME = "logistics_data"
csv_file_path = "/Users/vvsvivek/Library/CloudStorage/OneDrive-Personal/Skills & Career/3. Data Engineering/Data_Engineering_Assignments/3_MongoDB_Assignments/Assignment_1_Solution/delivery_trip_truck_data.csv"

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
    "basic.auth.user.info": (
        f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:"
        f"{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
    )
}

# ---------------------------------------------------------
# 3. Fetching latest schema from Schema Registry
# Why? - To get the schema that defines the structure of the Kafka message.
# ---------------------------------------------------------

# creating schema registry client so that we can work with it
schema_registry_client = SchemaRegistryClient(schema_registry_config)

# each schema in schema registry is has a subject name
subject_name = f"{TOPIC_NAME}-value"

# Fetching the latest schema
latest_schema = schema_registry_client.get_latest_version(subject_name)
schema_str = latest_schema.schema.schema_str

# ---------------------------------------------------------
# 4. Define serializers and create a SeriliazingProducer 
# Why? - To automatically convert Python data into the format Kafka expects.
# ---------------------------------------------------------

key_serializer = StringSerializer('utf-8')
value_serializer = AvroSerializer(schema_registry_client,schema_str)

# creating producer client
producer = SerializingProducer(
    {
        "bootstrap.servers": kafka_config["bootstrap.servers"],
        "security.protocol": kafka_config["security.protocol"],
        "sasl.mechanism": kafka_config["sasl.mechanism"],
        "sasl.username": kafka_config["sasl.username"],
        "sasl.password": kafka_config["sasl.password"],
        "key.serializer": key_serializer,
        "value.serializer":value_serializer
    }
)

# ---------------------------------------------------------------
# 5. Reading data from logistics csv file using pandas  
# ---------------------------------------------------------------

df = pd.read_csv(csv_file_path)

print(df.head())
print(df.columns)
print(df.dtypes)
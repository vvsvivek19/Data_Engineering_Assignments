from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import os
from pprint import pprint
from datetime import *
from pymongo import MongoClient
import time

# ---------------------------------------------------------
# 0. Intial configurations
# ---------------------------------------------------------

TOPIC_NAME = "logistics_data"
CONSUMER_GROUP = "logistics-consumer"

# ---------------------------------------------------------
# 1. Creating connection with MongoDB
# ---------------------------------------------------------

# Read the MongoDB Atlas connection string from the environment variable.
mongodb_uri = os.getenv("MONGODB_URI")


# Create a MongoDB client using the connection string.
client = MongoClient(mongodb_uri)

# Select the target database and collection.
db = client["de_practice"]
collection = db["logistics"]

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
        "enable.auto.commit": False
    }

# ---------------------------------------------------------
# 5. Consuming Messages & Validation & insertion
# ---------------------------------------------------------

consumer = DeserializingConsumer(consumer_config)
consumer.subscribe([TOPIC_NAME])

def validate_and_clean(record):
    cleaned_record = record.copy()
    warnings = []
    errors = []
    mandatory_fields = {
        'BookingID',
        'vehicle_no',
        'Origin_Location',
        'Destination_Location',
        'trip_start_date',
        'customerID',
        'supplierID'
    }
    
    # field validations / transformations

    for field,value in record.items():
        
        # Common null/empty validation
        
        if value == "":
            cleaned_record[field] = None
            value = None
            warnings.append(f"{field}: empty string converted to None")

        if value is None:
            if field in mandatory_fields:
                errors.append(f"{field}: mandatory field is missing")
            else:
                warnings.append(f"{field}: optional field is missing")
            continue

        # handling data types for numeric fields

        if field == "Curr_lat":

            if isinstance(value, (int, float)):
                if value >= -90 and value <= 90:
                    cleaned_record[field] = float(value)
                else:
                    errors.append(f"{field}: invalid coordinate value")
            else:
                try:
                    value = float(value)
                    if value >= -90 and value <= 90:
                        cleaned_record[field] = float(value)
                        warnings.append(f"{field}: value converted to float")
                    else:
                        errors.append(f"{field}: invalid coordinate value")
                except (ValueError, TypeError):
                    errors.append(f"{field}: invalid numeric value")

        if field == "Curr_lon":

            if isinstance(value, (int, float)):
                if value >= -180 and value <= 180:
                    cleaned_record[field] = float(value)
                else:
                    errors.append(f"{field}: invalid coordinate value")
            else:
                try:
                    value = float(value)
                    if value >= -180 and value <= 180:
                        cleaned_record[field] = float(value)
                        warnings.append(f"{field}: value converted to float")
                    else:
                        errors.append(f"{field}: invalid coordinate value")
                except (ValueError, TypeError):
                    errors.append(f"{field}: invalid numeric value")

        if field == "TRANSPORTATION_DISTANCE_IN_KM":
        
            if isinstance(value, (int, float)):
                    if value >= 0:
                        cleaned_record[field] = float(value)
                    else:
                       errors.append(f"{field}: distance cannot be negative")     
            else:
                try:
                    value = float(value)
                    if value >= 0:
                        cleaned_record[field] = value
                        warnings.append(f"{field}: value converted to float")
                    else:
                        errors.append(f"{field}: distance cannot be negative")       
                except (ValueError, TypeError):
                    errors.append(f"{field}: invalid numeric value")        
    
        if field == "Minimum_kms_to_be_covered_in_a_day":
        
            if isinstance(value, (int, float)):
                    if value >= 0:
                        cleaned_record[field] = float(value)
                    else:
                        errors.append(f"{field}: distance cannot be negative")     
            else:
                try:
                    value = float(value)
                    if value >= 0:
                        cleaned_record[field] = value
                        warnings.append(f"{field}: value converted to float")
                    else:
                        errors.append(f"{field}: distance cannot be negative")   
                except (ValueError, TypeError):
                    errors.append(f"{field}: invalid numeric value")  

        # Date/time format validation
        if field == "BookingID_Date":
            try:
                cleaned_record[field] = datetime.strptime(value, "%m/%d/%y")
            except (ValueError, TypeError):
                errors.append(f"{field}: invalid date/time format")

        if field == "actual_eta":
            try:
                cleaned_record[field] = datetime.strptime(value, "%m/%d/%y %H:%M")
            except (ValueError, TypeError):
                errors.append(f"{field}: invalid date/time format")

        if field == "trip_start_date":
            try:
                cleaned_record[field] = datetime.strptime(value, "%m/%d/%y %H:%M")
            except (ValueError, TypeError):
                errors.append(f"{field}: invalid date/time format")

        if field == "trip_end_date":
            try:
                cleaned_record[field] = datetime.strptime(value, "%m/%d/%y %H:%M")
            except (ValueError, TypeError):
                errors.append(f"{field}: invalid date/time format")

    return cleaned_record, warnings, errors
    

try:
    while True:

        msg = consumer.poll(timeout=10)

        if msg is None:
            continue
        elif msg.error():
            print(f"Consumer Error: {msg.error()}")
            continue
        else:
            record = msg.value()

            cleaned_record, warnings, errors = validate_and_clean(record)
            print("Validating and cleaning the record for Mongo DB....")
            print()

            if errors:
                print("Record Rejected")
                pprint(errors)
                consumer.commit(asynchronous=False)

            else:
                print("Record is valid and ready for MongoDB.")
                cleaned_record["_id"] = cleaned_record["BookingID"]
                try:
                    result = collection.insert_one(cleaned_record)
                    print(f"Inserted Booking ID: {result.inserted_id}")
                    # Commit only after successful MongoDB insertion.
                    consumer.commit(asynchronous=False)
                except Exception as e:
                    print(f"MongoDB insertion failed: {e}")
                    print("Kafka offset was NOT committed.")

            if warnings:
                print("Warnings:")
                pprint(warnings)

        time.sleep(2)
except KeyboardInterrupt as e:
    print("Stopped by user....")


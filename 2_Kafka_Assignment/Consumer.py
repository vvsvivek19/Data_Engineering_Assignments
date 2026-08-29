from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import os
import json
import threading
import time

# ---------------------------------------------------------
# 1. Configuration
# ---------------------------------------------------------

TOPIC_NAME = "product_updates"
NUM_CONSUMERS = 5
OUTPUT_DIRECTORY = "/Users/vvsvivek/Library/CloudStorage/OneDrive-Personal/Skills & Career/3. Data Engineering/Data_Engineering_Assignments/2_Kafka_Assignment/Consumer_files"
CONSUMER_GROUP = "buyonline-multi-consumer-v2"

# Create output directory
os.makedirs(OUTPUT_DIRECTORY,exist_ok=True)

# ---------------------------------------------------------
# 2. Setting up kafka config
# ---------------------------------------------------------

kafka_config = {
    "bootstrap.servers": "pkc-921jm.us-east-2.aws.confluent.cloud:9092",
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
        "value.deserializer":value_deserializer
    }

# ---------------------------------------------------------
# 5. Consumer Function
# ---------------------------------------------------------

def consume_messages(consumer_number):

    consumer_name = f"consumer_number_{consumer_number}"
    output_file = f"{OUTPUT_DIRECTORY}/{consumer_name}.json"
    consumer = DeserializingConsumer(consumer_config)
    consumer.subscribe([TOPIC_NAME])
    print(f"{consumer_name} subscribed to {TOPIC_NAME}")

    # Poll until Kafka assigns partitions to this consumer
    while not consumer.assignment():
        consumer.poll(0.5)

    print(f"{consumer_name} assigned partitions: {consumer.assignment()}")

    #running a infinite loop so that consumer keeps on polling
    try: 
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue
            elif message.error():
                print(f"{consumer_name} error: {message.error()}")
                continue

            product = message.value()

            #data tranformations
            product["name"] = product["name"].upper()

            if product["category"] == "Fitness":
                product["price"] = round(float(product["price"]) * 0.5,2)


            with open(output_file,"a") as file:
                json.dump(product,file)
                file.write("\n")

            print(
                f"{consumer_name} | "
                f"Partition: {message.partition()} | "
                f"Offset: {message.offset()} | "
                f"Key: {message.key()} | "
                f"Product: {product}"
            )
    except KeyboardInterrupt:
        print(f"{consumer_name} stopped")

    finally:
        consumer.close()
        print(f"{consumer_name} closed")

# ---------------------------------------------------------
# 7. Do basic thread to invoke multiple consumers instance
# ---------------------------------------------------------

threads = []
for consumer_number in range(1,NUM_CONSUMERS+1):
    thread = threading.Thread(target=consume_messages,args=(consumer_number,))
    threads.append(thread)
    thread.start()

# Wait for all consumers to finish
for thread in threads:
    thread.join()



    

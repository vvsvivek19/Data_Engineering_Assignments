import mysql.connector
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import os

# ---------------------------------------------------------
# 1. Configuration
# ---------------------------------------------------------

TOPIC_NAME = "product_updates"
CHECKPOINT_DIRECTORY = "/Users/vvsvivek/Library/CloudStorage/OneDrive-Personal/Skills & Career/3. Data Engineering/Data_Engineering_Assignments/2_Kafka_Assignment/Checkpoint.txt"

# ---------------------------------------------------------
# 2. Setting up kafka config
# ---------------------------------------------------------

kafka_config = {
    "bootstrap.servers": "pkc-9q8rv.ap-south-2.aws.confluent.cloud:9092",
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

# ---------------------------------------------------------
# 4. Fetching latest schema from Schema Registry
# Why? - To get the schema that defines the structure of the Kafka message.
# ---------------------------------------------------------

subject_name = 'product_updates-value' # each schema in registry is associated with a subject name
latest_schema = schema_registry_client.get_latest_version(subject_name)
schema_str = latest_schema.schema.schema_str

# ---------------------------------------------------------
# 5. Define serializers and create a SeriliazingProducer
# Why? - To automatically convert Python data into the format Kafka expects.
# ---------------------------------------------------------
key_serializer = StringSerializer('utf-8')
value_serializer = AvroSerializer(schema_registry_client,schema_str)

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
# 6. Read the last processed timestamp and fetch data accordingly
# Why? - To fetch only records added or updated since the last run.
# ---------------------------------------------------------------

try:
    with open(CHECKPOINT_DIRECTORY,'r') as file:
        last_processed_timestamp = file.read().strip()
        # Setting default timestamp
        if not last_processed_timestamp:
            last_processed_timestamp = '1900-01-01 00:00:00'
except FileNotFoundError:
    last_processed_timestamp = '1900-01-01 00:00:00'

print("Last processed timestamp:", last_processed_timestamp)

connection = mysql.connector.connect(
    host = "localhost",
    user = "root",
    database = "buyonline",
    password = os.getenv("MYSQL_PASSWORD")
)

cursor = connection.cursor()

select_query = """
    SELECT id,name,category,price,last_updated
    from products
    WHERE last_updated > %s
    ORDER BY last_updated;
"""

cursor.execute(select_query,(last_processed_timestamp,))
rows = cursor.fetchall()

# ---------------------------------------------------------------
# 7. Preparing a product list of dictionary from fetched data
# Preparing data that would be fed to producer for sending to Kafka Topic
# ---------------------------------------------------------------

products = []

for row in rows:
    product = {
        "id": row[0],
        "name":row[1],
        "category":row[2],
        "price":float(row[3]),
        "last_updated": row[4].strftime("%Y-%m-%dT%H:%M:%S")
    }
    products.append(product)

# saving timestamp of latest record so that we can later use it for updating checkpoint
if rows:
    latest_timestamp = rows[-1][4]
else:
    latest_timestamp = None

# ---------------------------------------------------------
# 8. Delivery callback
# ---------------------------------------------------------

# Tracking delivery status of messages
deliver_status = {
    "success": 0,
    "failed": 0
}

def deliver_report(err,msg):
    if err is not None:
        print(f"Message Delivery Failed: {err}")
        # counting if any message delivery failed
        deliver_status["failed"] += 1
    else:
        print(
            f"Message Delivered - "
            f"Topic: {msg.topic()} | "
            f"Key: {msg.key()} | "
            f"Partition: {msg.partition()}"
        )
        # updating this so that we later see how many messages successfully delivered
        deliver_status["success"] += 1


# ---------------------------------------------------------
# 9. Serialize products data and send them to Kafka
# Why? - To send the extracted product data to the Kafka topic.
# ---------------------------------------------------------

for product in products:
    producer.produce(
        topic = TOPIC_NAME,
        key = str(product["id"]),
        value = product,
        on_delivery= deliver_report
    )

#Waiting for messages to be delivered
# Why? - To make sure all messages are delivered before continuing.
producer.flush()

# ---------------------------------------------------------
# 10. Updating the checkpoint
# ---------------------------------------------------------

# if no new data was fetched
if not rows:
    print("No new or changed records found. Checkpoint not updated.")
# id now message delivery failed and all messages delivered successfully
elif deliver_status["failed"] == 0 and deliver_status["success"] == len(products):
    with open(CHECKPOINT_DIRECTORY,'w') as file:
        # using the latest_timestamp we saved before for use
        file.write(latest_timestamp.strftime("%Y-%m-%d %H:%M:%S"))
    print(f"Checkpoint updated to: {latest_timestamp}")
else:
    print(
            "Kafka delivery failure detected. "
            "Checkpoint was NOT updated."
        )

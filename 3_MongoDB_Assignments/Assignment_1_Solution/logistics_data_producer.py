from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import os
import pandas as pd
from pprint import pprint

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
# 5. Reading data from logistics csv file using pandas and 
# preparing it to sent to kafka topic
# ---------------------------------------------------------------


df = pd.read_csv(csv_file_path)
records = df.to_dict(orient="records")

logistics_records = []

for record in records:
    logistics_record = {}

    if pd.isna(record['GpsProvider']):
        logistics_record['GpsProvider'] = None
    else:
        logistics_record['GpsProvider'] = str(record['GpsProvider'])

    if pd.isna(record['BookingID']):
        logistics_record['BookingID'] = None
    else:
        logistics_record['BookingID'] = str(record['BookingID'])

    if pd.isna(record['Market_Regular']):
        logistics_record['Market_Regular'] = None
    else:
        logistics_record['Market_Regular'] = str(record['Market_Regular'])

    if pd.isna(record['BookingID_Date']):
        logistics_record['BookingID_Date'] = None
    else:
        logistics_record['BookingID_Date'] = str(record['BookingID_Date'])

    if pd.isna(record['vehicle_no']):
        logistics_record['vehicle_no'] = None
    else:
        logistics_record['vehicle_no'] = str(record['vehicle_no'])

    if pd.isna(record['Origin_Location']):
        logistics_record['Origin_Location'] = None
    else:
        logistics_record['Origin_Location'] = str(record['Origin_Location'])

    if pd.isna(record['Destination_Location']):
        logistics_record['Destination_Location'] = None
    else:
        logistics_record['Destination_Location'] = str(record['Destination_Location'])

    if pd.isna(record['Org_lat_lon']):
        logistics_record['Org_lat_lon'] = None
    else:
        logistics_record['Org_lat_lon'] = str(record['Org_lat_lon'])

    if pd.isna(record['Des_lat_lon']):
        logistics_record['Des_lat_lon'] = None
    else:
        logistics_record['Des_lat_lon'] = str(record['Des_lat_lon'])

    if pd.isna(record['Data_Ping_time']):
        logistics_record['Data_Ping_time'] = None
    else:
        logistics_record['Data_Ping_time'] = str(record['Data_Ping_time'])

    if pd.isna(record['Planned_ETA']):
        logistics_record['Planned_ETA'] = None
    else:
        logistics_record['Planned_ETA'] = str(record['Planned_ETA'])

    if pd.isna(record['Current_Location']):
        logistics_record['Current_Location'] = None
    else:
        logistics_record['Current_Location'] = str(record['Current_Location'])

    if pd.isna(record['DestinationLocation']):
        logistics_record['DestinationLocation'] = None
    else:
        logistics_record['DestinationLocation'] = str(record['DestinationLocation'])

    if pd.isna(record['actual_eta']):
        logistics_record['actual_eta'] = None
    else:
        logistics_record['actual_eta'] = str(record['actual_eta'])

    if pd.isna(record['Curr_lat']):
        logistics_record['Curr_lat'] = None
    else:
        logistics_record['Curr_lat'] = float(record['Curr_lat'])

    if pd.isna(record['Curr_lon']):
        logistics_record['Curr_lon'] = None
    else:
        logistics_record['Curr_lon'] = float(record['Curr_lon'])

    if pd.isna(record['ontime']):
        logistics_record['ontime'] = None
    else:
        logistics_record['ontime'] = str(record['ontime'])

    if pd.isna(record['delay']):
        logistics_record['delay'] = None
    else:
        logistics_record['delay'] = str(record['delay'])

    if pd.isna(record['OriginLocation_Code']):
        logistics_record['OriginLocation_Code'] = None
    else:
        logistics_record['OriginLocation_Code'] = str(record['OriginLocation_Code'])

    if pd.isna(record['DestinationLocation_Code']):
        logistics_record['DestinationLocation_Code'] = None
    else:
        logistics_record['DestinationLocation_Code'] = str(record['DestinationLocation_Code'])

    if pd.isna(record['trip_start_date']):
        logistics_record['trip_start_date'] = None
    else:
        logistics_record['trip_start_date'] = str(record['trip_start_date'])

    if pd.isna(record['trip_end_date']):
        logistics_record['trip_end_date'] = None
    else:
        logistics_record['trip_end_date'] = str(record['trip_end_date'])

    if pd.isna(record['TRANSPORTATION_DISTANCE_IN_KM']):
        logistics_record['TRANSPORTATION_DISTANCE_IN_KM'] = None
    else:
        logistics_record['TRANSPORTATION_DISTANCE_IN_KM'] = float(
            record['TRANSPORTATION_DISTANCE_IN_KM']
        )

    if pd.isna(record['vehicleType']):
        logistics_record['vehicleType'] = None
    else:
        logistics_record['vehicleType'] = str(record['vehicleType'])

    if pd.isna(record['Minimum_kms_to_be_covered_in_a_day']):
        logistics_record['Minimum_kms_to_be_covered_in_a_day'] = None
    else:
        logistics_record['Minimum_kms_to_be_covered_in_a_day'] = float(
            record['Minimum_kms_to_be_covered_in_a_day']
        )

    if pd.isna(record['Driver_Name']):
        logistics_record['Driver_Name'] = None
    else:
        logistics_record['Driver_Name'] = str(record['Driver_Name'])

    if pd.isna(record['Driver_MobileNo']):
        logistics_record['Driver_MobileNo'] = None
    else:
        logistics_record['Driver_MobileNo'] = str(record['Driver_MobileNo'])

    if pd.isna(record['customerID']):
        logistics_record['customerID'] = None
    else:
        logistics_record['customerID'] = str(record['customerID'])

    if pd.isna(record['customerNameCode']):
        logistics_record['customerNameCode'] = None
    else:
        logistics_record['customerNameCode'] = str(record['customerNameCode'])

    if pd.isna(record['supplierID']):
        logistics_record['supplierID'] = None
    else:
        logistics_record['supplierID'] = str(record['supplierID'])

    if pd.isna(record['supplierNameCode']):
        logistics_record['supplierNameCode'] = None
    else:
        logistics_record['supplierNameCode'] = str(record['supplierNameCode'])

    if pd.isna(record['Material Shipped']):
        logistics_record['Material_Shipped'] = None
    else:
        logistics_record['Material_Shipped'] = str(record['Material Shipped'])

    logistics_records.append(logistics_record)

pprint(logistics_records[0])

# ---------------------------------------------------------
# 6. Delivery callback
# ---------------------------------------------------------

def deliver_report(err,msg):
    if err is not None:
        print(f"Message Delivery Failed {err}")
    else:
        print(
            f"Message Delivered - "
            f"Topic: {msg.topic()} | "
            f"Key: {msg.key()} | "
            f"Partition: {msg.partition()}"
        )

# ---------------------------------------------------------
# 7. Serializing logistics data and sending it to Kafka
# ---------------------------------------------------------

for logistics_record in logistics_records:
    producer.produce(
        topic=TOPIC_NAME,
        key = logistics_record["BookingID"],
        value = logistics_record,
        on_delivery=deliver_report
    )

producer.flush()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import  *
import os

# ---------------------------------------------------------
# 1. Create SparkSession
# ---------------------------------------------------------


spark = SparkSession.builder.\
    appName("AdsStreamConsumer")\
    .config("spark.sql.shuffle.partitions", "2")\
    .config("spark.streaming.stopGracefullyOnShutdown", "true")\
    .getOrCreate()

# ---------------------------------------------------------
# 2. Create kafka options for Spark Structured Streaming
# ---------------------------------------------------------

kafka_options = {
    "kafka.bootstrap.servers": spark.conf.get("spark.kafka.url"),
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        "org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{spark.conf.get("spark.kafka.username")}" '
        f'password="{spark.conf.get("spark.kafka.password")}";'
    ),
    "subscribe": "ads_data",
    "startingOffsets": "earliest"
}

# ---------------------------------------------------------
# 3. Read data from Kafka topic using Spark Structured Streaming
# ---------------------------------------------------------
df = spark.readStream.format("kafka").options(**kafka_options).load()

from pyspark.sql.functions import col

raw_df = df.select(
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
    col("topic"),
    col("partition"),
    col("offset")
)

query = (
    raw_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()



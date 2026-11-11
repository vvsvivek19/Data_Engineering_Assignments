from pyspark.sql import SparkSession

# ---------------------------------------------------------
# 1. Create SparkSession
# ---------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("SparkConnectionTest")
    .getOrCreate()
)

# ---------------------------------------------------------
# 2. Verify Spark is running
# ---------------------------------------------------------
print("=" * 60)
print("Spark application started successfully!")
print(f"Spark version : {spark.version}")
print(f"Application ID: {spark.sparkContext.applicationId}")
print("=" * 60)

# ---------------------------------------------------------
# 3. Simple Spark operation
# ---------------------------------------------------------
data = [
    (1, "Kafka"),
    (2, "Spark"),
    (3, "MySQL")
]

df = spark.createDataFrame(data, ["id", "technology"])

df.show()

# ---------------------------------------------------------
# 4. Stop Spark
# ---------------------------------------------------------
spark.stop()






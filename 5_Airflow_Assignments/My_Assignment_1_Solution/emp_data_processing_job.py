from pyspark.sql import SparkSession
from pyspark.sql import *

def data_processing(): 
    # creating spark session
    spark = SparkSession.builder.appName("employee data processing job").enableHiveSupport().getOrCreate()
    
    # gcp bucket path
    bucket_name = "daily_data_assignment_1"
    file_path = f"gs://{bucket_name}/2026-10-24"
    
    employee_df = spark.read.format('csv').option("inferSchema",True).option("header",True).load(file_path)
    
    #employee_id,first_name,last_name,email,department,salary,location,hire_date
    
    # Performing a test logical operation on data frame
    employee_df.createOrReplaceTempView('employee_view')
    
    sql_query = """
    SELECT concat(first_name,' ',last_name) as full_name,
    email,
    department,
    location
    from employee_view
    where salary > 70000
    """
    
    filtered_employee_df = spark.sql(sql_query)
    
    # create a database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS employee_processed")
    
    # Hive query to create a table in the table if not exists
    table_creation_query = """
    CREATE TABLE IF NOT EXISTS employee_processed.employee_data (
        full_name STRING,
        email STRING,
        department STRING,
        location STRING
    ) STORED AS PARQUET
    """
    spark.sql(table_creation_query)
    
    #Writing the filtered df to hive table
    filtered_employee_df.write.mode("append").format("hive").saveAsTable("employee_processed.employee_data")
    
    print("Employee data processed successfully")
    spark.stop()

if __name__ == "__main__":
    data_processing()
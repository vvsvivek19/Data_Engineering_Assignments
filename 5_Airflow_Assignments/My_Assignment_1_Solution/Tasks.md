1. Create a GCP bucket
2. What task the dag will have?
- GCSObjectExistenceSensor: poke every 5 minutes
- DataprocClusterCreateOperator: create data proc cluster every time there is file
- PySpark Application - stored in a gcp bucket
    - It will read csv file from bucket
    - perform some logical operations on it
    - Write transformed data into a Hive table
- DataProcPySparkOperator: use this operator to execute the job on newly created cluster
- DataprocClusterDeleteOperator: to delete the newly created dataproc cluster.

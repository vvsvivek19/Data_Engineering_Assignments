-- ========================================================================================================================
--                                                  Problem 1: Data Ingestion
-- ========================================================================================================================

-- command to check all folders in hadoop hdfs
-- hadoop fs -ls /

-- Command to create directory in hadoop hdfs
-- hadoop fs -mkdir /assignments
-- hadoop fs -mkdir /assignments/assignment_02
-- hadoop fs -mkdir /assignments/assignment_02/dataset_customer_demographics
-- hadoop fs -mkdir /assignments/assignment_02/dataset_telecom_customer_churn_data

-- Command to move a local file into hadoop cluster in gcloud
-- gcloud compute scp C:\Users\vvsvi\OneDrive\Desktop\A2_CustomerDemographics.csv vvsvivek19@test-hive-cluster-m:/home/vvsvivek19
-- gcloud compute scp C:\Users\vvsvi\OneDrive\Desktop\A2_Telecom_customer_churn_data.csv vvsvivek19@test-hive-cluster-m:/home/vvsvivek19

-- Command to put file from local to hdfs
-- hadoop fs -put /home/vvsvivek19/A2_CustomerDemographics.csv /assignments/assignment_02/dataset_customer_demographics/
-- hadoop fs -put /home/vvsvivek19/A2_Telecom_customer_churn_data.csv /assignments/assignment_02/dataset_telecom_customer_churn_data/


-- HQL Command to show all the databases in Hive
show databases;

-- HQL Command to create database
create database assignment_02_db;

-- HQL Command to use database
use assignment_02_db;

-- HQL Command to show all tables in current database
show tables;

-- HQL Command to create external table for customer_demographics CSV data
CREATE EXTERNAL TABLE customer_demographics
(
    CustomerID string,
    City string,
    Lat float,
    Long float,
    country string,
    iso2 string,
    State string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/assignments/assignment_02/dataset_customer_demographics/'
TBLPROPERTIES("skip.header.line.count"="1");

-- HQL Command to get details of Hive Table
describe formatted customer_demographics;

-- Removes the table name prefix from the column headers
set hive.resultset.use.unique.column.names=false;

-- Ensures column headers are printed (you likely already have this on, but good to be sure)
set hive.cli.print.header=true;

-- SELECT limited rows from the table 
SELECT * FROM customer_demographics LIMIT 10;

-- HQL Command to create external table for telecom_customer_churn_data CSV data
CREATE EXTERNAL TABLE telecom_customer_churn_data
(
    customerID string,
    gender string,
    SeniorCitizen string,
    Partner string,
    Dependents string,
    tenure int,
    PhoneService string,
    MultipleLines string,
    InternetService string,
    OnlineSecurity string,
    OnlineBackup string,
    DeviceProtection string,
    TechSupport string,
    StreamingTV string,
    StreamingMovies string,
    Contract string,
    PaperlessBilling string,
    PaymentMethod string,
    MonthlyCharges float,
    TotalCharges float,
    Churn string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/assignments/assignment_02/dataset_telecom_customer_churn_data/'
TBLPROPERTIES("skip.header.line.count"="1");

-- HQL Command to get details of Hive Table
describe formatted telecom_customer_churn_data;

-- Removes the table name prefix from the column headers
set hive.resultset.use.unique.column.names=false;

-- Ensures column headers are printed (you likely already have this on, but good to be sure)
set hive.cli.print.header=true;

-- SELECT limited rows from the table 
SELECT * FROM telecom_customer_churn_data LIMIT 10;

-- ========================================================================================================================
--                                                  Problem 2: Data Exploration
-- ========================================================================================================================

-- Write a HiveQL query to find the total number of customers in the dataset.
SELECT COUNT(*) as total_customers FROM customer_demographics;

-- Write a HiveQL query to find the total number of customers who have churned.
SELECT COUNT(*) as total_customer_churn FROM telecom_customer_churn_data
WHERE churn = 'Yes';

-- Analyze the distribution of customers based on gender and SeniorCitizen status
SELECT gender,SeniorCitizen,COUNT(*) as total_count FROM telecom_customer_churn_data
GROUP BY gender,SeniorCitizen;

-- Determine the total charge to the company due to churned customers.
SELECT SUM(TotalCharges) as totalcharge FROM telecom_customer_churn_data
WHERE churn = 'Yes';

-- ========================================================================================================================
--                                                  Problem 3: Data Analysis (Intermediate)
-- ========================================================================================================================

-- Write a HiveQL query to find the number of customers who have churned, grouped by their Contract type
SELECT Contract, COUNT(*) as total_count FROM telecom_customer_churn_data
WHERE churn = 'Yes'
GROUP BY Contract;

-- Write a HiveQL query to find the average MonthlyCharges for customers who have churned vs those who have not
SELECT churn, AVG(MonthlyCharges) as avg_monthly_charges FROM telecom_customer_churn_data
GROUP BY churn;

-- Write a HiveQL query to find the maximum, minimum, and average tenure of customers
SELECT 
MAX(tenure) as max_tenure, 
MIN(tenure) as min_tenure, 
AVG(tenure) as avg_tenure 
FROM telecom_customer_churn_data;

-- Find out which PaymentMethod is most popular among customers
SELECT PaymentMethod, COUNT(*) as total_count FROM telecom_customer_churn_data
GROUP BY PaymentMethod
ORDER BY total_count DESC
LIMIT 1;

-- Analyze the relationship between PaperlessBilling and churn rate
SELECT PaperlessBilling, churn, COUNT(*) as total_count FROM telecom_customer_churn_data
GROUP BY PaperlessBilling, churn
ORDER BY total_count DESC;

-- ========================================================================================================================
--                                                  Problem 4: Partitioning (Intermediate)
-- ========================================================================================================================

-- Enables the use of dynamic partitioning
SET hive.exec.dynamic.partition = true;

-- Allows all partitions to be dynamic (default is 'strict', which requires at least one static partition)
SET hive.exec.dynamic.partition.mode = nonstrict;

-- Create a partitioned table by Contract and load the data from the original table
CREATE TABLE telecom_customer_churn_data_partitioned_by_contract
(
    customerID string,
    gender string,
    SeniorCitizen string,
    Partner string,
    Dependents string,
    tenure int,
    PhoneService string,
    MultipleLines string,
    InternetService string,
    OnlineSecurity string,
    OnlineBackup string,
    DeviceProtection string,
    TechSupport string,
    StreamingTV string,
    StreamingMovies string,
    PaperlessBilling string,
    PaymentMethod string,
    MonthlyCharges float,
    TotalCharges float,
    Churn string
)
Partitioned by(Contract string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Insert the data from the original table to the partitioned table
INSERT OVERWRITE TABLE telecom_customer_churn_data_partitioned_by_contract 
PARTITION(Contract)
SELECT 
    CustomerID, gender, SeniorCitizen, Partner, Dependents, tenure, 
    PhoneService, MultipleLines, InternetService, OnlineSecurity, OnlineBackup, 
    DeviceProtection, TechSupport, StreamingTV, StreamingMovies, 
    PaperlessBilling, PaymentMethod, MonthlyCharges, TotalCharges, Churn, Contract
FROM telecom_customer_churn_data;

-- Output note: Only 3 partitions are created because there are only 3 distinct values in the Contract column.

-- Write a HiveQL query to find the number of customers who have churned in each Contract type using the partitioned table.
SELECT Contract, Count(*) as total
FROM telecom_customer_churn_data_partitioned_by_contract
WHERE churn = 'Yes'
GROUP BY Contract;

-- Output time: 7.76 seconds

-- Compare this result with the result of the same query on the non-partitioned table and note the time difference.
SELECT Contract, COUNT(*) as total
FROM telecom_customer_churn_data
WHERE churn = 'Yes'
GROUP BY Contract;

-- Output time: 6.02 seconds

-- Learning note: I think the data set is small therefore we can't see that much difference in performance.

-- Find the average MonthlyCharges for each type of Contract using the partitioned table.
SELECT Contract, AVG(MonthlyCharges) as Avg_monthly_charges
FROM telecom_customer_churn_data_partitioned_by_contract
GROUP BY Contract;

-- Determine the maximum tenure in each Contract type partition.
SELECT Contract, MAX(tenure) as max_tenure
FROM telecom_customer_churn_data_partitioned_by_contract
GROUP BY Contract;

-- ========================================================================================================================
--                                                  Problem 5: Bucketing (Advance)
-- ========================================================================================================================

-- Set Properties and Command to create buckets
set hive.enforce.bucketing=true;

-- Create a bucketed table by 'tenure' in 6 buckets
CREATE EXTERNAL TABLE telecom_customer_churn_data_bucketed_by_tenure
(
    customerID string,
    gender string,
    SeniorCitizen string,
    Partner string,
    Dependents string,
    tenure int,
    PhoneService string,
    MultipleLines string,
    InternetService string,
    OnlineSecurity string,
    OnlineBackup string,
    DeviceProtection string,
    TechSupport string,
    StreamingTV string,
    StreamingMovies string,
    Contract string,
    PaperlessBilling string,
    PaymentMethod string,
    MonthlyCharges float,
    TotalCharges float,
    Churn string
)
CLUSTERED BY (tenure)
INTO 6 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- load data from the original table to the bucketed table
INSERT OVERWRITE TABLE telecom_customer_churn_data_bucketed_by_tenure
SELECT * FROM telecom_customer_churn_data;

-- Analyze the average MonthlyCharges for each bucket of tenure.
SELECT tenure, AVG(MonthlyCharges) as avg_monthly_charges
FROM telecom_customer_churn_data_bucketed_by_tenure
GROUP BY tenure;

-- Find the maximum TotalCharges for each bucket of tenure.
SELECT tenure, MAX(TotalCharges) as max_total_charges
FROM telecom_customer_churn_data_bucketed_by_tenure
GROUP BY tenure;

-- ========================================================================================================================
--                                                  Problem 6: Performance Optimization with Joins (Advanced)
-- ========================================================================================================================

/*
Write HiveQL queries to join the customer churn table and the customer_demographics table on customerID using different types of joins - 
1. Common join
2. Map join
3. Bucket map join
4. Sorted merge bucket join.
Observe and document the performance of each join type
*/

-- 1. Common join
SELECT t1.customerID, t1.Contract, t2.city, t2.state FROM telecom_customer_churn_data t1 JOIN customer_demographics t2 ON t1.customerID = t2.customerID;
/*
Performance of Common Join:
Time taken: 15.656 seconds, 
Fetched: 7043 row(s)
*/

-- 2. Map join
SET hive.auto.convert.join=true;
-- SET hive.mapjoin.smalltable.filesize=20000000; -- not neccesaary in this case
SELECT /*+ MAPJOIN(t2) */ t1.customerID, t1.Contract, t2.city, t2.state FROM customer_demographics t2 JOIN telecom_customer_churn_data t1 ON t1.customerID = t2.customerID;
/*
Performance of Map Join:
Time taken: 15.104 seconds
Fetched: 7043 row(s)
*/

-- 3. Bucket map join

-- Turn on bucket map join
SET hive.optimize.bucketmapjoin = true;
-- creating a bucketed table for customer_demographics on CustomerID column in 4 buckets
CREATE TABLE customer_demographics_bucketed_by_customerid
(
    CustomerID string,
    City string,
    Lat float,
    Long float,
    country string,
    iso2 string,
    State string
)
CLUSTERED BY (CustomerID)
INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- insert data into bucketed table
INSERT OVERWRITE TABLE customer_demographics_bucketed_by_customerid
SELECT * FROM customer_demographics;

-- creating a bucketed table for telecom_customer_churn_data on CustomerID column in 4 buckets
CREATE TABLE telecom_customer_churn_data_bucketed_by_customerid
(
    customerID string,
    gender string,
    SeniorCitizen string,
    Partner string,
    Dependents string,
    tenure int,
    PhoneService string,
    MultipleLines string,
    InternetService string,
    OnlineSecurity string,
    OnlineBackup string,
    DeviceProtection string,
    TechSupport string,
    StreamingTV string,
    StreamingMovies string,
    Contract string,
    PaperlessBilling string,
    PaymentMethod string,
    MonthlyCharges float,
    TotalCharges float,
    Churn string
)
CLUSTERED BY (customerID)
INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- insert data into bucketed table
INSERT OVERWRITE TABLE telecom_customer_churn_data_bucketed_by_customerid
SELECT * FROM telecom_customer_churn_data;


SELECT t1.customerID, t1.Contract, t2.city, t2.state FROM telecom_customer_churn_data_bucketed_by_customerid t1 JOIN customer_demographics_bucketed_by_customerid t2 ON t1.customerID = t2.customerID;
/*
Performance of Bucket Map Join:
Time taken: 12.001 seconds
Fetched: 7043 row(s)
*/

-- 4. Sorted merge bucket join

-- 1. Create Sorted Bucketed Table for Demographics
CREATE TABLE customer_demographics_smb
(
    CustomerID string,
    City string,
    Lat float,
    Long float,
    country string,
    iso2 string,
    State string
)
CLUSTERED BY (CustomerID) 
SORTED BY (CustomerID) -- New required clause
INTO 4 BUCKETS
STORED AS TEXTFILE;

-- 2. Create Sorted Bucketed Table for Churn
CREATE TABLE telecom_customer_churn_data_smb
(
    customerID string,
    gender string,
    SeniorCitizen string,
    Partner string,
    Dependents string,
    tenure int,
    PhoneService string,
    MultipleLines string,
    InternetService string,
    OnlineSecurity string,
    OnlineBackup string,
    DeviceProtection string,
    TechSupport string,
    StreamingTV string,
    StreamingMovies string,
    Contract string,
    PaperlessBilling string,
    PaymentMethod string,
    MonthlyCharges float,
    TotalCharges float,
    Churn string
)
CLUSTERED BY (customerID) 
SORTED BY (customerID) -- New required clause
INTO 4 BUCKETS
STORED AS TEXTFILE;

-- Ensure Hive respects the sorting and bucketing during the insert
SET hive.enforce.bucketing = true;
SET hive.enforce.sorting = true;

INSERT OVERWRITE TABLE customer_demographics_smb 
SELECT * FROM customer_demographics;

INSERT OVERWRITE TABLE telecom_customer_churn_data_smb 
SELECT * FROM telecom_customer_churn_data;

-- Turn on sorted merge bucket join
SET hive.optimize.sortmerge.join=true;
SET hive.optimize.bucketmapjoin.sortedmerge = true;

SELECT t1.customerID, t1.Contract, t2.city, t2.state FROM telecom_customer_churn_data_smb t1 JOIN customer_demographics_smb t2 ON t1.customerID = t2.customerID;
/*
Performance of Sorted Merge Bucket Join:
Time taken: 11.855 seconds
Fetched: 7043 row(s)
*/

-- ========================================================================================================================
--                                                  Problem 7: Advance Analysis (Expert)
-- ========================================================================================================================

-- Find the distribution of PaymentMethod among churned customers
SELECT PaymentMethod, COUNT(*) as churn_count
FROM telecom_customer_churn_data
WHERE Churn = 'Yes' 
GROUP BY PaymentMethod
ORDER BY churn_count DESC;
/*
PaymentMethod             | churn_count
****************************************
Electronic check          | 1071
Mailed check              | 308
Bank transfer (automatic) | 258
Credit card (automatic)   | 232
****************************************
*/

-- Calculate the churn rate (percentage of customers who left) for each InternetService category
SELECT InternetService, (COUNT(*)/(SELECT Count(*) FROM telecom_customer_churn_data WHERE churn = 'Yes'))*100  ChurnRate
FROM telecom_customer_churn_data
WHERE churn = 'Yes'
GROUP BY InternetService;
/*
InternetService          | churn_rate
****************************************
DSL                      | 24.558587479935795
Fiber optic              | 69.39539860888175
No                       | 6.046013911182451
****************************************
*/

-- Find the number of customers who have no dependents and have churned, grouped by Contract typ
SELECT Contract, COUNT(*) as total
FROM telecom_customer_churn_data
WHERE Dependents = 'No' AND churn = 'Yes'
GROUP BY Contract;

/*
Contract         | total
****************************************
Month-to-month   | 1396
One year         | 117
Two year         | 30
****************************************
*/

-- Find the top 5 tenure lengths that have the highest churn rates
SELECT tenure, (COUNT(*) / (SELECT COUNT(*) FROM telecom_customer_churn_data WHERE churn = 'Yes'))*100  ChurnRate
FROM telecom_customer_churn_data
WHERE churn = 'Yes'
GROUP BY tenure
ORDER BY ChurnRate DESC
LIMIT 5; 

/*
tenure    | churn_rate
*****************************
1         | 20.331728196896737
2         | 6.5810593900481535
3         | 5.029427501337614
4         | 4.440877474585339
5         | 3.4242910647405025   
*****************************
*/

-- Calculate the average MonthlyCharges for customers who have PhoneService and have churned, grouped by Contract type.
SELECT Contract, AVG(MonthlyCharges) as average_monthly_charges
FROM telecom_customer_churn_data
WHERE PhoneService = 'Yes' AND churn = 'Yes'
GROUP BY Contract;
/*
Contract         | average_monthly_charges
****************************************
Month-to-month   | 76.74384155095656
One year         | 88.59835529327393
Two year         | 89.19777755737304
****************************************
*/

-- Identify which InternetService type is most associated with churned customers
SELECT InternetService, COUNT(*) as churn_count
FROM telecom_customer_churn_data
WHERE Churn = 'Yes'
GROUP BY InternetService
ORDER BY churn_count DESC
LIMIT 1;
/*
InternetService  | churn_count
****************************************
Fiber optic      | 1297
****************************************
*/

-- Determine if customers with a partner have a lower churn rate compared to those without.
SELECT Partner, COUNT(*)*100.0/(SELECT COUNT(*) FROM
telecom_customer_churn_data WHERE Partner = 'Yes') AS
churn_rate_with_partner
FROM telecom_customer_churn_data
WHERE Churn = 'Yes' AND Partner = 'Yes'
GROUP BY partner
UNION ALL
SELECT Partner, COUNT(*)*100.0/(SELECT COUNT(*) FROM
telecom_customer_churn_data WHERE Partner = 'No') AS
churn_rate_without_partner
FROM telecom_customer_churn_data
WHERE Churn = 'Yes' AND Partner = 'No'
GROUP BY partner;

/*
partner  | churn_rate
*****************************
Yes      | 9.49879312792844
No       | 17.038193951441148   
*****************************
*/


-- Analyse the relationship between multiplelines and churn rate
SELECT MultipleLines, COUNT(*)*100.0/(SELECT COUNT(*) FROM
telecom_customer_churn_data WHERE MultipleLines = 'Yes') AS
churn_rate_with_multiplelines
FROM telecom_customer_churn_data
WHERE Churn = 'Yes' AND MultipleLines = 'Yes'
GROUP BY MultipleLines
UNION ALL
SELECT MultipleLines, COUNT(*)*100.0/(SELECT COUNT(*) FROM
telecom_customer_churn_data WHERE MultipleLines = 'No') AS
churn_rate_without_multiplelines
FROM telecom_customer_churn_data
WHERE Churn = 'Yes' AND MultipleLines = 'No'
GROUP BY MultipleLines;

/*
MultipleLines | churn_rate
*****************************
Yes           | 12.068720715604146
No            | 12.054522220644612
*****************************
*/

-- ========================================================================================================================
--                                                      Finish
-- ======================================================================================================================== 
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
-- ========================================================================================================================
--                                                     Hive: Assignment 1 Solutions
-- ========================================================================================================================

-- ========================================================================================================================
--                                                  Problem 1: Data Ingestion
-- ========================================================================================================================

-- command to check all folders in hadoop hdfs
-- hadoop fs -ls /

-- Command to create directory in hadoop hdfs
-- hadoop fs -mkdir /assignments
-- hadoop fs -mkdir /assignments/assignment_01

-- Command to move a local file into hadoop cluster in gcloud
-- gcloud compute scp C:\Users\vvsvi\OneDrive\Desktop\A1_car_insurance_cold_calls_dataset.csv vvsvivek19@test-hive-cluster-m:/home/vvsvivek19

-- Command to put file from local to hdfs
-- hadoop fs -put /home/vvsvivek19/A1_car_insurance_cold_calls_dataset.csv /assignments/assignment_01/


-- HQL Command to show all the databases in Hive
show databases;

-- HQL Command to create database
create database assignment_01_db;

-- HQL Command to use database
use assignment_01_db;

-- HQL Command to show all tables in current database
show tables;

-- HQL Command to create table for CSV data
create external table car_insurance_data
(
    ID int,
    Age int,
    Job string,
    Marital string,
    Education string,
    Default int,
    Balance int,
    HHInsurance int,
    Carloan int,
    Communication string,
    LastContactDay int,
    LastContactMonth string,
    NoOfContacts int,
    DaysPassed int,
    PrevAttempts int,
    Outcome string,
    CallStart string,
    CallEnd string,
    CarInsurance int
)
row format delimited
fields terminated by ','
location '/assignments/assignment_01';

-- HQL Command to get details of Hive Table
describe formatted car_insurance_data;

-- Removes the table name prefix from the column headers
set hive.resultset.use.unique.column.names=false;

-- Ensures column headers are printed (you likely already have this on, but good to be sure)
set hive.cli.print.header=true;

-- SELECT limited rows from the table 
SELECT * FROM car_insurance_data LIMIT 10;

-- ========================================================================================================================
--                                                  Problem 2: Data Exploration
-- ========================================================================================================================

-- HQL Command to find total number of records in the table
SELECT COUNT(*) as total_records FROM car_insurance_data;

-- HQL Command to find unique job categories
SELECT DISTINCT Job FROM car_insurance_data;

-- HQL Command to find number of unique job categories
SELECT COUNT(DISTINCT Job) as unique_job_categories FROM car_insurance_data;

-- What is the age distribution of customers in the dataset? Provide a breakdown by age group: 18-30, 31-45, 46-60, 61+
-- First type 
SELECT 
    CASE 
        WHEN Age BETWEEN 18 AND 30 THEN '18-30'
        WHEN Age BETWEEN 31 AND 45 THEN '31-45'
        WHEN Age BETWEEN 46 AND 60 THEN '46-60'
        WHEN Age >= 61 THEN '61+'
    END as age_group,
    COUNT(*) as count
FROM car_insurance_data
GROUP BY age_group;

-- second type
SELECT 
    CASE 
        WHEN Age BETWEEN 18 AND 30 THEN '18-30'
        WHEN Age BETWEEN 31 AND 45 THEN '31-45'
        WHEN Age BETWEEN 46 AND 60 THEN '46-60'
        WHEN Age >= 61 THEN '61+'
    END as age_group,
    COUNT(*) as count
FROM car_insurance_data
GROUP BY 
    CASE 
        WHEN Age BETWEEN 18 AND 30 THEN '18-30'
        WHEN Age BETWEEN 31 AND 45 THEN '31-45'
        WHEN Age BETWEEN 46 AND 60 THEN '46-60'
        WHEN Age >= 61 THEN '61+'
    END;

-- third type - with cte
WITH AgeGroups AS (
    SELECT
        CASE
            WHEN Age BETWEEN 18 AND 30 THEN '18-30'
            WHEN Age BETWEEN 31 AND 45 THEN '31-45'
            WHEN Age BETWEEN 46 AND 60 THEN '46-60'
            ELSE '61+'
        END as age_group
    FROM car_insurance_data
)
SELECT
    age_group,
    COUNT(*) as count
FROM AgeGroups
GROUP BY age_group;

-- We can use this hive setting to enable alias support:
SET hive.groupby.orderby.position.alias=true;

-- Count the number of records that have missing values in any field
SELECT COUNT(*) as missing_values_count
FROM car_insurance_data
WHERE ID IS NULL 
   OR Age IS NULL 
   OR Job IS NULL 
   OR Marital IS NULL 
   OR Education IS NULL 
   OR Default IS NULL 
   OR Balance IS NULL 
   OR HHInsurance IS NULL 
   OR Carloan IS NULL 
   OR Communication IS NULL 
   OR LastContactDay IS NULL 
   OR LastContactMonth IS NULL 
   OR NoOfContacts IS NULL 
   OR DaysPassed IS NULL 
   OR PrevAttempts IS NULL 
   OR Outcome IS NULL 
   OR CallStart IS NULL 
   OR CallEnd IS NULL 
   OR CarInsurance IS NULL;

-- Determine the number of distinct 'Outcome' values and their respective counts
SELECT Outcome, COUNT(*) as count
FROM car_insurance_data
GROUP BY Outcome;

-- Find the number of customers who have both a car loan and home insurance
SELECT COUNT(*) as count
FROM car_insurance_data
WHERE Carloan = 1 AND HHInsurance = 1;

-- ========================================================================================================================
--                                                  Problem 3: Data Aggregation 
-- ========================================================================================================================

-- What is the average, minimum, and maximum balance for each job category
SELECT 
    Job,
    AVG(Balance) as avg_balance,
    MIN(Balance) as min_balance,
    MAX(Balance) as max_balance
FROM car_insurance_data
GROUP BY Job;

-- Find the total number of customers with and without car insurance using CTE
WITH InsuranceStatus AS (
    SELECT
        CASE 
            WHEN CarInsurance = 1 THEN 'With Insurance'
            WHEN CarInsurance = 0 THEN 'Without Insurance'
        END as insurance_status
    FROM car_insurance_data
)
SELECT 
    insurance_status,
    COUNT(*) as count
FROM InsuranceStatus GROUP BY insurance_status;

-- Count the number of customers for each communication type.
SELECT Communication, COUNT(*) as count
FROM car_insurance_data GROUP BY Communication;

-- Calculate the sum of 'Balance' for each 'Communication' type
SELECT 
    Communication,
    SUM(Balance) as total_balance
FROM car_insurance_data GROUP BY Communication;

-- Count the number of 'PrevAttempts' for each 'Outcome' type.
SELECT Outcome, SUM(PrevAttempts) as total_attempts FROM car_insurance_data 
GROUP BY Outcome;

-- Calculate the average 'NoOfContacts' for people with and without 'CarInsurance'
SELECT CarInsurance, AVG(NoOfContacts) as avg_no_of_contacts
FROM car_insurance_data GROUP BY CarInsurance;

-- ========================================================================================================================
--                                                  Problem 4: Partitioning and bucketing
-- ========================================================================================================================

-- set this property for dynamic partioning
set hive.exec.dynamic.partition.mode=nonstrict;

-- Create a partitioned table on 'Education' and 'Marital' status. Load data from the original table to this new partitioned table.
CREATE TABLE car_insurance_data_partitioned(
    ID int,
    Age int,
    Job string,
    Default int,
    Balance int,
    HHInsurance int,
    Carloan int,
    Communication string,
    LastContactDay int,
    LastContactMonth string,
    NoOfContacts int,
    DaysPassed int,
    PrevAttempts int,
    Outcome string,
    CallStart string,
    CallEnd string,
    CarInsurance int
)
PARTITIONED BY (Education string, Marital string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;  

-- load data in dynamic partition table
INSERT OVERWRITE TABLE car_insurance_data_partitioned 
PARTITION(Education, Marital)
SELECT 
    id, age, job, default, balance, hhinsurance, carloan, communication, 
    lastcontactday, lastcontactmonth, noofcontacts, dayspassed, prevattempts, 
    outcome, callstart, callend, carinsurance,
    education, -- Penultimate column
    marital    -- Very last column
FROM car_insurance_data;

-- Set Properties and Command to create buckets
set hive.enforce.bucketing=true;

-- Create a bucketed table on 'Age', bucketed into 4 groups (as per the age groups mentioned above). Load data from the original table into this bucketed table.
CREATE TABLE car_insurance_data_bucketed(
    ID int,
    Age int,
    Job string,
    Marital string,
    Education string,
    Default int,
    Balance int,
    HHInsurance int,
    Carloan int,
    Communication string,
    LastContactDay int,
    LastContactMonth string,
    NoOfContacts int,
    DaysPassed int,
    PrevAttempts int,
    Outcome string,
    CallStart string,
    CallEnd string,
    CarInsurance int
)
CLUSTERED BY (Age)
INTO 4 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- load data into bucketed table
INSERT OVERWRITE TABLE car_insurance_data_bucketed SELECT * FROM car_insurance_data;

-- Add an additional partition on 'Job' to the partitioned table created earlier and move the data accordingly
-- Note: You can't add partition to already existing table, so first create a new table with additional partition and then load data into it
CREATE TABLE car_insurance_data_partitioned_new(
    ID int,
    Age int,
    Default int,
    Balance int,
    HHInsurance int,
    Carloan int,
    Communication string,
    LastContactDay int,
    LastContactMonth string,
    NoOfContacts int,
    DaysPassed int,
    PrevAttempts int,
    Outcome string,
    CallStart string,
    CallEnd string,
    CarInsurance int
)
PARTITIONED BY (Education string, Marital string, Job string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE; 

-- load data in dynamic partition table
INSERT OVERWRITE TABLE car_insurance_data_partitioned_new 
PARTITION(Education, Marital, Job)
SELECT 
    id, age, default, balance, hhinsurance, carloan, communication, 
    lastcontactday, lastcontactmonth, noofcontacts, dayspassed, prevattempts, 
    outcome, callstart, callend, carinsurance,
    education, -- Third last column
    marital,   -- Second last column
    job       -- last column
FROM car_insurance_data;

--  Increase the number of buckets in the bucketed table to 10 and redistribute the data.
-- Note: You can't add buckets to already existing bucketed table. Buckets are defined at the time of table creation. So, create a new bucketed table with 10 buckets and load the data accordingly.
CREATE TABLE car_insurance_data_bucketed_new(
    ID int,
    Age int,
    Job string,
    Marital string,
    Education string,
    Default int,
    Balance int,
    HHInsurance int,
    Carloan int,
    Communication string,
    LastContactDay int,
    LastContactMonth string,
    NoOfContacts int,
    DaysPassed int,
    PrevAttempts int,
    Outcome string,
    CallStart string,
    CallEnd string,
    CarInsurance int
)
CLUSTERED BY (Age)
INTO 10 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- load data into bucketed table
INSERT OVERWRITE TABLE car_insurance_data_bucketed_new SELECT * FROM car_insurance_data;

-- ========================================================================================================================
--                                                  Problem 5: Optimised Joins
-- ========================================================================================================================

-- Join the original table with the partitioned table and find out the average 'Balance' for each 'Job' and 'Education' level
SELECT c.Job, p.Education, AVG(c.Balance) as avg_balance
FROM car_insurance_data c
JOIN car_insurance_data_partitioned p ON c.id = p.id
GROUP BY c.Job, p.Education;

-- Join the bucketed table with the original table and find out the total number of contacts for each age group.

SELECT b.Age, SUM(c.NoOfContacts) as total_no_of_contacts
FROM car_insurance_data c
JOIN car_insurance_data_bucketed b ON c.id = b.id
GROUP BY b.Age;

-- Join the partitioned table and the bucketed table based on the 'Id' field and find the total balance for each education level and marital status for each age group 
SELECT b.Age, p.Education, p.Marital, SUM(b.Balance) as total_balance
FROM car_insurance_data_partitioned p
JOIN car_insurance_data_bucketed b ON p.id = b.id
GROUP BY b.Age, p.Education, p.Marital;

-- ========================================================================================================================
--                                                  Problem 6: Window Functions
-- ========================================================================================================================

-- Calculate the cumulative sum of 'NoOfContacts' for each 'Job' category, ordered by 'Age
SELECT Age, Job, NoOfContacts,
SUM(NoOfContacts) OVER (PARTITION BY job ORDER BY Age) as cumulative_no_of_contacts
FROM car_insurance_data;

-- Calculate the running average of 'Balance' for each 'Job' category, ordered by 'Age'
SELECT Age, Job, Balance,
AVG(Balance) OVER (PARTITION BY job ORDER BY Age) as running_avg_balance
FROM car_insurance_data;

-- For each 'Job' category, find the maximum 'Balance' for each 'Age' group using window functions
SELECT job, age, balance
FROM 
(
    SELECT job, age, balance,
    ROW_NUMBER() OVER (PARTITION BY job, Age ORDER BY balance DESC) as ranking
    FROM car_insurance_data
)
WHERE ranking = 1
ORDER BY job, age;

-- Calculate the rank of 'Balance' within each 'Job' category, ordered by 'Balance' descending
SELECT job,
DENSE_RANK(Balance) OVER (PARTITION BY job ORDER BY Balance DESC) as rank_balance
FROM car_insurance_data;

-- ========================================================================================================================
--                                                  Problem 7: Advance Aggregations
-- ========================================================================================================================

-- Find the job category with the highest number of car insurances.
SELECT Job
FROM
(
    SELECT Job, COUNT(CarInsurance) as car_insurance_count
    FROM car_insurance_data
    WHERE CarInsurance = 1
    GROUP BY Job
)t
ORDER BY car_insurance_count DESC
LIMIT 1;

-- Which month has seen the highest number of last contacts?
SELECT LastContactMonth, COUNT(*) total_count
FROM car_insurance_data
GROUP BY LastContactMonth
ORDER BY total_count DESC
LIMIT 1;

-- Calculate the ratio of the number of customers with car insurance 
-- to the number of customers without car insurance for each job category
WITH CTE_without_insurance AS (
    SELECT Job, COUNT(*) as count_without
    FROM Car_insurance_data
    WHERE CarInsurance = 0
    GROUP BY Job
),
CTE_with_insurance AS (
    SELECT Job, COUNT(*) AS count_with
    FROM Car_insurance_data
    WHERE CarInsurance = 1
    GROUP BY Job	
)
SELECT 
    w.Job, 
    -- We multiply by 1.0 to avoid integer division issues in Hive
    (wi.count_with * 1.0 / w.count_without) AS insurance_ratio
FROM CTE_without_insurance w
JOIN CTE_with_insurance wi ON w.Job = wi.Job;


-- Find out the 'Job' and 'Education' level combination which has the highest number of car insurances
SELECT job,education, Count(carinsurance) as total_carinsurance
FROM car_insurance_data
WHERE CarInsurance = 1
GROUP BY job,education
ORDER BY total_carinsurance DESC
LIMIT 1;

--Calculate the average 'NoOfContacts' for each 'Outcome' and 'Job' combination
SELECT Outcome,Job, AVG(NoOfContacts) as avg_no_contacts
FROM car_insurance_data
GROUP BY Outcome,Job;

-- Determine the month with the highest total 'Balance' of customers.
SELECT LastContactMonth, SUM(balance) as total_balance
FROM car_insurance_data
GROUP BY LastContactMonth
ORDER BY total_balance DESC
LIMIT 1;

-- ========================================================================================================================
--                                                  Problem 8: Complex Joins and Aggregations
-- ========================================================================================================================

-- For customers who have both a car loan and home insurance, find out the average 'Balance' for each 'Education' level
SELECT Education, AVG(balance) as avg_balance
FROM car_insurance_data
WHERE HHInsurance = 1 AND CarLoan = 1
GROUP BY Education;

-- Identify the top 3 'Communication' types for customers with 'CarInsurance', and display their average 'NoOfContacts'.
SELECT Communication,AVG(NoOfContacts) as avg_no_contacts
FROM car_insurance_data
WHERE CarInsurance = 1
GROUP BY Communication
LIMIT 3;

-- For customers who have a car loan, calculate the average balance for each job category
SELECT Job, AVG(balance) as avg_balance
FROM car_insurance_data
WHERE CarLoan = 1
GROUP BY Job;

-- Identify the top 5 job categories that have the most customers with a 'default', and show their average 'balance'.
SELECT Job, COUNT(*) as total_customers, AVG(Balance)
FROM car_insurance_data
WHERE default = 1
GROUP BY Job
ORDER BY total_customers DESC
LIMIT 5;

-- ========================================================================================================================
--                                                  Problem 9: Advance Window Functions
-- ========================================================================================================================

-- Calculate the difference in 'NoOfContacts' between each customer and the customer with the next highest number of contacts in the same 'Job' category.
-- Note: Took help of AI in solving this one
SELECT 
    ID, 
    Job, 
    NoOfContacts,
    -- Get the value from the next row
    LEAD(NoOfContacts) OVER(PARTITION BY Job ORDER BY NoOfContacts ASC) as next_highest_contacts,
    -- Calculate the actual difference
    (LEAD(NoOfContacts) OVER(PARTITION BY Job ORDER BY NoOfContacts ASC) - NoOfContacts) as contact_difference
FROM car_insurance_data;

-- For each customer, calculate the difference between their 'balance' and the average 'balance' of their 'job' category.
WITH JobAverages AS (
    SELECT id, job, balance, AVG(balance * 1.0) OVER(PARTITION BY job) as avg_job_balance
    FROM car_insurance_data
)
SELECT id, job, balance, avg_job_balance, (balance - avg_job_balance) as diff_from_avg
FROM JobAverages;


-- For each 'Job' category, find the customer who had the longest call duration.
SELECT job,duration
FROM (
SELECT 
    job, duration
    ROW_NUMBER() OVER(PARTITION BY Job ORDER BY duration DESC) as rn
FROM car_insurance_data
)
WHERE rn=1;

-- Calculate the moving average of 'NoOfContacts' within each 'Job' category, using a window frame of the current row and the two preceding rows
SELECT id, job, NoOfContacts, AVG(NoOfContacts * 1.0) OVER(PARTITION BY job ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg
FROM car_insurance_data;

-- ========================================================================================================================
--                                                  Problem 10: Performance Tuning
-- ========================================================================================================================

-- creating table to store data in orc file format
create table car_insurance_data_orc
(
    ID int,
    Age int,
    Job string,
    Marital string,
    Education string,
    Default int,
    Balance int,
    HHInsurance int,
    Carloan int,
    Communication string,
    LastContactDay int,
    LastContactMonth string,
    NoOfContacts int,
    DaysPassed int,
    PrevAttempts int,
    Outcome string,
    CallStart string,
    CallEnd string,
    CarInsurance int
)
stored as orc;

-- insert into orc table
insert into car_insurance_data_orc select * from car_insurance_data;

-- creating table to store data in parquet file format
create table car_insurance_data_parquet
(
    ID int,
    Age int,
    Job string,
    Marital string,
    Education string,
    Default int,
    Balance int,
    HHInsurance int,
    Carloan int,
    Communication string,
    LastContactDay int,
    LastContactMonth string,
    NoOfContacts int,
    DaysPassed int,
    PrevAttempts int,
    Outcome string,
    CallStart string,
    CallEnd string,
    CarInsurance int
)
stored as parquet;

-- insert into orc table
insert into car_insurance_data_parquet select * from car_insurance_data;

-- ========================================================================================================================
--                                                      Finish
-- ========================================================================================================================
-- Creating Table 1

CREATE EXTERNAL TABLE campaign_events_by_os (
    campaign_id STRING,
    `date` DATE,
    `hour` INT,
    type STRING,
    value STRING,
    event STRUCT<impression: INT,click: INT,`video ad`: INT >
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/assignment_1_data/output_data_1';

-- Creating Table 2

CREATE EXTERNAL TABLE campaign_events_by_store (
    campaign_id STRING,
    `date` DATE,
    `hour` INT,
    type STRING,
    value STRING,
    event STRUCT<impression: INT,click: INT,`video ad`: INT >
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/assignment_1_data/output_data_2';


-- Creating Table 3

CREATE EXTERNAL TABLE campaign_events_by_gender (
    campaign_id STRING,
    `date` DATE,
    `hour` INT,
    type STRING,
    value STRING,
    event STRUCT<impression: INT,click: INT,`video ad`: INT >
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/assignment_1_data/output_data_3';
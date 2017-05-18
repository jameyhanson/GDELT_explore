-- Create Hive Metastore tables related to GDELT and associated
--    lookup tables

CREATE DATABASE gdelt;

CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.country_codes (
    country_code STRING,
    coutry_name STRING)
COMMENT 'CAMEO Country Codes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/data/country_codes';

CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.ethnic_codes (
    ethnic_code STRING,
    ethnicity_name STRING)
COMMENT 'CAMEO Ethnic Codes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/data/ethnic_codes';

CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.known_groups (
    group_code STRING,
    group[_name STRING)
COMMENT 'CAMEO Known Groups'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/data/known_groups';

CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.religion_codes (
    religion_code STRING,
    religion_name STRING)
COMMENT 'CAMEO Religion Codes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/data/religion_codes';

CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.type_codes (
    type_code STRING,
    type_name STRING)
COMMENT 'CAMEO Type Codes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/data/type_codes';

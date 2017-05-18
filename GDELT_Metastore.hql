-- Create Hive Metastore tables related to GDELT and associated
--    lookup tables

CREATE DATABASE IF NOT EXISTS gdelt;

CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.gdelt_events_v1 (
    GLOBALEVENTID int,
    SQLDATE int,
    MonthYear int,
    Year int,
    FractionDate float,
    Actor1Code varchar,
    Actor1Name varchar,
    Actor1CountryCode varchar,
    Actor1KnownGroupCode varchar,
    Actor1EthnicCode varchar,
    Actor1Religion1Code varchar,
    Actor1Religion2Code varchar,
    Actor1Type1Code varchar,
    Actor1Type2Code varchar,
    Actor1Type3Code varchar,
    Actor2Code varchar,
    Actor2Name varchar,
    Actor2CountryCode varchar,
    Actor2KnownGroupCode varchar,
    Actor2EthnicCode varchar,
    Actor2Religion1Code varchar,
    Actor2Religion2Code varchar,
    Actor2Type1Code varchar,
    Actor2Type2Code varchar,
    Actor2Type3Code varchar,
    IsRootEvent int,
    EventCode int,
    EventBaseCode int,
    EventRootCode int,
    QuadClass int,
    GoldsteinScale float,
    NumMentions int,
    NumSources int,
    NumArticles int,
    AvgTone float,
    Actor1Geo_Type int,
    Actor1Geo_FullName varchar,
    Actor1Geo_CountryCode varchar,
    Actor1Geo_ADM1Code varchar,
    Actor1Geo_Lat float,
    Actor1Geo_Long float,
    Actor1Geo_FeatureID varchar,
    Actor2Geo_Type int,
    Actor2Geo_FullName varchar,
    Actor2Geo_CountryCode varchar,
    Actor2Geo_ADM1Code varchar,
    Actor2Geo_Lat float,
    Actor2Geo_Long float,
    Actor2Geo_FeatureID varchar,
    ActionGeo_Type int,
    ActionGeo_FullName varchar,
    ActionGeo_CountryCode varchar,
    ActionGeo_ADM1Code varchar,
    ActionGeo_Lat float,
    ActionGeo_Long float,
    ActionGeo_FeatureID varchar,
    DATEADDED int)
COMMENT 'GDELT data, v1 schema from 1979 to March-2013'
ROW FORMAT DELIMITED
FIELD TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
STORED AS TEXTFILE
LOCATION  '/data/gdelt_v1/events';

CREATE TABLE IF NOT EXISTS gdelt.gdelt_events_v2 (
    GLOBALEVENTID int,
    SQLDATE int,
    MonthYear int,
    Year int,
    FractionDate float,
    Actor1Code varchar,
    Actor1Name varchar,
    Actor1CountryCode varchar,
    Actor1KnownGroupCode varchar,
    Actor1EthnicCode varchar,
    Actor1Religion1Code varchar,
    Actor1Religion2Code varchar,
    Actor1Type1Code varchar,
    Actor1Type2Code varchar,
    Actor1Type3Code varchar,
    Actor2Code varchar,
    Actor2Name varchar,
    Actor2CountryCode varchar,
    Actor2KnownGroupCode varchar,
    Actor2EthnicCode varchar,
    Actor2Religion1Code varchar,
    Actor2Religion2Code varchar,
    Actor2Type1Code varchar,
    Actor2Type2Code varchar,
    Actor2Type3Code varchar,
    IsRootEvent int,
    EventCode int,
    EventBaseCode int,
    EventRootCode int,
    QuadClass int,
    GoldsteinScale float,
    NumMentions int,
    NumSources int,
    NumArticles int,
    AvgTone float,
    Actor1Geo_Type int,
    Actor1Geo_FullName varchar,
    Actor1Geo_CountryCode varchar,
    Actor1Geo_ADM1Code varchar,
    Actor1Geo_Lat float,
    Actor1Geo_Long float,
    Actor1Geo_FeatureID varchar,
    Actor2Geo_Type int,
    Actor2Geo_FullName varchar,
    Actor2Geo_CountryCode varchar,
    Actor2Geo_ADM1Code varchar,
    Actor2Geo_Lat float,
    Actor2Geo_Long float,
    Actor2Geo_FeatureID varchar,
    ActionGeo_Type int,
    ActionGeo_FullName varchar,
    ActionGeo_CountryCode varchar,
    ActionGeo_ADM1Code varchar,
    ActionGeo_Lat float,
    ActionGeo_Long float,
    ActionGeo_FeatureID varchar,
    DATEADDED int,
    SOURCEURL varchar)
COMMENT 'GDELT data, v2 schema from 1-Apr-2013'
ROW FORMAT DELIMITED
FIELD TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/data/gdelt_v2/events';;

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
    group_name STRING)
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

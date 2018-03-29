-- Create Hive Metastore tables related to GDELT and associated
--    lookup tables
-- See https://www.gdeltproject.org/data.html

CREATE DATABASE IF NOT EXISTS gdelt;

CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.gdelt_events_v1 (
    GLOBALEVENTID INT,
    SQLDATE INT,
    MonthYear INT,
    Year INT,
    FractionDate FLOAT,
    Actor1Code STRING,
    Actor1Name STRING,
    Actor1CountryCode STRING,
    Actor1KnownGroupCode STRING,
    Actor1EthnicCode STRING,
    Actor1Religion1Code STRING,
    Actor1Religion2Code STRING,
    Actor1Type1Code STRING,
    Actor1Type2Code STRING,
    Actor1Type3Code STRING,
    Actor2Code STRING,
    Actor2Name STRING,
    Actor2CountryCode STRING,
    Actor2KnownGroupCode STRING,
    Actor2EthnicCode STRING,
    Actor2Religion1Code STRING,
    Actor2Religion2Code STRING,
    Actor2Type1Code STRING,
    Actor2Type2Code STRING,
    Actor2Type3Code STRING,
    IsRootEvent INT,
    EventCode INT,
    EventBaseCode INT,
    EventRootCode INT,
    QuadClass INT,
    GoldsteinScale FLOAT,
    NumMentions INT,
    NumSources INT,
    NumArticles INT,
    AvgTone FLOAT,
    Actor1Geo_Type INT,
    Actor1Geo_FullName STRING,
    Actor1Geo_CountryCode STRING,
    Actor1Geo_ADM1Code STRING,
    Actor1Geo_Lat FLOAT,
    Actor1Geo_Long FLOAT,
    Actor1Geo_FeatureID STRING,
    Actor2Geo_Type INT,
    Actor2Geo_FullName STRING,
    Actor2Geo_CountryCode STRING,
    Actor2Geo_ADM1Code STRING,
    Actor2Geo_Lat FLOAT,
    Actor2Geo_Long FLOAT,
    Actor2Geo_FeatureID STRING,
    ActionGeo_Type INT,
    ActionGeo_FullName STRING,
    ActionGeo_CountryCode STRING,
    ActionGeo_ADM1Code STRING,
    ActionGeo_Lat FLOAT,
    ActionGeo_Long FLOAT,
    ActionGeo_FeatureID STRING,
    DATEADDED INT)
COMMENT 'GDELT data, v1 schema from 1979 to March-2013'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/unencrypted/GDELT/v1/';

CREATE TABLE IF NOT EXISTS gdelt.gdelt_events_v2 (
    GLOBALEVENTID INT,
    SQLDATE INT,
    MonthYear INT,
    Year INT,
    FractionDate FLOAT,
    Actor1Code STRING,
    Actor1Name STRING,
    Actor1CountryCode STRING,
    Actor1KnownGroupCode STRING,
    Actor1EthnicCode STRING,
    Actor1Religion1Code STRING,
    Actor1Religion2Code STRING,
    Actor1Type1Code STRING,
    Actor1Type2Code STRING,
    Actor1Type3Code STRING,
    Actor2Code STRING,
    Actor2Name STRING,
    Actor2CountryCode STRING,
    Actor2KnownGroupCode STRING,
    Actor2EthnicCode STRING,
    Actor2Religion1Code STRING,
    Actor2Religion2Code STRING,
    Actor2Type1Code STRING,
    Actor2Type2Code STRING,
    Actor2Type3Code STRING,
    IsRootEvent INT,
    EventCode INT,
    EventBaseCode INT,
    EventRootCode INT,
    QuadClass INT,
    GoldsteinScale FLOAT,
    NumMentions INT,
    NumSources INT,
    NumArticles INT,
    AvgTone FLOAT,
    Actor1Geo_Type INT,
    Actor1Geo_FullName STRING,
    Actor1Geo_CountryCode STRING,
    Actor1Geo_ADM1Code STRING,
    Actor1Geo_Lat FLOAT,
    Actor1Geo_Long FLOAT,
    Actor1Geo_FeatureID STRING,
    Actor2Geo_Type INT,
    Actor2Geo_FullName STRING,
    Actor2Geo_CountryCode STRING,
    Actor2Geo_ADM1Code STRING,
    Actor2Geo_Lat FLOAT,
    Actor2Geo_Long FLOAT,
    Actor2Geo_FeatureID STRING,
    ActionGeo_Type INT,
    ActionGeo_FullName STRING,
    ActionGeo_CountryCode STRING,
    ActionGeo_ADM1Code STRING,
    ActionGeo_Lat FLOAT,
    ActionGeo_Long FLOAT,
    ActionGeo_FeatureID STRING,
    DATEADDED INT,
    SOURCEURL STRING)
COMMENT 'GDELT data, v2 schema from 1-Apr-2013'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/unencrypted/GDELT/v2/';

-- from https://www.gdeltproject.org/data/lookups/CAMEO.country.txt
-- wget -O /data01/lookup/country_codes.txt https://www.gdeltproject.org/data/lookups/CAMEO.country.txt
-- hdfs dfs -mkdir -p /unencrypted/CAMEO/country_codes
-- hdfs dfs -put /data01/lookup/country_codes.txt /unencrypted/CAMEO/country_codes/
CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.country_codes (
    country_code STRING,
    coutry_name STRING)
COMMENT 'CAMEO Country Codes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/unencrypted/CAMEO/country_codes/'
tblproperties ("skip.header.line.count"="1");

-- from https://www.gdeltproject.org/data/lookups/CAMEO.ethnic.txt
-- wget -O /data01/lookup/ethnic_codes.txt https://www.gdeltproject.org/data/lookups/CAMEO.ethnic.txt
-- hdfs dfs -mkdir -p /unencrypted/CAMEO/ethnic_codes
-- hdfs dfs -put /data01/lookup/ethnic_codes.txt /unencrypted/CAMEO/ethnic_codes/
CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.ethnic_codes (
    ethnic_code STRING,
    ethnicity_name STRING)
COMMENT 'CAMEO Ethnic Codes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/unencrypted/CAMEO/ethnic_codes/'
tblproperties ("skip.header.line.count"="1");

-- from https://www.gdeltproject.org/data/lookups/CAMEO.knowngroup.txt
-- wget -O /data01/lookup/known_groups.txt https://www.gdeltproject.org/data/lookups/CAMEO.knowngroup.txt
-- hdfs dfs -mkdir -p /unencrypted/CAMEO/known_groups
-- hdfs dfs -put /data01/lookup/known_groups.txt /unencrypted/CAMEO/known_groups/
CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.known_groups (
    group_code STRING,
    group_name STRING)
COMMENT 'CAMEO Known Groups'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/unencrypted/CAMEO/known_groups/'
tblproperties ("skip.header.line.count"="1");

-- from https://www.gdeltproject.org/data/lookups/CAMEO.religion.txt
-- wget -O /data01/lookup/religion_codes.txt https://www.gdeltproject.org/data/lookups/CAMEO.religion.txt
-- hdfs dfs -mkdir -p /unencrypted/CAMEO/religion_codes
-- hdfs dfs -put /data01/lookup/religion_codes.txt /unencrypted/CAMEO/religion_codes/
CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.religion_codes (
    religion_code STRING,
    religion_name STRING)
COMMENT 'CAMEO Religion Codes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/unencrypted/CAMEO/religion_codes/'
tblproperties ("skip.header.line.count"="1");

-- https://www.gdeltproject.org/data/lookups/CAMEO.type.txt
-- wget -O /data01/lookup/type_codes.txt https://www.gdeltproject.org/data/lookups/CAMEO.type.txt
-- hdfs dfs -mkdir -p /unencrypted/CAMEO/type_codes
-- hdfs dfs -put /data01/lookup/type_codes.txt /unencrypted/CAMEO/type_codes/
CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.type_codes (
    type_code STRING,
    type_name STRING)
COMMENT 'CAMEO Type Codes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/unencrypted/CAMEO/type_codes/'
tblproperties ("skip.header.line.count"="1");

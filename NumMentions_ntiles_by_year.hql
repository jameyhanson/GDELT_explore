-- DEFINE Quantile datafu.pig.stats.StreamingQuantile('0.0','0.05', '0.25', '0.5', '0.75', '0.9', '1.0');

CREATE TABLE IF NOT EXISTS gdelt_events_v1 (
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
COMMENT 'GDELT data, v1 schema'
ROW FORMAT DELIMITED
FIELD TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA INPATH '/data/gdelt_v1/events/*'
OVERWRITE INTO TABLE gdelt_v1;

CREATE TABLE IF NOT EXISTS gdelt_events_v2 (
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
COMMENT 'GDELT data, v2 schema'
ROW FORMAT DELIMITED
FIELD TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA INPATH '/data/gdelt_v2/events/*'
OVERWRITE INTO TABLE gdelt_v2;

-- INSERT OVERWRITE gdelt_v1_samp
-- SELECT * FROM gdelt_v1
-- TABLESAMPLE (1m ROWS) t; -- alternate syntax 1
-- TABLESAMPLE (1 PERCENT) t; -- alternate syntax 2

-- INSERT OVERWRITE gdelt_v2_samp
-- SELECT * FROM gdelt_v2
-- TABLESAMPLE (1m ROWS) t; -- alternate syntax 1
-- TABLESAMPLE (1 PERCENT) t; -- alternate syntax 2

-- https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF

INSERT OVERWRITE DIRECTORY '/user/pigtez/hive_NumMentions/'
SELECT
   MAX(gdelt_ua.NumMentions) AS min_NumMentions,
   percentile(gdelt_ua.NumMentions,0.05) AS q05, 
   percentile(gdelt_ua.NumMentions,0.25) AS q25,
   percentile(gdelt_ua.NumMentions,0.5) AS q50,
   percentile(gdelt_ua.NumMentions,0.75) AS q75,
   percentile(gdelt_ua.NumMentions,0.95) AS q95,
   MAX(gdelt_ua.NumMentions) AS max_NumMentions,
   DATEADDED/1000 AS YearAdded
FROM (
SELECT 
    GLOBALEVENTID,
    DateAdded/1000 AS YearAdded,
    NumMentions
FROM gdelt_v1
WHERE (
    GLOBALEVENTID IS NOT NULL
    AND (cast(NumMentions AS int) IS NOT NULL)
    )
UNION ALL
SELECT 
    GLOBALEVENTID,
    DATEADDED/1000 AS Year,
    NumMentions
FROM gdelt_v2
WHERE (
    GLOBALEVENTID IS NOT NULL
    AND (cast(NumMentions AS int) IS NOT NULL)
    )) gdelt_ua
GROUP BY gdelt_ua.YearAdded;

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

-- UPDATING LINE 

-- gdelt_v1 = SAMPLE gdelt_v1 0.1;
-- gdelt_v2 = SAMPLE gdelt_v2 0.1;

gdelt_v1_nums = FOREACH gdelt_v1 GENERATE 
    GLOBALEVENTID,
    Year,
    NumMentions;

gdelt_v2_nums = FOREACH gdelt_v2 GENERATE 
    GLOBALEVENTID,
    Year,
    NumMentions;

gdelt_v1 = FILTER gdelt_v1 BY (GLOBALEVENTID IS NOT NULL)
                               AND (Year IS NOT NULL)
                               AND org.apache.pig.piggybank.evaluation.IsInt(NumMentions);


gdelt_v2 = FILTER gdelt_v2 BY (GLOBALEVENTID IS NOT NULL)
                               AND (Year IS NOT NULL)
                               AND org.apache.pig.piggybank.evaluation.IsInt(NumMentions);

gdelt_nums = UNION ONSCHEMA gdelt_v1_nums, gdelt_v2_nums;

gdelt_nums_by_year = GROUP gdelt_nums BY Year;

gdelt_NumMentions_ntiles_by_year = FOREACH gdelt_nums_by_year GENERATE
    group AS year,
    Quantile(gdelt_nums.NumMentions) AS NumMentions_ntile; 
 
STORE gdelt_NumMentions_ntiles_by_year INTO 'gdelt_NumMentions_ntiles' 
   USING PigStorage('\t', '-tagsource');

-- Who writes bad stuff about the USA?
-- Approach:
--   1. Who writes about the USA?
--      w_usa_actors
--   2. How many articles to they write about the USA each month?
--      host_count_by_month
--   3. Which hosts write a lot of articles about the USA each month?
--      hosts_that_report_alot_on_USA
--   4. What is the tone of articles about the USA?
--      tone_of_articles_on_USA
--   5. Which articles about the USA have a very negative tone?
--      very_negative_records_about_usa
--   6. How many very negative tone articles about USA to they write each month?
--      host_count_very_negative_by_month
--   7. What hosts write a large fraction of their articles about the USA with a very negative tone?
--      large_fraction_negative_about_USA

-- Driving thresholds:
--     Q:  What is the aggregation interval?
--         A: YearMonth that the article was created
--     Q: What defines a host with a lot of articles about the USA?
--         A: any host with more than the median number of articles about the USA
--     Q: What defines a an article about the USA with a very negative tone?
--         A: any article with a tone more than 2-sigma below the average tone
--     Q: What defines a host that writes a lot of very negative articles about the USA?
--         A: Any host FOR WHICH more than 1/2 of their articles about about the USA
--             have a very negative tone.

-- AvgTone_ntiles_by_day.pig
-- Average tone or records in GDELT grouped by year.
-- Creates lines for:
-- +2 sigma p=0.9545
-- +1 sigma p=0.6827
-- median   p=0.5
-- -1 sigma p=0.3173
-- -2 sigma p=0.0455

-- gdelpt epoch began on 1-Jan-1979.  gdelt_epoch is since 1-Jan-1979

-- Register DataFu and define an alias for the function
-- https://datafu.incubator.apache.org/docs/datafu/guide.html

REGISTER '/opt/cloudera/parcels/CDH-5.11.0-1.cdh5.11.0.p0.34/lib/pig/datafu.jar';
DEFINE Quantile datafu.pig.stats.StreamingQuantile('0.0455', '0.3173', '0.5', '0.6827', '0.9545');

gdelt_v2 = LOAD '/data/gdelt_v2/events/' AS (
    GLOBALEVENTID:long,
    SQLDATE:chararray,      -- dates when the event occurred
    MonthYear:chararray,    -- dates when the event occurred
    Year:int,          -- dates when the event occurred
    FractionDate:float,    -- dates when the event occurred
    Actor1Code:chararray,
    Actor1Name:chararray,
    Actor1CountryCode:chararray,
    Actor1KnownGroupCode:chararray,
    Actor1EthnicCode:chararray,
    Actor1Religion1Code:chararray,
    Actor1Religion2Code:chararray,
    Actor1Type1Code:chararray,
    Actor1Type2Code:chararray,
    Actor1Type3Code:chararray,
    Actor2Code:chararray,
    Actor2Name:chararray,
    Actor2CountryCode:chararray,
    Actor2KnownGroupCode:chararray,
    Actor2EthnicCode:chararray,
    Actor2Religion1Code:chararray,
    Actor2Religion2Code:chararray,
    Actor2Type1Code:chararray,
    Actor2Type2Code:chararray,
    Actor2Type3Code:chararray,
    IsRootEvent:int,
    EventCode:int,
    EventBaseCode:int,
    EventRootCode:int,
    QuadClass:int,
    GoldsteinScale:float,
    NumMentions:int,
    NumSources:int,
    NumArticles:int,
    AvgTone:float,
    Actor1Geo_Type:int,
    Actor1Geo_FullName:chararray,
    Actor1Geo_CountryCode:chararray,
    Actor1Geo_ADM1Code:chararray,
    Actor1Geo_Lat:float,
    Actor1Geo_Long:float,
    Actor1Geo_FeatureID:chararray,
    Actor2Geo_Type:int,
    Actor2Geo_FullName:chararray,
    Actor2Geo_CountryCode:chararray,
    Actor2Geo_ADM1Code:chararray,
    Actor2Geo_Lat:float,
    Actor2Geo_Long:float,
    Actor2Geo_FeatureID:chararray,
    ActionGeo_Type:int,
    ActionGeo_FullName:chararray,
    ActionGeo_CountryCode:chararray,
    ActionGeo_ADM1Code:chararray,
    ActionGeo_Lat:float,
    ActionGeo_Long:float,
    ActionGeo_FeatureID:chararray,
    DATEADDED:chararray,    -- dates when the event was reported on
    SOURCEURL:chararray
);

gdelt_v2_sel_fields = FOREACH gdelt_v2 GENERATE 
    GLOBALEVENTID,
    ToDate(DATEADDED, 'YYYYMMDD') AS DATADDED,
    DaysBetween(ToDate(DATEADDED, 'YYYYMMDD'), ToDate('19790101, 'YYYYMMDD')) AS gdelt_epoch,
    (Actor1CountryCode IS NULL ? 'was_null': Actor1CountryCode) AS Actor1CountryCode,
    (Actor2CountryCode IS NULL ? 'was_null': Actor2CountryCode) AS Actor2CountryCode,
    AvgTone,
    SOURCEURL,
    (SOURCEURL IS NULL ? 'was_null' : org.apache.pig.piggybank.evaluation.util.apachelogparser.HostExtractor(SOURCEURL)) AS host;  
  
-- gdelt_v2_sel_fields = LIMIT gdelt_v2_sel_fields 10;
-- DUMP gdelt_v2_sel_fields;

-- DESCRIBE gdelt_v2_sel_fields;
  
-- w_usa_actors = FILTER gdelt_v2_sel_fields BY 
--    (Actor1CountryCode == 'USA' OR Actor2CountryCode == 'USA')
--    AND (AvgTone IS NOT NULL)
--    AND (host IS NOT NULL);
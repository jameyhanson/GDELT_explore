-- Who writes bad stuff about the USA?
-- Approach:
--   1. Who writes about the USA?
--      w_usa_actors
--   2. How many articles to they write about the USA each month?
--      host_count_by_month
--   3. Which hosts write more than the median number of articles about the USA each month?
--      hosts_that_report_alot_on_USA
--   4. What is the tone of articles about the USA?
--      tone_of_articles_on_USA
--   5. Which records about the USA have a very negative tone?
--      very_negative_records_about_usa

-- AvgTone_ntiles_by_day.pig
-- Average tone or records in GDELT grouped by year.
-- Creates lines for:
-- +2 sigma p=0.9545
-- +1 sigma p=0.6827
-- median   p=0.5
-- -1 sigma p=0.3173
-- -2 sigma p=0.0455

-- Register DataFu and define an alias for the function
-- https://datafu.incubator.apache.org/docs/datafu/guide.html

REGISTER '/opt/cloudera/parcels/CDH-5.11.0-1.cdh5.11.0.p0.34/lib/pig/datafu.jar';
DEFINE Quantile datafu.pig.stats.StreamingQuantile('0.0455', '0.3173', '0.5', '0.6827', '0.9545');

gdelt_v2 = LOAD '/data/gdelt_v2/events/' AS (
    GLOBALEVENTID:long,
    SQLDATE:long,      -- dates when the event occurred
    MonthYear:long,    -- dates when the event occurred
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
    DATEADDED:long,    -- dates when the event was reported on
    SOURCEURL:chararray
);

gdelt_v2_sel_fields = FOREACH gdelt_v2 GENERATE 
    GLOBALEVENTID,
    DATEADDED,
    DATEADDED/10 AS MonthYearReported,
    (Actor1CountryCode IS NULL ? 'was_null': Actor1CountryCode) AS Actor1CountryCode,
    (Actor2CountryCode IS NULL ? 'was_null': Actor2CountryCode) AS Actor2CountryCode,
    AvgTone,
    SOURCEURL,
    (SOURCEURL IS NULL ? 'was_null' : org.apache.pig.piggybank.evaluation.util.apachelogparser.HostExtractor(SOURCEURL)) AS host;  
    
w_usa_actors = FILTER gdelt_v2_sel_fields BY 
    (Actor1CountryCode == 'USA' OR Actor2CountryCode == 'USA')
    AND (AvgTone IS NOT NULL)
    AND (host IS NOT NULL);

grp_month_host = GROUP w_usa_actors BY (MonthYearReported,  host);

--  host_count_by_month :: number of records that include an American actor
--                         for each host grouped by month
host_count_by_month = FOREACH grp_month_host GENERATE 
    FLATTEN(group) AS (MonthYearReported, host),
    COUNT(w_usa_actors) AS num_records;
    
grp_host_count_by_month = GROUP host_count_by_month BY MonthYearReported;

-- host_count_by_month_ntiles: {ntiles of the number of records with an American actor
--                            echo host has by month, given that the host has one record
host_count_by_month_ntiles = FOREACH grp_host_count_by_month GENERATE
    FLATTEN(group) AS MonthYearReported,
    Quantile(host_count_by_month.num_records) AS num_records_ntile;    

-- host_count_and_ntiles_by_month: {
--     host_count_by_month::MonthYearReported: long,
-- 	   host_count_by_month::host: chararray,
-- 	   host_count_by_month::num_records: long,
-- 	   host_count_by_month_ntiles::MonthYearReported: long,
-- 	   host_count_by_month_ntiles::num_records_ntile:(
-- 	       quantile_0_0455: double,
-- 		   quantile_0_3173: double,
-- 		   quantile_0_5: double,
-- 		   quantile_0_6827: double,
-- 		   quantile_0_9545: double
-- 	   )
-- }
host_count_and_ntiles_by_month = JOIN host_count_by_month BY MonthYearReported,
    host_count_by_month_ntiles BY MonthYearReported;
    
hosts_that_report_alot_on_USA = FILTER host_count_and_ntiles_by_month BY 
    host_count_by_month::num_records >= host_count_by_month_ntiles::num_records_ntile.quantile_0_5;

-- hosts_that_report_alot_on_USA: {
--     host_count_by_month::MonthYearReported: long,
--      host_count_by_month::host: chararray,
--       host_count_by_month::num_records: long,
--       host_count_by_month_ntiles::MonthYearReported: long,
--       host_count_by_month_ntiles::num_records_ntile: (
--           quantile_0_0455: double,
--           quantile_0_3173: double,
--           quantile_0_5: double,
--           quantile_0_6827: double,
--           quantile_0_9545: double
--       )
-- }

AvgTone_about_USA_by_month = GROUP w_usa_actors BY MonthYearReported;

AvgTone_about_USA_by_month_ntiles = FOREACH AvgTone_about_USA_by_month GENERATE
    group AS MonthYearReported,
    Quantile(w_usa_actors.AvgTone) as AvgTone_ntile;

w_usa_plus_AvgTone_monthly_ntiles = JOIN AvgTone_about_USA_by_month_ntiles BY MonthYearReported,
   w_usa_actors BY MonthYearReported;
    
very_negative_records_about_USA = FILTER w_usa_plus_AvgTone_monthly_ntiles BY
    w_usa_actors::AvgTone <= AvgTone_about_USA_by_month_ntiles::AvgTone_ntile.quantile_0_0455;
    
very_negative_records_about_USA = LIMIT very_negative_records_about_USA 100;
DUMP very_negative_records_about_USA;
DESCRIBE very_negative_records_about_USA;
    

-- Who writes bad stuff about the USA?
-- Approach:
--   1. Who creates records with USA actors?
--      w_usa_actors
--   2. How many records with USA actors does a host create each week?
--      host_records_by_week
--   3. Which hosts write a lot of articles about the USA each month?
--      hosts_that_report_alot_on_USA
--   4. What is the tone of records about the USA?
--      tone_of_articles_on_USA
--   5. Which articles about the USA have a very negative tone?
--      very_negative_records_about_usa
--   6. How many very negative tone articles about USA to they write each month?
--      host_count_very_negative_by_month
--   7. What hosts write a large fraction of their articles about the USA with a very negative tone?
--      large_fraction_negative_about_USA

-- Driving thresholds:
--     Q:  What is the aggregation interval?
--         A: epoch_week the article was created
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
    DaysBetween(ToDate(DATEADDED, 'YYYYMMDD'), ToDate('19790101', 'YYYYMMDD'))%7+1 AS day_added,  -- Sun = 0
    DaysBetween(ToDate(DATEADDED, 'YYYYMMDD'), ToDate('19790101', 'YYYYMMDD')) AS gdelt_epoch_day,
    DaysBetween(ToDate(DATEADDED, 'YYYYMMDD'), ToDate('19790101', 'YYYYMMDD'))/7 AS gdelt_epoch_week,
    (Actor1CountryCode IS NULL ? 'was_null': Actor1CountryCode) AS Actor1CountryCode,
    (Actor2CountryCode IS NULL ? 'was_null': Actor2CountryCode) AS Actor2CountryCode,
    AvgTone,
    (SOURCEURL IS NULL ? 'was_null' : org.apache.pig.piggybank.evaluation.util.apachelogparser.HostExtractor(SOURCEURL)) AS host,
    SOURCEURL;

-- ##### Which hosts report on the USA frequently? #####
-- Records that include at least one actor from USA
w_usa_actors = FILTER gdelt_v2_sel_fields BY 
   (Actor1CountryCode == 'USA' OR Actor2CountryCode == 'USA')
   AND (AvgTone IS NOT NULL)
   AND (host IS NOT NULL);

grp_week_host = GROUP w_usa_actors BY (gdelt_epoch_week, host);

host_records_by_week = FOREACH grp_week_host GENERATE
    FLATTEN(group) AS (gdelt_epoch_week, host),
    COUNT(w_usa_actors) AS num_records;
    
grp_host_records_by_week = GROUP host_records_by_week BY gdelt_epoch_week;

host_records_by_week_ntiles = FOREACH grp_host_records_by_week GENERATE
    FLATTEN(group) AS gdelt_epoch_week,
    Quantile(host_records_by_week.num_records) AS num_records_ntile;
    
-- host_records_and_ntiles_by_week: {
--     host_records_by_week::gdelt_epoch_week: long,
--     host_records_by_week::host: chararray,
--     host_records_by_week::num_records: long,
--     host_records_by_week_ntiles::gdelt_epoch_week: long,
--     host_records_by_week_ntiles::num_records_ntile: (
--         quantile_0_0455: double,
--         quantile_0_3173: double,
--         quantile_0_5: double,
--         quantile_0_6827: double,
--         quantile_0_9545: double
--     )
-- }
host_records_and_ntiles_by_week = JOIN
    host_records_by_week BY gdelt_epoch_week,
    host_records_by_week_ntiles BY gdelt_epoch_week;
    
hosts_that_report_alot_on_USA = FILTER host_records_and_ntiles_by_week BY
   host_records_by_week::num_records >= host_records_by_week_ntiles::num_records_ntile.quantile_0_3173;
    
-- ##### What is the AvgTone of records on the USA? #####
AvgTone_about_USA_by_week = GROUP w_usa_actors BY gdelt_epoch_week;

AvgTone_about_USA_by_week_ntiles = FOREACH AvgTone_about_USA_by_week GENERATE
    FLATTEN(group) AS gdelt_epoch_week,
    Quantile(w_usa_actors.AvgTone) AS AvgTone_ntile;

w_usa_AvgTone_and_ntiles_by_week = JOIN
    AvgTone_about_USA_by_week_ntiles BY gdelt_epoch_week,
    w_usa_actors BY gdelt_epoch_week;
    
hosts_that_report_alot_on_USA = LIMIT hosts_that_report_alot_on_USA 10;
DUMP hosts_that_report_alot_on_USA;
DESCRIBE hosts_that_report_alot_on_USA;

-- w_usa_AvgTone_and_ntiles_by_week: {
--     AvgTone_about_USA_by_week_ntiles::gdelt_epoch_week: long,
--     AvgTone_about_USA_by_week_ntiles::AvgTone_ntile: (
--         quantile_0_0455: double,
--         quantile_0_3173: double,
--         quantile_0_5: double,
--         quantile_0_6827: double,
--         quantile_0_9545: double
--     ),
--     w_usa_actors::GLOBALEVENTID: long,
--     w_usa_actors::DATEDDED: datetime,
--     w_usa_actors::day_added: long,
--     w_usa_actors::gdelt_epoch_day: long,
--     w_usa_actors::gdelt_epoch_week: long,
--     w_usa_actors::Actor1CountryCode: chararray,
--     w_usa_actors::Actor2CountryCode: chararray,
--     w_usa_actors::AvgTone: float,
--     w_usa_actors::host: chararray,
--     w_usa_actors::SOURCEURL: chararray
-- }

very_negative_tone_about_USA = FILTER w_usa_AvgTone_and_ntiles_by_week BY
    w_usa_actors::AvgTone <= AvgTone_about_USA_by_week_ntiles::AvgTone_ntile.quantile_0_0455; -- AvgTone_ntile minus2sigma
    
very_negative_tone_about_USA_by_week = GROUP very_negative_tone_about_USA BY (
    w_usa_actors::gdelt_epoch_week,
    w_usa_actors::host);
    
host_count_of_very_negative_by_week = FOREACH very_negative_tone_about_USA_by_week GENERATE
    FLATTEN(group) AS (gdelt_epoch_week, host),
    COUNT(very_negative_tone_about_USA) AS num_very_negative_records;
   
join_host_counts_by_week = JOIN
    host_records_by_week BY (gdelt_epoch_week, host),
    host_count_of_very_negative_by_week BY (gdelt_epoch_week, host);
    
fraction_of_very_negative_by_week = FOREACH join_host_counts_by_week GENERATE
    host_records_by_week::gdelt_epoch_week AS gdelt_epoch_week,
    host_records_by_week::host AS host,
    host_records_by_week::num_records AS total_num_records,
    host_count_of_very_negative_by_week::num_very_negative_records,
    (float)host_count_of_very_negative_by_week::num_very_negative_records/host_records_by_week::num_records AS fraction_of_very_negative;

hosts_with_lots_of_very_negative_by_week = FILTER fraction_of_very_negative_by_week BY
    fraction_of_very_negative >= 0.5;

STORE hosts_with_lots_of_very_negative_by_week INTO '/results/hosts_with_lots_of_very_negative_by_week';

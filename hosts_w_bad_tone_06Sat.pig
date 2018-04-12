-- Hosts with writing articles with bad tones about the USA actors
-- weekly moving averages for the 7 days before each Saturday

REGISTER '/usr/lib/pig/piggybank.jar';
REGISTER '/opt/cloudera/parcels/CDH-5.12.2-1.cdh5.12.2.p0.4/lib/pig/datafu.jar';
DEFINE Quantile datafu.pig.stats.StreamingQuantile('0.0455', '0.3173', '0.5', '0.6827', '0.9545');

REGISTER 'top_level_domain.py' using jython as example_udf;

gdelt_v2 = LOAD '$V2_DATA_DIR' AS (
    GLOBALEVENTID:long,
    SQLDATE:chararray,      -- dates when the event occurred
    MonthYear:chararray,    -- dates when the event occurred
    Year:int,               -- dates when the event occurred
    FractionDate:float,     -- dates when the event occurred
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

w_usa_actors = FILTER gdelt_v2 BY 
    AvgTone IS NOT NULL AND
    SOURCEURL IS NOT NULL AND
    org.apache.pig.piggybank.evaluation.util.apachelogparser.HostExtractor(SOURCEURL) IS NOT NULL AND
    (
        Actor1CountryCode == 'USA'
        OR Actor2CountryCode == 'USA'
    ); 

w_usa_actors_sel_fields = FOREACH w_usa_actors GENERATE 
    GLOBALEVENTID,
    ToDate(DATEADDED, 'YYYYMMdd') AS DATEADDED,
    ToDate('1979-01-01') AS epoch_start,
    DaysBetween(ToDate(DATEADDED, 'YYYYMMdd'), ToDate('1979-01-01')) AS epoch_days,
    AvgTone,
    org.apache.pig.piggybank.evaluation.util.apachelogparser.HostExtractor(SOURCEURL) AS host,
    SOURCEURL;
    
w_usa_actors = FOREACH w_usa_actors_sel_fields GENERATE 
    GLOBALEVENTID,
    DATEADDED,
    epoch_start,
    epoch_days,
    epoch_days%7 + 1 AS weekday,
--    CONCAT('P', (chararray)(((epoch_days-0)/7+1)*7+0), 'D') AS ew_offset_mon,
--    CONCAT('P', (chararray)(((epoch_days-1)/7+1)*7+1), 'D') AS ew_offset_tue,
--    CONCAT('P', (chararray)(((epoch_days-2)/7+1)*7+2), 'D') AS ew_offset_wed,
--    CONCAT('P', (chararray)(((epoch_days-3)/7+1)*7+3), 'D') AS ew_offset_thu,
--    CONCAT('P', (chararray)(((epoch_days-4)/7+1)*7+4), 'D') AS ew_offset_fri,
    CONCAT('P', (chararray)(((epoch_days-5)/7+1)*7+5), 'D') AS ew_offset_sat,
--    CONCAT('P', (chararray)(((epoch_days-6)/7+1)*7+6), 'D') AS ew_offset_sun,
    AvgTone,
    host,
    example_udf.tld(host) AS tld,
    SOURCEURL;         
    
w_usa_actors = FOREACH w_usa_actors GENERATE 
    GLOBALEVENTID,
    DATEADDED,
    epoch_days%7 + 1 AS weekday,
--    AddDuration(epoch_start, ew_offset_mon) AS ew_date_mon,
--    AddDuration(epoch_start, ew_offset_tue) AS ew_date_tue,
--    AddDuration(epoch_start, ew_offset_wed) AS ew_date_wed,
--    AddDuration(epoch_start, ew_offset_thu) AS ew_date_thu,
--    AddDuration(epoch_start, ew_offset_fri) AS ew_date_fri,
    AddDuration(epoch_start, ew_offset_sat) AS ew_date_sat,
--    AddDuration(epoch_start, ew_offset_sun) AS ew_date_sun,
    AvgTone,
    host,
    tld,
    SOURCEURL;      

-- &&&&& Saturdays code &&&&&
-- ##### Which hosts report on the USA frequently? #####
-- Records that include at least one actor from USA

grp_week_host = GROUP w_usa_actors BY (ew_date_sat, host, tld);

host_records_by_week = FOREACH grp_week_host GENERATE
    FLATTEN(group) AS (ew_date_sat, host, tld),
    COUNT(w_usa_actors) AS num_records;
    
grp_host_records_by_week = GROUP host_records_by_week BY (ew_date_sat);

host_records_by_week_ntiles = FOREACH grp_host_records_by_week GENERATE
    FLATTEN(group) AS (ew_date_sat),
    Quantile(host_records_by_week.num_records) AS num_records_ntile;
    
host_records_and_ntiles_by_week = JOIN
    host_records_by_week BY ew_date_sat,
    host_records_by_week_ntiles BY ew_date_sat;
    
hosts_that_report_alot_on_USA = FILTER host_records_and_ntiles_by_week BY
   host_records_by_week::num_records >= host_records_by_week_ntiles::num_records_ntile.quantile_0_3173;
   
hosts_that_report_alot_on_USA = FOREACH hosts_that_report_alot_on_USA GENERATE
    host_records_by_week::ew_date_sat AS ew_date_sat,
    host_records_by_week::host AS host,
    host_records_by_week::tld AS tld,
    host_records_by_week::num_records AS num_records,
    host_records_by_week_ntiles::num_records_ntile AS num_records_ntile;
    
-- ##### What is the AvgTone of records on the USA? #####
AvgTone_about_USA_by_week = GROUP w_usa_actors BY ew_date_sat;

AvgTone_about_USA_by_week_ntiles = FOREACH AvgTone_about_USA_by_week GENERATE
    FLATTEN(group) AS ew_date_sat,
    Quantile(w_usa_actors.AvgTone) AS AvgTone_ntile;

w_usa_AvgTone_and_ntiles_by_week = JOIN
    AvgTone_about_USA_by_week_ntiles BY ew_date_sat,
    w_usa_actors BY ew_date_sat;

very_negative_tone_about_USA = FILTER w_usa_AvgTone_and_ntiles_by_week BY
    w_usa_actors::AvgTone <= AvgTone_about_USA_by_week_ntiles::AvgTone_ntile.quantile_0_0455; -- AvgTone_ntile minus2sigma
  
very_negative_tone_about_USA_by_week = GROUP very_negative_tone_about_USA BY (
    w_usa_actors::ew_date_sat,
    w_usa_actors::host,
    w_usa_actors::tld);
    
host_count_of_very_negative_by_week = FOREACH very_negative_tone_about_USA_by_week GENERATE
    FLATTEN(group) AS (w_usa_actors::ew_date_sat, w_usa_actors::host, w_usa_actors::tld),
    COUNT(very_negative_tone_about_USA) AS num_very_negative_records;
   
join_host_counts_by_week = JOIN
    hosts_that_report_alot_on_USA BY (ew_date_sat, host),
    host_count_of_very_negative_by_week BY (ew_date_sat, host);
    
fraction_of_very_negative_by_week = FOREACH join_host_counts_by_week GENERATE
    hosts_that_report_alot_on_USA::ew_date_sat AS ew_date_sat,
    hosts_that_report_alot_on_USA::host AS host,
    hosts_that_report_alot_on_USA::tld AS tld,
    host_count_of_very_negative_by_week::num_very_negative_records AS num_very_negative_records,    
    hosts_that_report_alot_on_USA::num_records AS total_num_records,
    (float)host_count_of_very_negative_by_week::num_very_negative_records/hosts_that_report_alot_on_USA::num_records AS fraction_of_very_negative;

hosts_with_lots_of_very_negative_by_week = FILTER fraction_of_very_negative_by_week BY
    fraction_of_very_negative >= 0.5
    AND tld != 'com'
    AND tld != 'org'
    AND tld != 'net';

STORE hosts_with_lots_of_very_negative_by_week INTO '$RESULTS_BASE_DIR/hosts_with_lots_of_very_negative/06Sat_results';

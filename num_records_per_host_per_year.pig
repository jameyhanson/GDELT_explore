-- File: num_records_per_host_per_year.pig
-- Use gdelt_v2, which has SOURCEURL

-- Generate ntiles for number of records records a host appears in for each month

-- Register DataFu and define an alias for the function
-- https://datafu.incubator.apache.org/docs/datafu/guide.html

REGISTER '/opt/cloudera/parcels/CDH-5.11.0-1.cdh5.11.0.p0.34/lib/pig/datafu.jar';
DEFINE Quantile datafu.pig.stats.StreamingQuantile('0.0455', '0.3173', '0.5', '0.6827', '0.9545');

-- only gdelt_v2 data includes SOURCEURL
gdelt = LOAD '/data/gdelt_v2/events/' AS (
    GLOBALEVENTID:long,
    SQLDATE:long,
    MonthYear:long,
    Year:int,
    FractionDate:float,
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
    DATEADDED:long,
    SOURCEURL:chararray
);

gdelt = FILTER gdelt BY (SOURCEURL IS NOT NULL);

gdelt_limited_cols = FOREACH gdelt GENERATE 
    GLOBALEVENTID,
    MonthYear,
    org.apache.pig.piggybank.evaluation.util.apachelogparser.HostExtractor(SOURCEURL) AS host;
        
by_month_host = GROUP gdelt_limited_cols BY (MonthYear, host);

by_month_host_count = FOREACH by_month_host GENERATE
    FLATTEN(group) AS (MonthYear, host),
    COUNT(gdelt_limited_cols.host) AS num_hosts;
    
by_month_host_count = FOREACH by_month_host_count GENERATE 
  $0 as MonthYear,
  $1 AS host,
  $2 AS host_count;
  
top_hosts_by_month = FILTER by_month_host_count BY
    host_count >= 10);
    
STORE top_hosts_by_month INTO '/results/top_hosts_by_month'
    USING PigStorage('\t', '-tagsource');
    
host_count_ntiles_by_month = FOREACH by_month_host_count GENERATE
   group AS month,
   Quantiles(by_month_host_count.host_count) AS HostCount_ntile;

gdelt_
   
 
DUMP gdelt_by_month_host_count;


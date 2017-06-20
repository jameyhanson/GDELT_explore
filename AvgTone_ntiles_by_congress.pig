-- GDETL AvgTone, grouped by congress number
--     https://www.senate.gov/reference/Years_to_Congress.htm
--     CongressNum = (Year + 1)/2 - 894 with integer division

-- Register DataFu and define an alias for the function
-- https://datafu.incubator.apache.org/docs/datafu/guide.html

REGISTER '/opt/cloudera/parcels/CDH-5.11.0-1.cdh5.11.0.p0.34/lib/pig/datafu.jar';
DEFINE DIST datafu.pig.geo.HaversineDistInMiles;
DEFINE Quantile datafu.pig.stats.StreamingQuantile('0.0455', '0.3173', '0.5', '0.6827', '0.9545');

-- gdelt_v1 = LOAD '/data/gdelt_v1/events/19*.csv' AS (
gdelt_v1 = LOAD '/data/gdelt_v1/events/' AS (
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
    DATEADDED:long
);

-- gdelt_v2 = LOAD '/data/gdelt_v2/events/20?????1.export.csv' AS (
gdelt_v2 = LOAD '/data/gdelt_v2/events/' AS (
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

gdelt_v1_nums = FOREACH gdelt_v1 GENERATE 
    GLOBALEVENTID,
    DATEADDED,
    DATEADDED/10 AS MonthYearAdded,
    (Year + 1)/2 - 894 AS CongressNum,
    AvgTone;

gdelt_v2_nums = FOREACH gdelt_v2 GENERATE 
    GLOBALEVENTID,
    DATEADDED,
    DATEADDED/10 AS MonthYearAdded,
    (DATEADDED/10000 + 1)/2 - 894 AS CongressNum,
    AvgTone;

gdelt_v1 = FILTER gdelt_v1 BY AvgTone IS NOT NULL;

gdelt_v2 = FILTER gdelt_v2 BY AvgTone IS NOT NULL;

gdelt_nums = UNION ONSCHEMA gdelt_v1_nums, gdelt_v2_nums;

gdelt_nums_by_congress = GROUP gdelt_nums BY CongressNum;

gdelt_AvgTone_ntiles_by_congress = FOREACH gdelt_nums_by_congress GENERATE
    group AS CongressNum,
    Quantile(gdelt_nums.AvgTone) AS AvgTone_ntile; 
 
gdelt_AvgTone_flat_ntiles_by_congress = FOREACH gdelt_AvgTone_ntiles_by_congress GENERATE
    CongressNum,
    AvgTone_ntile.$0 AS minus2sigma,
    AvgTone_ntile.$1 AS minus1sigmma,
    AvgTone_ntile.$2 AS median,
    AvgTone_ntile.$3 AS plus1sigma,
    AvgTone_ntile.$4 AS plus2sigma;
    
STORE gdelt_AvgTone_flat_ntiles_by_congress INTO '/results/gdelt_AvgTone_ntiles_by_congress'
    USING PigStorage('\t', '-tagsource');

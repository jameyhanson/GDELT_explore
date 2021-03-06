-- Find record where Actor1 and Actor2 are the farthest apart
-- Run DataFu.HaversineDistInMiles on a sample of a subset of records

-- Register DataFu and define an alias for the function
REGISTER '/opt/cloudera/parcels/CDH-5.12.2-1.cdh5.12.2.p0.4/lib/pig/datafu.jar';
DEFINE DIST datafu.pig.geo.HaversineDistInMiles;

gdelt = LOAD '/unencrypted/GDELT/v2/' AS (
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
    Actor1Geo_Lat:double,
    Actor1Geo_Long:double,
    Actor1Geo_FeatureID:chararray,
    Actor2Geo_Type:int,
    Actor2Geo_FullName:chararray,
    Actor2Geo_CountryCode:chararray,
    Actor2Geo_ADM1Code:chararray,
    Actor2Geo_Lat:double,
    Actor2Geo_Long:double,
    Actor2Geo_FeatureID:chararray,
    ActionGeo_Type:int,
    ActionGeo_FullName:chararray,
    ActionGeo_CountryCode:chararray,
    ActionGeo_ADM1Code:chararray,
    ActionGeo_Lat:double,
    ActionGeo_Long:double,
    ActionGeo_FeatureID:chararray,
    DATEADDED:long,
    SOURCEURL:chararray
);

gdelt_w_locs = FILTER gdelt BY (Actor1Geo_Lat IS NOT NULL) 
                               AND (Actor1Geo_Long IS NOT NULL)
                               AND (Actor2Geo_Lat IS NOT NULL)
                               AND (Actor2Geo_Long IS NOT NULL);

gdelt_w_locs = SAMPLE gdelt_w_locs 0.01;

milesapart = FOREACH gdelt_w_locs GENERATE 
                    Actor1Name,
                    Actor1CountryCode,
                    Actor2Name,
                    Actor2CountryCode,
                    GoldsteinScale,
                    NumMentions,
                    NumSources,
                    NumArticles,
                    Actor1Geo_FullName,
                    Actor2Geo_FullName,
                    DIST(Actor1Geo_Lat, Actor1Geo_Long,
                        Actor2Geo_Lat, Actor2Geo_Long) AS milesapart;
                        
mostmilesapart = ORDER milesapart BY milesapart DESC;

farthest_apart = LIMIT mostmilesapart 10;

DUMP farthest_apart;                        

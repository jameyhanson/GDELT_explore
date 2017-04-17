-- Register DataFu and define an alias for the function
REGISTER '/opt/cloudera/parcels/CDH-5.10.0-1.cdh5.10.0.p0.41/lib/pig/datafu.jar';
DEFINE DIST datafu.pig.geo.HaversineDistInMiles;

gdelt = LOAD '/Data/GDELT/201704*.export.csv' AS (
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
    ActionGeo_Lat:flaot,
    ActionGeo_Long:float,
    ActionGeo_FeatureID:chararray,
    DATEADDED:long,
    SOURCEURL:url
);

gdelt_part = SAMPLE gdelt 0.01;

miles2atwaters = FOREACH gdelt_part GENERATE 
                    Actor1Name,
                    Actor1CountryCode,
                    Actor2Name,
                    Actor2CountryCode,
                    GoldsteinScale,
                    NumMentions,
                    NumSources,
                    NumArticle,
                    Actor1Geo_FullName,
                    Actor2Geo_FullName,
                    DIST(39.364243, -76.608669,
                        Actor1Geo_Lat, Actor1Geo_Long) AS miles2atwaters;
                        
mostmiles2atwaters = ORDER miles2atwaters BY miles2atwaters DESC;

farthest_events = LIMIT mostmiles2atwaters 10;

DUMP farthest_events;                        

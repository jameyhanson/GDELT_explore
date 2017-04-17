-- Register DataFu and define an alias for the function
REGISTER '/opt/cloudera/parcels/CDH-5.10.0-1.cdh5.10.0.p0.41/lib/pig/datafu.jar';
DEFINE DIST datafu.pig.geo.HaversineDistInMiles;

gdelt = LOAD '/Data/GDELT/201704*.export.csv' AS (
    GLOBALEVENTID:long,
    SQLDATE:long,
    MonthYear:long,
    Year:int,
    FractionDate:float,
    Actor1Code:string,
    Actor1Name:string,
    Actor1CountryCode:string,
    Actor1KnownGroupCode:string,
    Actor1EthnicCode:string,
    Actor1Religion1Code:string,
    Actor1Religion2Code:string,
    Actor1Type1Code:string,
    Actor1Type2Code:string,
    Actor1Type3Code:string,
    Actor2Code:string,
    Actor2Name:string,
    Actor2CountryCode:string,
    Actor2KnownGroupCode:string,
    Actor2EthnicCode:string,
    Actor2Religion1Code:string,
    Actor2Religion2Code:string,
    Actor2Type1Code:string,
    Actor2Type2Code:string,
    Actor2Type3Code:string,
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
    Actor1Geo_FullName:string,
    Actor1Geo_CountryCode:string,
    Actor1Geo_ADM1Code:string,
    Actor1Geo_Lat:float,
    Actor1Geo_Long:float,
    Actor1Geo_FeatureID:string,
    Actor2Geo_Type:int,
    Actor2Geo_FullName:string,
    Actor2Geo_CountryCode:string,
    Actor2Geo_ADM1Code:string,
    Actor2Geo_Lat:float,
    Actor2Geo_Long:float,
    Actor2Geo_FeatureID:string,
    ActionGeo_Type:int,
    ActionGeo_FullName:string,
    ActionGeo_CountryCode:string,
    ActionGeo_ADM1Code:string,
    ActionGeo_Lat:flaot,
    ActionGeo_Long:float,
    ActionGeo_FeatureID:string,
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

-- Register DataFu and define an alias for the function
-- https://datafu.incubator.apache.org/docs/datafu/guide.html

REGISTER '/opt/cloudera/parcels/CDH-5.12.2-1.cdh5.12.2.p0.4/lib/pig/datafu.jar';
DEFINE DIST datafu.pig.geo.HaversineDistInMiles;
DEFINE Quantile datafu.pig.stats.StreamingQuantile('0.0', '0.05', '0.25', '0.5', '0.75', '0.9', '1.0');

gdelt_v1 = LOAD '$V1_DATA_DIR' AS (
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

gdelt_v2 = LOAD '$V2_DATA_DIR' AS (
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
    DATEADDED/1000 AS YearAdded,
    NumMentions;

gdelt_v2_nums = FOREACH gdelt_v2 GENERATE 
    GLOBALEVENTID,
    DATEADDED/1000 AS YearAdded,
    NumMentions;

gdelt_v1_nums = FILTER gdelt_v1_nums BY (GLOBALEVENTID IS NOT NULL)
                               AND org.apache.pig.piggybank.evaluation.IsInt(NumMentions);


gdelt_v2_nums = FILTER gdelt_v2_nums BY (GLOBALEVENTID IS NOT NULL)
                               AND org.apache.pig.piggybank.evaluation.IsInt(NumMentions);

gdelt_nums = UNION ONSCHEMA gdelt_v1_nums, gdelt_v2_nums;

gdelt_nums_by_year = GROUP gdelt_nums BY YearAdded;

gdelt_NumMentions_ntiles_by_year = FOREACH gdelt_nums_by_year GENERATE
    group AS YearAdded,
    Quantile(gdelt_nums.NumMentions) AS NumMentions_ntile; 
   
gdelt_NumMentions_flat_ntiles_by_year = FOREACH gdelt_NumMentions_ntiles_by_year GENERATE
    YearAdded,
    NumMentions_ntile.$0 AS min,
    NumMentions_ntile.$1 AS q05,
    NumMentions_ntile.$2 AS q25,
    NumMentions_ntile.$3 AS median,
    NumMentions_ntile.$4 AS q75,
    NumMentions_ntile.$5 AS q95,
    NumMentions_ntile.$6 AS max;
    
STORE gdelt_NumMentions_flat_ntiles_by_year INTO '/results/gdelt_NumMentions_ntiles_by_year'
    USING PigStorage('\t', '-tagsource');   

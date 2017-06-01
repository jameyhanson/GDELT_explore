-- gew_date = gdelt epoch week.  The date following the week of aggregation

gdelt_v2 = LOAD '/data/gdelt_v2/events/' AS (
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

gdelt_v2_sel_fields = FOREACH gdelt_v2 GENERATE 
    GLOBALEVENTID,
    ToDate(DATEADDED, 'YYYYMMDD') AS DATEADDED,
    DaysBetween(ToDate(DATEADDED, 'YYYYMMDD'), ToDate('19790101', 'YYYYMMDD'))%7+1 AS day_added,  -- Monday = 1, per ISO8601
    DaysBetween(ToDate(DATEADDED, 'YYYYMMDD'), ToDate('19790101', 'YYYYMMDD')) AS gdelt_epoch_day,
    DaysBetween(ToDate(DATEADDED, 'YYYYMMDD'), ToDate('19790101', 'YYYYMMDD'))/7 AS gdelt_epoch_week,
    (Actor1CountryCode IS NULL ? 'was_null': Actor1CountryCode) AS Actor1CountryCode,
    (Actor2CountryCode IS NULL ? 'was_null': Actor2CountryCode) AS Actor2CountryCode,
    AvgTone,
    (SOURCEURL IS NULL ? 'was_null' : org.apache.pig.piggybank.evaluation.util.apachelogparser.HostExtractor(SOURCEURL)) AS host,
    SOURCEURL;
    
-- gew_end (gdelt epoch week) is the day after then end of the week, Monday.
--     the gdelt_epoch_week is the 7 days preceeding gew_end
gdelt_v2_sel_fields = FOREACH gdelt_v2_sel_fields GENERATE
    GLOBALEVENTID,
    DATEADDED,
    day_added,
    gdelt_epoch_day,
    gdelt_epoch_week,
    AddDuration(ToDate('19790108', 'YYYYMMDD'), CONCAT('P', (chararray)(gdelt_epoch_week*7), 'D')) AS gew_end,
    host;

gdelt_v2_sel_fields = LIMIT gdelt_v2_sel_fields 50;
DUMP gdelt_v2_sel_fields;
DESCRIBE gdelt_v2_sel_fields;

-- gew_date = gdelt epoch week.  The date following the week of aggregation

raw_date = LOAD '/data/date_test/' AS (
    DATEADDED:chararray    -- dates when the event was reported on
);

date_cols = FOREACH raw_date GENERATE 
    ToDate(DATEADDED, 'YYYYMMDD') AS DATEADDED,
    ;
    
date_cols = FOREACH date_cols GENERATE
    DATEADDED,
    ToDate('1979-01-01') AS start_of_epoch;
    
date_cols = LIMIT date_cols 50;
DUMP date_cols;
DESCRIBE date_cols;

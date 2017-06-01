-- gew_date = gdelt epoch week.  The date following the week of aggregation

raw_date = LOAD '/data/date_test/' AS (
    DATEADDED:chararray    -- dates when the event was reported on
);

date_cols = FOREACH raw_date GENERATE 
    ToDate(DATEADDED, 'YYYYMMDD') AS DATEADDED,
    DaysBetween(ToDate(DATEADDED, 'YYYYMMDD'), ToDate('1979-01-01')) AS epoch_days;
       
date_cols = FOREACH date_cols GENERATE
    DATEADDED,
    epoch_days,
    epoch_days%7 + 1 AS weekday,
    ((epoch_days-0)/7+1)*7+0 AS ew_offest;
    
date_cols = LIMIT date_cols 50;
DUMP date_cols;
DESCRIBE date_cols;

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
    ((epoch_days-0)/7+1)*7+0 AS ew_offest_a,
    ((epoch_days-1)/7+1)*7+1 AS ew_offest_b,
    ((epoch_days-2)/7+1)*7+2 AS ew_offest_c,
    ((epoch_days-3)/7+1)*7+3 AS ew_offest_d,
    ((epoch_days-4)/7+1)*7+4 AS ew_offest_e,
    ((epoch_days-5)/7+1)*7+5 AS ew_offest_f,
    ((epoch_days-6)/7+1)*7+6 AS ew_offest_g;
    
date_cols = LIMIT date_cols 50;
DUMP date_cols;
DESCRIBE date_cols;

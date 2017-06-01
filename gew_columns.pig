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
    CONCAT('P', (chararray)(((epoch_days-0)/7+1)*7+0), 'D') AS ew_offet_a,
    CONCAT('P', (chararray)(((epoch_days-1)/7+1)*7+1), 'D') AS ew_offet_b,
    CONCAT('P', (chararray)(((epoch_days-2)/7+1)*7+2), 'D') AS ew_offet_c,
    CONCAT('P', (chararray)(((epoch_days-3)/7+1)*7+3), 'D') AS ew_offet_d,
    CONCAT('P', (chararray)(((epoch_days-4)/7+1)*7+4), 'D') AS ew_offet_e,
    CONCAT('P', (chararray)(((epoch_days-5)/7+1)*7+5), 'D') AS ew_offet_f,
    CONCAT('P', (chararray)(((epoch_days-6)/7+1)*7+6), 'D') AS ew_offet_g';
        
date_cols = FOREACH date_cols GENERATE
    DATEADDED,
    weekday,
    AddDuration(DATEADDED, ew_offset_a) AS ew_date_a;
    
date_cols = LIMIT date_cols 50;
DUMP date_cols;
DESCRIBE date_cols;

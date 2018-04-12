-- Hosts with writing articles with bad tones about the USA actors
-- weekly moving averages for the 7 days before each Sunday
-- collect and sort final results

very_negative_hosts_by_moving_week_avg = LOAD '$RESULTS_BASE_DIR/hosts_with_lots_of_very_negative' AS (
    week_ended_date:chararray,
    host:chararray,
    tld:chararray,
    num_very_negative_records:int,    
    total_num_records:int,
    fraction_of_very_negative:float
);	

very_negative_hosts_by_moving_week_avg = FOREACH very_negative_hosts_by_moving_week_avg GENERATE 
    ToDate(week_ended_date) AS week_ended_date,
    host,
    tld,
    num_very_negative_records,    
    total_num_records,
    fraction_of_very_negative
;

very_negative_hosts_by_moving_week_avg = ORDER very_negative_hosts_by_moving_week_avg BY week_ended_date DESC;

STORE very_negative_hosts_by_moving_week_avg INTO '$RESULTS_BASE_DIR/very_negative_hosts_by_moving_week_avg';

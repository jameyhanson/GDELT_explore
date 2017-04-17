-- Identifhy sources with extreme tone, expressed as:
-- "which sources have a highest percentage of their events in the 10th or 90th decile
--    or the AvgTone scale?"
-- See https://datafu.incubator.apache.org/docs/datafu/1.3.0/datafu/pig/stats/Quantile.html

REGISTER '/opt/cloudera/parcels/CDH-5.10.0-1.cdh5.10.0.p0.41/lib/pig/datafu.jar';
define Quantile datafu.pig.stats.Quantile('0.05','0.95');
 
gdelt = LOAD '/Data/GDELT/201704*.export.csv' AS (
    GLOBALEVENTID:long,
    SQLDATE:long,
    MonthYear:long,
   ...

gdelt_tone = FOREACH gdelt GENERATE avgTone;

grouped_tone = GROUP gdelt_tone ALL;

quantiles = FOREACH grouped_tone {
    sorted = ORDER input BY avgTone;
    GENERATE Quantile(sorted);
 }
 

-- Hive Metastore table ddl for results
--     of GDELT analysis

USE gdelt;

CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.avgtone_by_year (
    year     INT,
    min      FLOAT,
    p05      FLOAT,
    p25      FLOAT,
    median   FLOAT,
    p75      FLOAT,
    p95      FLOAT,
    max      FLOAT)
COMMENT 'GDELT AvgTone, grouped by Year'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/results/gdelt_AvgTone_ntiles_by_year';

CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.avgtone_by_month (
    month        INT,
    minus2sigma  FLOAT,
    minus1sigma  FLOAT,
    median       FLOAT,
    plus1sigma   FLOAT,
    plus2sigma   FLOAT)
COMMENT 'GDELT AvgTone, grouped by Month'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/results/gdelt_AvgTone_ntiles_by_month';

CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.avgtone_by_day (
    day        INT,
    minus2sigma  FLOAT,
    minus1sigma  FLOAT,
    median       FLOAT,
    plus1sigma   FLOAT,
    plus2sigma   FLOAT)
COMMENT 'GDELT AvgTone, grouped by Day'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/results/gdelt_AvgTone_ntiles_by_day';

CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.num_articles_by_year (
    year     INT,
    min      FLOAT,
    p05      FLOAT,
    p25      FLOAT,
    median   FLOAT,
    p75      FLOAT,
    p95      FLOAT,
    max      FLOAT)
COMMENT 'GDELT NumArticles, grouped by Year'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/results/gdelt_NumArticles_ntiles_by_year';

CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.num_mentions_by_year (
    year     INT,
    min      FLOAT,
    p05      FLOAT,
    p25      FLOAT,
    median   FLOAT,
    p75      FLOAT,
    p95      FLOAT,
    max      FLOAT)
COMMENT 'GDELT NumMentions, grouped by Year'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/results/gdelt_NumMentions_ntiles_by_year';

CREATE EXTERNAL TABLE IF NOT EXISTS gdelt.num_sources_by_year (
    year     INT,
    min      FLOAT,
    p05      FLOAT,
    p25      FLOAT,
    median   FLOAT,
    p75      FLOAT,
    p95      FLOAT,
    max      FLOAT)
COMMENT 'GDELT NumSources, grouped by Year'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION  '/results/gdelt_NumSources_ntiles_by_year';

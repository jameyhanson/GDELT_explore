#!/bin/bash

# Run Pig scripts

framework=tez
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_day.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_month.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_year.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_congress.pig
hdfs dfs -rmr /results/*

framework=mapreduce
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_day.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_month.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_year.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_congress.pig
hdfs dfs -rmr /results/*


framework=tez
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_day.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_month.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_year.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_congress.pig
hdfs dfs -rmr /results/*

framework=mapreduce
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_day.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_month.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_year.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_congress.pig
hdfs dfs -rmr /results/*


framework=tez
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_day.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_month.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_year.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_congress.pig
hdfs dfs -rmr /results/*

framework=mapreduce
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_day.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_month.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_year.pig
/usr/local/pig-0.16.0/bin/pig -x $framework AvgTone_ntiles_by_congress.pig

#!/bin/bash

# Run Pig scripts

framework=tez
pig6 -x $framework AvgTone_ntiles_by_day.pig
pig6 -x $framework AvgTone_ntiles_by_month.pig
pig6 -x $framework AvgTone_ntiles_by_year.pig
pig6 -x $framework AvgTone_ntiles_by_congress.pig
hdfs dfs -rmr /results/*

framework=mapreduce
pig6 -x $framework AvgTone_ntiles_by_day.pig
pig6 -x $framework AvgTone_ntiles_by_month.pig
pig6 -x $framework AvgTone_ntiles_by_year.pig
pig6 -x $framework AvgTone_ntiles_by_congress.pig
hdfs dfs -rmr /results/*


framework=tez
pig6 -x $framework AvgTone_ntiles_by_day.pig
pig6 -x $framework AvgTone_ntiles_by_month.pig
pig6 -x $framework AvgTone_ntiles_by_year.pig
pig6 -x $framework AvgTone_ntiles_by_congress.pig
hdfs dfs -rmr /results/*

framework=mapreduce
pig6 -x $framework AvgTone_ntiles_by_day.pig
pig6 -x $framework AvgTone_ntiles_by_month.pig
pig6 -x $framework AvgTone_ntiles_by_year.pig
pig6 -x $framework AvgTone_ntiles_by_congress.pig
hdfs dfs -rmr /results/*


framework=tez
pig6 -x $framework AvgTone_ntiles_by_day.pig
pig6 -x $framework AvgTone_ntiles_by_month.pig
pig6 -x $framework AvgTone_ntiles_by_year.pig
pig6 -x $framework AvgTone_ntiles_by_congress.pig
hdfs dfs -rmr /results/*

framework=mapreduce
pig6 -x $framework AvgTone_ntiles_by_day.pig
pig6 -x $framework AvgTone_ntiles_by_month.pig
pig6 -x $framework AvgTone_ntiles_by_year.pig
pig6 -x $framework AvgTone_ntiles_by_congress.pig

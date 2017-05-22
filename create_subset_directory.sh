#!/bin/bash

# create subset directory for development
hdfs dfs -mkdir -p /data/subset_gdelt_v2/events/

hdfs dfs -cp /data/gdelt_v2/events/20130402.export.csv /data/subset_gdelt_v2/events/
hdfs dfs -cp /data/gdelt_v2/events/20130602.export.csv /data/subset_gdelt_v2/events/
hdfs dfs -cp /data/gdelt_v2/events/20130802.export.csv /data/subset_gdelt_v2/events/
hdfs dfs -cp /data/gdelt_v2/events/20140102.export.csv /data/subset_gdelt_v2/events/
hdfs dfs -cp /data/gdelt_v2/events/20140302.export.csv /data/subset_gdelt_v2/events/
hdfs dfs -cp /data/gdelt_v2/events/20140502.export.csv /data/subset_gdelt_v2/events/
hdfs dfs -cp /data/gdelt_v2/events/20150702.export.csv /data/subset_gdelt_v2/events/
hdfs dfs -cp /data/gdelt_v2/events/20150902.export.csv /data/subset_gdelt_v2/events/
hdfs dfs -cp /data/gdelt_v2/events/20151002.export.csv /data/subset_gdelt_v2/events/
hdfs dfs -cp /data/gdelt_v2/events/20161002.export.csv /data/subset_gdelt_v2/events/
hdfs dfs -cp /data/gdelt_v2/events/20161102.export.csv /data/subset_gdelt_v2/events/
hdfs dfs -cp /data/gdelt_v2/events/20161202.export.csv /data/subset_gdelt_v2/events/
hdfs dfs -cp /data/gdelt_v2/events/20170102.export.csv /data/subset_gdelt_v2/events/
hdfs dfs -cp /data/gdelt_v2/events/20170402.export.csv /data/subset_gdelt_v2/events/
hdfs dfs -cp /data/gdelt_v2/events/20170502.export.csv /data/subset_gdelt_v2/events/

# swap directories for development / testing
# hdfs dfs -mv /data/gdelt_v2 /data/full_gdelt_v2
# hdfs dfs -mv /data/subset_gdelt_v2 /data/gdelt_v2

# swap directories for full load
# hdfs dfs -mv /data/gdelt_v2 /data/subset_gdelt_v2
# hdfs dfs -mv /data/full_gdelt_v2 /data/gdelt_v2

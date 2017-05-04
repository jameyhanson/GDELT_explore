# GDELT_explore
Explore the [GDELT project](http://gdeltproject.org/ "GDELT project") with Pig-on-MapReduce, Pig-on-Tez and other tools.  This repository is a mix of tool installation, tool exporation, tool performance testing, and data exploration.  

### Setup
GDELT data is available as an AWS public dataset.  That requires installing the AWS-CLI to `distcp` to the cluster.  

NOTE:  CDH 5.x incudes Pig 0.13 and does not include Tez, which means that Pig 0.16 and Tez 0.70 must be installed.  

1. Install AWS-CLI  
AWS-CLI [installation documentation] (http://docs.aws.amazon.com/cli/latest/userguide/installing.html)
2. Copy GDELT.  The scripts assume `/Data/GDELT_v[1-2]/events`.  The format changed slightly, which is why we put the files in two directories.  
```
hadoop distcp -Dfs.s3n.awsAccessKeyId=XXXXXXXXXXXXXXXXXXXX -Dfs.s3n.awsSecretAccessKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX s3n://gdelt-open-data/events/ hdfs:///Data/GDELT_v2/
hdfs dfs -mv /Data/GDELT_v2/events/19??.csv /Data/GDELT_v1/events
hdfs dfs -mv /Data/GDELT_v2/events/200?.csv /Data/GDELT_v1/events
hdfs dfs -mv /Data/GDELT_v2/events/200???.csv /Data/GDELT_v1/events
hdfs dfs -mv /Data/GDELT_v2/events/201???.csv /Data/GDELT_v1/events
```


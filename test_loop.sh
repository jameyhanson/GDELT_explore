#!/bin/bash
# see http://ryanstutorials.net/bash-scripting-tutorial/bash-loops.php

# loop through programs in mapreduce and tez

# scripts to test
read -d '' scripts << EOF
AvgTone_ntiles_by_congress.pig
AvgTone_ntiles_by_day.pig
AvgTone_ntiles_by_month.pig
AvgTone_ntiles_by_year.pig
EOF

frameworks='tez mapreduce'
# outer loop for frameworks
for framework in $frameworks
do
    echo $framework 'tests'
    
    # middle loop for run number
    for run_num in {1..3}
    do
        echo "clean output directory"
        hdfs dfs -rm -r -skipTrash /results/*
    
        # inner loop for pig script
        for script in $scripts
        do
            echo $framework $script
            export HADOOP_OPTS=-Dmapreduce.job.tags=_trial:_$run_num
            /usr/local/pig-0.16.0/bin/pig -x $framework $script
        done
    done
done
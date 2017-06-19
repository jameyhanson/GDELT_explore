#!/bin/bash
# run hosts_w_bad_tone_0[1-7]_???.pig
# see http://ryanstutorials.net/bash-scripting-tutorial/bash-loops.php

# loop through scripts using tez and mapreduce
num_runs = 2

# scripts to test
read -d '' scripts << EOF
hosts_w_bad_tone_01Mon.pig
hosts_w_bad_tone_02Tue.pig
hosts_w_bad_tone_03Wed.pig
hosts_w_bad_tone_04Thu.pig
hosts_w_bad_tone_05Fri.pig
hosts_w_bad_tone_06Sat.pig
hosts_w_bad_tone_07Sun.pig
EOF

frameworks='tez mapreduce'
# outer loop for frameworks
for framework in $frameworks
do
    echo "####### $framework tests ######"
    
    # middle loop for run number
    for run_num in {1..$num_runs}
    do
        echo "clean output directory"
        hdfs dfs -rm -r -skipTrash /results/hosts_with_lots_of_very_negative*
    
        # inner loop for pig script
        for script in $scripts
        do
            echo $framework $script
            # NOTE:  applicaiton tags do not work with Tez
            export HADOOP_OPTS=-Dmapreduce.job.tags=trial_number:_$run_num
            /usr/local/pig-0.16.0/bin/pig -x $framework $script
        done
    done
done

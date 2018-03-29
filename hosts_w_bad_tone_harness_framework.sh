#!/bin/bash
# run hosts_w_bad_tone_0[1-7]_???.pig
# see http://ryanstutorials.net/bash-scripting-tutorial/bash-loops.php

# loop through scripts using tez and mapreduce

# scripts to test
read -d '' scripts << EOF
pig -param_file pig.cfg hosts_w_bad_tone_01Mon.pig
pig -param_file pig.cfg hosts_w_bad_tone_02Tue.pig
pig -param_file pig.cfg hosts_w_bad_tone_03Wed.pig
pig -param_file pig.cfg hosts_w_bad_tone_04Thu.pig
pig -param_file pig.cfg hosts_w_bad_tone_05Fri.pig
pig -param_file pig.cfg hosts_w_bad_tone_06Sat.pig
pig -param_file pig.cfg hosts_w_bad_tone_07Sun.pig
pig -param_file pig.cfg hosts_w_bad_tone_99summary.pig
EOF

frameworks='mapreduce'
# outer loop for frameworks # tez and/or local if needed
for framework in $frameworks
do
    echo "####### $framework tests ######"
    
    # middle loop for run number
    for run_num in {1..1}
    do
        echo "clean output directory"
        hdfs dfs -rm -r -skipTrash /results/hosts_with_lots_of_very_negative*
        hdfs dfs -rm -r -skipTrash /results/very_negative_hosts_by_moving_week_avg
    
        # inner loop for pig script
        for script in $scripts
        do
            echo $framework $script
            # NOTE:  applicaiton tags do not work with Tez
            export HADOOP_OPTS=-Dmapreduce.job.tags=trial_number:_$run_num
            /usr/bin/pig -x $framework $script
        done
    done
done

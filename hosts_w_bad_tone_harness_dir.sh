#!/bin/bash
# run hosts_w_bad_tone_0[1-7]_???.pig
# see http://ryanstutorials.net/bash-scripting-tutorial/bash-loops.php

# loop through scripts using different pig configuration files

# scripts to test
read -d '' scripts << EOF
hosts_w_bad_tone_01Mon.pig
hosts_w_bad_tone_02Tue.pig
hosts_w_bad_tone_03Wed.pig
hosts_w_bad_tone_04Thu.pig
hosts_w_bad_tone_05Fri.pig
hosts_w_bad_tone_06Sat.pig
hosts_w_bad_tone_07Sun.pig
hosts_w_bad_tone_99summary.pig
EOF

config_files='pig_enc_un.cfg pig_enc_enc.cfg pig_un_un.cfg pig_un_enc.cfg'
# outer loop for frameworks # tez and/or local if needed
for config_file in $config_files
do
    echo "####### $encrypted data tests ######"
    
    # middle loop for run number
    for run_num in {1..3}
    do
        echo "clean unencrypted output directory"
        hdfs dfs -rm -r -skipTrash /unencrypted/results/hosts_with_lots_of_very_negative/*
        hdfs dfs -rm -r -skipTrash /unencrypted/results/very_negative_hosts_by_moving_week_avg

        echo "clean encrypted output directory"
        hdfs dfs -rm -r -skipTrash /enc_zone1/results/hosts_with_lots_of_very_negative/*
        hdfs dfs -rm -r -skipTrash /enc_zone1/results/very_negative_hosts_by_moving_week_avg    
    
        # inner loop for pig script
        for script in $scripts
        do
            echo $config_file $script
            # NOTE:  application tags do not work with Tez
            export HADOOP_OPTS=-Dmapreduce.job.tags=_trial_$run_num:_conf_$config_file
            time /usr/bin/pig -param_file $config_file $script
        done
    done
done

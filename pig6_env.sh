#!/bin/bash

# setup environment to run Pig6

export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
export PIG_HOME=/usr/local/pig-0.16.0
export HADOOP_CONF_DIR=/etc/hadoop/conf/
export HADOOP_USER_CLASSPATH_FIRST=true
export PATH=$PATH:$PIG_HOME/bin
export LD_LIBRARY_PATH=/usr/lib
alias pig6='$PIG_HOME/bin/pig'

# pig6 --version 
#     expect Apache Pig version 0.16.0

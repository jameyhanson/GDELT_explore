# GDELT_explore
Explore the [GDELT project](http://gdeltproject.org/ "GDELT project") with Pig-on-MapReduce, Pig-on-Tez and other tools.  This repository is a mix of tool installation, tool exporation, tool performance testing, and data exploration.  

GDELT data is available as an AWS public dataset, documented [here](https://aws.amazon.com/public-datasets/gdelt/).  The fields are defined [here](http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf).  That requires installing the AWS-CLI to `distcp` to the cluster.  

NOTE:  CDH 5.x incudes Pig 0.13 and does not include Tez, which means that Pig 0.16 and Tez 0.70 must be installed.  
NOTE:  Change the Pig logging directory in the `pig.properties pig.logfile`.

[Pig cheat-sheet] (https://www.qubole.com/resources/cheatsheet/pig-function-cheat-sheet/).  

## Call pig scripts with parameter file
''pig -param_file pig.cfg AvgTone_USA_ntiles_by_day.pig''

## Pig script common header
Q:  Who writes bad stuff about the USA?  
Approach:
  1. Who creates records with USA actors?
      w_usa_actors
  2. How many records with USA actors does a host create each week?
      host_records_by_week
   3. Which hosts write a lot of articles about the USA each month?
      hosts_that_report_alot_on_USA
   4. What is the tone of records about the USA?
      tone_of_articles_on_USA
   5. Which articles about the USA have a very negative tone?
      very_negative_records_about_usa
   6. How many very negative tone articles about USA to they write each month?
      host_count_very_negative_by_month
   7. What hosts write a large fraction of their articles about the USA with a very negative tone?
      large_fraction_negative_about_USA

Driving thresholds:  
     Q:  What is the aggregation interval?  
         A: epoch_week the article was created  
     Q: What defines a host with a lot of articles about the USA?  
         A: any host with more than the median number of articles about the USA  
     Q: What defines a an article about the USA with a very negative tone?  
         A: any article with a tone more than 2-sigma below the average tone  
     Q: What defines a host that writes a lot of very negative articles about the USA?  
         A: Any host FOR WHICH more than 1/2 of their articles about about the USA  
             have a very negative tone.  

DataFu quantiles    
Creates lines for:  
```
 +2 sigma p=0.9545  
 +1 sigma p=0.6827  
 median   p=0.5  
 -1 sigma p=0.3173  
 -2 sigma p=0.0455
```

gdelpt epoch began on 1-Jan-1979.  

Register DataFu and define an alias for the function  
Ref:  https://datafu.incubator.apache.org/docs/datafu/guide.html  

### Install AWS-CLI and `distcp` the GDELT dataset to your cluster
1. Install AWS-CLI  
AWS-CLI [installation documentation] (http://docs.aws.amazon.com/cli/latest/userguide/installing.html)
2. Review the GDELT dataset
`aws s3 ls s3://gdelt-open-data/events/ --recursive --human-readable --summarize`
3. Copy GDELT.  The scripts assume `/Data/GDELT_v[1-2]/events`.  
On 1-Apr-2013 the a URL column was added.  GDELT_V1 refers to the scheme before 1-Apr-2013, i.e. without URL 
```
export awsAccessKey=XXXXXXXXXXXXXXXXXXXX
export awsSecretAccessKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
hadoop distcp -Dfs.s3n.awsAccessKeyId=$awsAccessKey -Dfs.s3n.awsSecretAccessKey=$awsSecretAccessKey s3n://gdelt-open-data/events/ hdfs:///data/gdelt_v2/
sudo -u hdfs hdfs dfs -mv /data/gdelt_v2/events/19??.csv /data/gdelt_v1/events
sudo -u hdfs hdfs dfs -mv /data/gdelt_v2/events/200?.csv /data/gdelt_v1/events
sudo -u hdfs hdfs dfs -mv /data/gdelt_v2/events/200???.csv /data/gdelt_v1/events
sudo -u hdfs hdfs dfs -mv /data/gdelt_v2/events/201???.csv /data/gdelt_v1/events
```
#### Updated GDELT_V2 with most recent days
NOTE:  It is recommended to overwrite the two most recent files because, per the GDELT documentation, records are occassionally added a few days late.  
```
export awsAccessKey=XXXXXXXXXXXXXXXXXXXX
export awsSecretAccessKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
hadoop distcp -Dfs.s3n.awsAccessKeyId=$awsAccessKey -Dfs.s3n.awsSecretAccessKey=$awsSecretAccessKey s3n://gdelt-open-data/events/ 2017????.export.csv hdfs:///data/gdelt_v2/
```
### Install Maven and build Protobuf
1.  Install `bzip2`, `gcc*`, and `wget`  
`sudo yum -y install bzip2 gcc* wget`  
2. Install Maven.  
```
wget http://mirror.nexcess.net/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz  
sudo tar -xvf apache-maven-3.3.9-bin.tar.gz -C /usr/local/  
export PATH=$PATH:/usr/local/apache-maven-3.3.9/bin`  
export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera  
```
`mvn -version`  
expect `Apache Maven 3.3.9`  
3. Relax `/usr/local` permissions.    
`sudo chmod -R ugo+rwx /usr/local`  
4.  Download Protobuf 2.5.0  
NOTE:  The Yum install does not include protoc, so it must be made.  
`wget https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz`  
`tar -xvf protobuf-2.5.0.tar.gz`  
5. Make Protobuf
```
cd protobuf-2.5.0  
./configure --prefix=/usr  
make  
make check  
sudo make install  
```
6. Verify that protoc is installed correctly  
`export LD_LIBRARY_PATH=/usr/lib`  
`protoc --version`   
expect `libprotoc 2.5.0`
### Install Pig 0.16
1. Download Pig
```
wget http://www.us.apache.org/dist/pig/pig-0.16.0/pig-0.16.0.tar.gz
sudo tar -xvf pig-0.16.0.tar.gz -C /usr/local
```
2. Setup environment  
```export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
export PIG_HOME=/usr/local/pig-0.16.0
export HADOOP_CONF_DIR=/etc/hadoop/conf/
export HADOOP_USER_CLASSPATH_FIRST=true
export PATH=$PATH:$PIG_HOME/bin
export LD_LIBRARY_PATH=/usr/lib
alias pig6='$PIG_HOME/bin/pig'
```
3.  Verify the version and cluster connection for Pig  
`$PIG_HOME/bin/pig --version`  
`pig6 --version`  
`pig6`  
`grunt> ls /user`  
expect the directory structure of hdfs `/user/`  
`grunt> quit`  
### Install Tez 0.7.0
NOTE:  The Tez UI was not used or installed  
1. Download and untar Tez 0.7.0 source.  
```
cd ~  
wget http://archive.apache.org/dist/tez/0.7.0/apache-tez-0.7.0-src.tar.gz  
tar -xvf apache-tez-0.7.0-src.tar.gz  
```
2. Edit `.\apache-tez-0.7.0-src\pom.xml`  
`cd apache-tez-0.7.0-src`  
Set the values:  
```
<pig.version>0.16.0</pig.version>
<hadoop.version>2.6.0</hadoop.version>
```
3. Build Tez with Maven  
`mvn clean package -DskipTests=true`
4. Copy to tez-0.7.0-minimal.tar.gz to HDFS
```
cd ./tez-dist/target
cp tez-0.7.0.tar.gz /tmp
sudo -u hdfs hdfs dfs -mkdir -p /apps/tez-0.7.0
sudo -u hdfs hdfs dfs -put /tmp/tez-0.7.0.tar.gz /apps/tez-0.7.0
```
5.  Create a `tez-site.xml` file in the `$TEZ_CONF_DIR` directory, `/etc/hadoop/conf`, with:
**_NOTE:  This tez-site.xml is overwritten by CM when client configuration is redeployed._**
```
<configuration>
    <property>
        <name>tez.lib.uris</name>
        <value>hdfs:///apps/tez-0.7.0/tez-0.7.0.tar.gz</value>
    </property>
</configuration>
```
6. Create local `$TEZ_JARS`
```
mkdir /usr/local/tez_jars
tar -xvf ~/apache-tez-0.7.0-src/tez-dist/target/tez-0.7.0.tar.gz -C /usr/local/tez_jars 
chmod -R 777 /usr/local/tez_jars
```
7. Setup and source the environment for Pig 0.16 and Tex 0.7.0
```
export PATH=$PATH:/usr/local/apache-maven-3.3.9/bin
export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
export LD_LIBRARY_PATH=/usr/lib
export PIG_HOME=/usr/local/pig-0.16.0
export HADOOP_CONF_DIR=/etc/hadoop/conf/
export HADOOP_USER_CLASSPATH_FIRST=true
export PATH=$PATH:$PIG_HOME/bin
export TEZ_CONF_DIR=/etc/hadoop/conf
export TEZ_JARS=/usr/local/tez_jars
export HADOOP_CLASSPATH=${TEZ_CONF_DIR}:${TEZ_JARS}:${TEZ_JARS}/lib:${HADOOP_CLASSPATH}:${JAVA_JDBC_LIBS}:${MAPREDUCE_LIBS}
alias pig6='$PIG_HOME/bin/pig'
```
### Install jython
NOTE:  Jython is used with the Python UDF.   
Installation instructions for Jython are https://wiki.python.org/jython/InstallationInstructions  
1. Ensure that >= Java 1.7 is installed.  
2. Download the Jython installer .jar  
`wget http://search.maven.org/remotecontent?filepath=org/python/jython-installer/2.7.0/jython-installer-2.7.0.jar -o jython-installer-2.7.0.jar`
3.  Install Jython
`java -jar jython_installer-2.7.0.jar --console`
## Create an explain plan ##
```
pig6 -x <engine> -e 'explain -script <file>.pig'
```

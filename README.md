# GDELT_explore
Explore the [GDELT project](http://gdeltproject.org/ "GDELT project") with Pig-on-MapReduce, Pig-on-Tez and other tools.  This repository is a mix of tool installation, tool exporation, tool performance testing, and data exploration.  

GDELT data is available as an AWS public dataset, documented [here](https://aws.amazon.com/public-datasets/gdelt/).  The fields are defined [here](http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf).  That requires installing the AWS-CLI to `distcp` to the cluster.  

NOTE:  CDH 5.x incudes Pig 0.13 and does not include Tez, which means that Pig 0.16 and Tez 0.70 must be installed.  
NOTE:  Change the Pig logging directory in the `pig.properties pig.logfile`.

[Pig cheat-sheet] (https://www.qubole.com/resources/cheatsheet/pig-function-cheat-sheet/).  

### Install AWS-CLI and `distcp` the GDELT dataset to your cluster
1. Install AWS-CLI  
AWS-CLI [installation documentation] (http://docs.aws.amazon.com/cli/latest/userguide/installing.html)
2. Review the GDELT dataset
`aws s3 ls s3://gdelt-open-data/events/ --recursive --human-readable --summarize`
3. Copy GDELT.  The scripts assume `/Data/GDELT_v[1-2]/events`.  The format changed slightly, which is why we put the files in two directories.  
```
hadoop distcp -Dfs.s3n.awsAccessKeyId=XXXXXXXXXXXXXXXXXXXX -Dfs.s3n.awsSecretAccessKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX s3n://gdelt-open-data/events/ hdfs:///data/gdelt_v2/
hdfs dfs -mv /Data/GDELT_v2/events/19??.csv /data/gdelt_v1/events
hdfs dfs -mv /Data/GDELT_v2/events/200?.csv /data/gdelt_v1/events
hdfs dfs -mv /Data/GDELT_v2/events/200???.csv /data/gdelt_v1/events
hdfs dfs -mv /Data/GDELT_v2/events/201???.csv /data/gdelt_v1/events
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
`grunt> ls /user` Expect the directory structure of hdfs `/user/`
### Install Tez 0.7.0
NOTE:  The Tez UI was not used or installed  
1. Download and untar Tez 0.7.0 source.
`wget http://archive.apache.org/dist/tez/0.7.0/apache-tez-0.7.0-src.tar.gz`
`tar -xvf apache-tez-0.7.0-src.tar.gz`
2. Edit `pom.xml`
`cd apache-tez-0.7.0-src`
Set the values:
```
<pig.version>0.16.0</pig.version>
<hadoop.version>2.6.0</hadoop.version>
```
3. Build Tex with Maven  
`mvn clean package -DskipTests=true`
4. Copy to tez-0.7.0-minimal.tar.gz to HDFS
```
cd ./tez-dist/target
cp tez-0.7.0.tar.gz /tmp
sudo -u hdfs hdfs dfs -mkdir -p /apps/tez-0.7.0
sudo -u hdfs hdfs dfs -put /tmp/tez-0.7.0.tar.gz /apps/tez-0.7.0
```
5.  Create a `tez-site.xml` file in the `$TEZ_CONF_DIR` directory, `/etc/hadoop/conf`, with:
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


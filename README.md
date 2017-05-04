# GDELT_explore
Explore the [GDELT project](http://gdeltproject.org/ "GDELT project") with Pig-on-MapReduce, Pig-on-Tez and other tools.  This repository is a mix of tool installation, tool exporation, tool performance testing, and data exploration.  

GDELT data is available as an AWS public dataset, documented [here] (https://aws.amazon.com/public-datasets/gdelt/).  That requires installing the AWS-CLI to `distcp` to the cluster.  

NOTE:  CDH 5.x incudes Pig 0.13 and does not include Tez, which means that Pig 0.16 and Tez 0.70 must be installed.  

### Install AWS-CLI and `distcp` the GDELT dataset
1. Install AWS-CLI  
AWS-CLI [installation documentation] (http://docs.aws.amazon.com/cli/latest/userguide/installing.html)
2. Review the GDELT dataset
`aws s3 ls s3://gdelt-open-data/events/ --recursive --human-readable --summarize`
3. Copy GDELT.  The scripts assume `/Data/GDELT_v[1-2]/events`.  The format changed slightly, which is why we put the files in two directories.  
```
hadoop distcp -Dfs.s3n.awsAccessKeyId=XXXXXXXXXXXXXXXXXXXX -Dfs.s3n.awsSecretAccessKey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX s3n://gdelt-open-data/events/ hdfs:///Data/GDELT_v2/
hdfs dfs -mv /Data/GDELT_v2/events/19??.csv /Data/GDELT_v1/events
hdfs dfs -mv /Data/GDELT_v2/events/200?.csv /Data/GDELT_v1/events
hdfs dfs -mv /Data/GDELT_v2/events/200???.csv /Data/GDELT_v1/events
hdfs dfs -mv /Data/GDELT_v2/events/201???.csv /Data/GDELT_v1/events
```
### Install Pig 0.16 and Tez 0.70 on a CDH 5.11 cluster
1.  Install `bzip2`, `gcc*`, and `wget`  
yum -y install bzip2 gcc* wget
2. Install Maven.  
```
wget http://mirror.nexcess.net/apache/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
sudo tar -xvf apache-maven-3.3.9-bin.tar.gz -C /usr/local/
export PATH=$PATH:/usr/local/apache-maven-3.3.9/bin
export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera
mvn -version
```
expect Apache Maven 3.3.9
Relax `/usr/local` permissions.  `sudo chmod -R ugo+rwx /usr/local`
3.  Download Protobuf 2.5.0  
NOTE:  The Yum install does not include protoc, so it must be made.  
wget https://github.com/google/protobuf/releases/download/v2.5.0/protobuf-2.5.0.tar.gz
`tar -xvf protobuf-2.5.0.tar.gz`
4. Make Protobuf
```
cd protobuf-2.5.0
./configure --prefix=/usr
make
make check
sudo make install
```
5. Verify that protoc is installed correctly
```export LD_LIBRARY_PATH=/usr/lib```
```protoc --version``` expect ```libprotoc 2.5.0```

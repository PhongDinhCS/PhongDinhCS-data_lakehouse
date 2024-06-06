# make sure that you run (chmod +x create_topic.sh) from the directory STOCK_ANALYSIS_APP before run (sudo docker-compose up)
#docker-compose up --build -d

# run this code to check list of Topics (sudo docker-compose exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092)
# run this code to open producer and consumer
#sudo docker-compose exec kafka /opt/bitnami/kafka/bin/kafka-console-producer.sh --topic default_topic --bootstrap-server localhost:9092
#sudo docker-compose exec kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh --topic default_topic --from-beginning --bootstrap-server localhost:9092

Instructions:
Ensure you run chmod +x create_topic.sh before starting the services.
Use docker-compose up -d to start all services.
Check the list of topics with sudo docker-compose exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092.
Use docker exec -it hiveserver2 beeline -u 'jdbc:hive2://hiveserver2:10000/' to interact with Hive.


ubuntu2020@ubuntu2020-virtual-machine:/etc$ sudo nano /etc/hosts
[sudo] password for ubuntu2020: 

# Docker containers in phongdt_data_lakehouse_architecture_research_lakehouse_network

172.20.0.3 nodemanager
172.20.0.12 metastore
172.20.0.2 resourcemanager
172.20.0.11 spark-worker
172.20.0.10 datanode3
172.20.0.5 namenode
172.20.0.6 spark-master
172.20.0.9 datanode
172.20.0.4 datanode2
172.20.0.8 hiveserver2
172.20.0.7 kafka


---
ubuntu2020@ubuntu2020-virtual-machine:~/phongdinhcs_project/phongdt_data_lakehouse_architecture_research/backend/ingest_dir$ docker exec -it --user hadoop namenode /bin/bash
bash-4.2$ hdfs dfs -mkdir /raw
bash-4.2$ hdfs dfs -chmod -R 777 /raw
bash-4.2$ 
---
git clone https://github.com/PhongDinhCS/phongdt_data_lakehouse_architecture_research.git
docker compose down
docker stop $(docker ps -aq) && docker rm $(docker ps -aq)
docker volume prune
docker container prune
docker-compose down -v --remove-orphans
docker-compose up -d
docker network ls
docker network prune
docker network inspect phongdt_data_lakehouse_architecture_research_lakehouse_network
docker network rm phongdt_data_lakehouse_architecture_research_lakehouse_network
docker exec -it hiveserver2 bash
docker exec -it metastore beeline -u 'jdbc:hive2://localhost:10002'
docker exec -it hiveserver2 beeline -u 'jdbc:hive2://hiveserver2:10000/'


git commit
git commit -m "install and run kafka to scrape htlm"
git add .
git push origin main

#scala
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
sudo apt update
sudo apt install sbt



---
mkdir stock_analysis_app
cd stock_analysis_app

# Backend
mkdir -p backend
touch backend/Dockerfile
touch backend/requirements.txt
touch backend/app.py

# Frontend
mkdir -p frontend
touch frontend/Dockerfile
touch frontend/package.json
mkdir -p frontend/src  # Create src directory
touch frontend/src/sample.js  # Create a sample file inside src

# Spark
mkdir -p spark
touch spark/Dockerfile
touch spark/submit.sh
touch spark/analysis.py

# Docker Compose
touch docker-compose.yml

# Environment Variables
touch .env

# Volumes
mkdir -p volumes


sudo apt update
sudo apt install openjdk-11-jdk

docker run --name delta-spark --rm -d --entrypoint bash --network phongdt_data_lakehouse_architecture_research_lakehouse_network deltaio/delta-docker:latest -c "while true; do sleep 30; done;"
docker exec -it delta-spark /opt/spark/bin/spark-shell

Scala Shell
Open a bash shell (if on windows use git bash, WSL, or any shell configured for bash commands)

Run a container from the image with a bash entrypoint (build | DockerHub)

Launch a scala interactive shell session

docker exec -it delta-spark bash

$SPARK_HOME/bin/spark-shell --packages io.delta:${DELTA_PACKAGE_VERSION} \
--conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"


docker cp /home/ubuntu2020/Downloads/hadoop delta-spark:opt/spark/work-dir
delta-spark:/opt/spark/work-dir/hadoop/bin/hdfs

docker cp /home/ubuntu2020/phongdinhcs_project/phongdt_data_lakehouse_architecture_research/backend/ingest_dir/DeltaOnHDFS.scala delta-spark:/opt/spark/work-dir
docker exec -it delta-spark bash
cd /opt/spark/work-dir

/opt/spark/bin/spark-submit \
  --class DeltaOnHDFS \
  --master local[*] \
  --conf spark.delta.logStore.class=org.apache.spark.sql.delta.storage.HDFSLogStore \
  /opt/spark/work-dir/DeltaOnHDFS.scala


docker start $(docker ps -aq)

---
#check the permission of hdfs user on delta-spark able to write to hdfs://namenode:8020
NBuser@d38823102863:/opt/spark/work-dir$ hdfs groups -fs hdfs://namenode:8020
NBuser :
NBuser@d38823102863:/opt/spark/work-dir$ export HADOOP_USER_NAME=hdfs
NBuser@d38823102863:/opt/spark/work-dir$ hdfs groups -fs hdfs://namenode:8020
hdfs :
NBuser@d38823102863:/opt/spark/work-dir$ hdfs dfs -ls hdfs://namenode:8020/
NBuser@d38823102863:/opt/spark/work-dir$ hdfs dfs -mkdir hdfs://namenode:8020/test_spark_connection
NBuser@d38823102863:/opt/spark/work-dir$ hdfs dfs -ls hdfs://namenode:8020/
Found 1 items
drwxr-xr-x   - hdfs supergroup          0 2024-06-04 23:41 hdfs://namenode:8020/test_spark_connection
NBuser@d38823102863:/opt/spark/work-dir$ 

# run this script in namenode frist: hdfs dfs -chmod 777 /

hdfs groups -fs hdfs://namenode:8020
export HADOOP_USER_NAME=hdfs
hdfs groups -fs hdfs://namenode:8020
hdfs dfs -ls hdfs://namenode:8020/
hdfs dfs -mkdir hdfs://namenode:8020/test_spark_connection
hdfs dfs -ls hdfs://namenode:8020/

----

#create /delta-table from spark shell scala
scala> import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.{FileSystem, Path}

scala> import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configuration

scala> val hadoopConf = new Configuration()
hadoopConf: org.apache.hadoop.conf.Configuration = Configuration: core-default.xml, core-site.xml, mapred-default.xml, mapred-site.xml, yarn-default.xml, yarn-site.xml, hdfs-default.xml, hdfs-rbf-default.xml, hdfs-site.xml, hdfs-rbf-site.xml

scala> hadoopConf.set("fs.defaultFS", "hdfs://namenode:8020")

scala> val fs = FileSystem.get(hadoopConf)
fs: org.apache.hadoop.fs.FileSystem = DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_140862490_1, ugi=NBuser (auth:SIMPLE)]]

scala> val deltaTablePath = new Path("/delta-table")
deltaTablePath: org.apache.hadoop.fs.Path = /delta-table

scala> if (fs.mkdirs(deltaTablePath)) {
     |   println("Directory /delta-table created successfully.")
     | } else {
     |   println("Failed to create directory /delta-table.")
     | }
Directory /delta-table created successfully.

scala> 

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

// Get the Hadoop configuration
val hadoopConf = new Configuration()

// Set the HDFS Namenode URI
hadoopConf.set("fs.defaultFS", "hdfs://namenode:8020")

// Get the FileSystem instance
val fs = FileSystem.get(hadoopConf)

// Define the path for the new directory
val deltaTablePath = new Path("/delta-table")

// Create the directory
if (fs.mkdirs(deltaTablePath)) {
  println("Directory /delta-table created successfully.")
} else {
  println("Failed to create directory /delta-table.")
}

---

# log in to namenode container then check permission of hdfs

bash-4.2$ hdfs dfs -ls -d /
drwxrwxrwx   - hadoop supergroup          0 2024-06-04 23:48 /
bash-4.2$ hdfs dfs -ls /           
Found 2 items
drwxr-xr-x   - NBuser supergroup          0 2024-06-04 23:48 /delta-table
drwxr-xr-x   - hdfs   supergroup          0 2024-06-04 23:41 /test_spark_connection
bash-4.2$ hdfs dfs -chmod 777 /delta-table
77 /test_spark_connection

bash-4.2$ hdfs dfs -chmod 777 /test_spark_connection
bash-4.2$ 
bash-4.2$ hdfs dfs -ls /
Found 2 items
drwxrwxrwx   - NBuser supergroup          0 2024-06-04 23:48 /delta-table
drwxrwxrwx   - hdfs   supergroup          0 2024-06-04 23:41 /test_spark_connection
bash-4.2$ 

run this:

hdfs dfs -ls -d /
hdfs dfs -ls /
hdfs dfs -chmod 777 /delta-table
hdfs dfs -chmod 777 /test_spark_connection
hdfs dfs -ls /



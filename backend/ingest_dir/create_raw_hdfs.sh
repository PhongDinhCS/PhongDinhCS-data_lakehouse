#make sure to run this follow line to avoice Permission denied before execute this file: chmod +x create_raw_hdfs.sh
#./create_raw_hdfs.sh

#!/bin/bash

# Copy example_hadoop.txt to namenode container
docker cp example_hadoop.txt namenode:/example_hadoop.txt

# Create the /raw directory in HDFS if it doesn't exist
docker exec -it namenode hdfs dfs -mkdir -p /raw

# Set permissions for the /raw directory
docker exec -it namenode hdfs dfs -chmod 777 /raw

# Put the file into HDFS
docker exec -it namenode hdfs dfs -put /example_hadoop.txt /raw/

# Verify the file is in HDFS
docker exec -it namenode hdfs dfs -ls /raw

# Read the contents of the file
docker exec -it namenode hdfs dfs -cat /raw/example_hadoop.txt

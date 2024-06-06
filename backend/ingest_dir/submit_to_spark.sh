#!/bin/bash

# Submit your Scala application to the Spark cluster
spark-submit \
  --class KafkaToHDFS \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 \
  /home/ubuntu2020/phongdinhcs_project/phongdt_data_lakehouse_architecture_research/backend/ingest_dir/KafkaToHDFS.jar

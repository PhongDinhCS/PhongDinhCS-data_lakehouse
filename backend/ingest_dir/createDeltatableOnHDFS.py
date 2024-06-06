# # Copy the Python script to the delta-spark container
# docker cp createDeltatableOnHDFS.py delta-spark:/opt/spark/work-dir/

# # Access the delta-spark container
# docker exec -it delta-spark bash

# # Inside the container, run the Python script
# python /opt/spark/work-dir/createDeltatableOnHDFS.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Create Delta Table on HDFS") \
    .getOrCreate()

# Define the schema for the CSV file
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True)
])

# Read the CSV file with the specified schema
hdfs_listing = spark.read.option("header", "false").schema(schema).csv("hdfs://namenode:8020/")

# Print the schema of the DataFrame
hdfs_listing.printSchema()

# Write the DataFrame to HDFS using the Delta format
hdfs_listing.write.format("delta").save("hdfs://namenode:8020/delta-table")

# Stop the SparkSession
spark.stop()

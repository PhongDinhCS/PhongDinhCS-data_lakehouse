from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, current_timestamp

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Read from Kafka and Save to HDFS") \
    .getOrCreate()

# Read data from Kafka topic
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "currency_html") \
  .load()

# Extract necessary columns from the Kafka data
extracted_df = df.selectExpr("CAST(value AS STRING)")

# Convert extracted data to a DataFrame and add execution date and time columns
parsed_df = extracted_df.select("value") \
                        .withColumn("html_content", extracted_df["value"]) \
                        .withColumn("execute_date", current_date()) \
                        .withColumn("execute_time", current_timestamp())

# Write the data to HDFS partitioned by the execution date
query = parsed_df \
    .writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .partitionBy("execute_date") \
    .option("path", f"hdfs://path/to/save/data/{current_timestamp().strftime('%Y-%m-%d-%H-%M-%S')}") \
    .start()

# Wait for the query to terminate
query.awaitTermination()

from pyspark.sql import SparkSession

# Step 1: Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaToHDFS") \
    .getOrCreate()

# Step 2: Set Kafka and HDFS configurations
kafka_bootstrap_servers = 'kafka:9092'  # Replace with your Kafka broker address
kafka_topic = 'html_topic'  # Replace with the topic name where Parquet data is sent
hdfs_output_directory = "hdfs://namenode:8020/raw"  # Replace with your HDFS directory

# Step 3: Read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Step 4: Write data to HDFS
query = df \
    .writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/checkpoint_location") \
    .start(hdfs_output_directory)

# Step 5: Wait for termination
query.awaitTermination()
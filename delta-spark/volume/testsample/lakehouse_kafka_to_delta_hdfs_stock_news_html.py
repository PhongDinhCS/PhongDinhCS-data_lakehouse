from pyspark.sql import SparkSession
from datetime import datetime
from kafka import KafkaConsumer

# Initialize Spark session with Delta Lake configurations
spark = SparkSession.builder \
    .appName("KafkaToDelta") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Kafka configuration
bootstrap_servers = '172.18.0.99:9092'
topic = 'phongdinhcs-test-topic'

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    value_deserializer=lambda x: x.decode('utf-8')  # Decode bytes to string
)

# Consume a message from Kafka
for message in consumer:
    html_text = message.value
    break  # For demo purposes, only consume one message

# Define keywords and initialize counts
keywords = ['AAPL', 'AMZN', 'GOOG', 'MSFT', 'NVDA', 'TSLA']
keyword_counts = {keyword: html_text.upper().count(keyword) for keyword in keywords}

# Prepare data
timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
hdfs_location = 'hdfs://namenode:8020/testlakehouse/stock_news_html'
crawl_from_website = '"https://seekingalpha.com/market-news"'

# Create DataFrame with all required columns, ensuring html_text is the last column
data = [(timestamp, *keyword_counts.values(), hdfs_location, crawl_from_website, html_text)]
columns = ['timestamp'] + keywords + ['hdfs_location', 'crawl_from_website', 'html_text']
df = spark.createDataFrame(data, columns)

# Write DataFrame to Delta table
df.write.format("delta").mode("overwrite").save("hdfs://namenode:8020/testlakehouse/stock_news_html")

print(f"Data written to Delta table in HDFS with timestamp {timestamp}")

# Stop Spark session
spark.stop()

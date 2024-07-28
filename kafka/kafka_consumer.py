import io
from kafka import KafkaConsumer
import pyarrow.parquet as pq
from datetime import datetime

# Kafka consumer configuration (replace with your actual details)
bootstrap_servers = 'kafka:9092'  # Replace with your Kafka broker address
# Uncomment the line below if your Kafka broker is named "kafka" and uses default port 9092
# bootstrap_servers = 'kafka:9092'
topic = 'html_topic'  # Replace with the topic name where Parquet data is sent
group_id = 'parquet_consumer_group'  # Consumer group ID (can be any string)

# Create a KafkaConsumer instance
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    group_id=group_id,
    auto_offset_reset='earliest'  # Start consuming from the beginning
)

# Connect to the Kafka topic
print(f"Kafka consumer connected to topic: {topic}. Waiting for data...")

try:
    for message in consumer:
        # Get the current date and time
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"Parquet data received in topic: {topic} - Time: {current_time}")
        parquet_data = message.value  # Get the byte data from the message

        # Create an in-memory buffer from the byte data
        buffer = io.BytesIO(parquet_data)
        
        # Read the parquet data using pyarrow
        parquet_file = pq.ParquetFile(buffer)
        table = parquet_file.read()

        # Print the data or metadata
        print("Parquet file content:")
        print(table.to_pandas())  # Convert to pandas DataFrame and print

except KeyboardInterrupt:
    print("Keyboard interrupt detected. Stopping the Kafka consumer.")
finally:
    # Close the consumer when done
    consumer.close()

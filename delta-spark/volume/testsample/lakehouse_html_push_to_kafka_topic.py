import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
from datetime import datetime

# Make a request to the URL
url = 'https://seekingalpha.com/market-news'
headers = {'User-Agent': 'Mozilla/5.0'}
response = requests.get(url, headers=headers)

# Parse the HTML content
soup = BeautifulSoup(response.content, 'html.parser')

# Extract the text from the entire HTML
text = soup.get_text()

# Kafka configuration
bootstrap_servers = '172.18.0.99:9092'
topic = 'phongdinhcs-test-topic'

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: str(v).encode('utf-8')  # Convert messages to bytes
)

# Send the HTML text to the Kafka topic
producer.send(topic, value=text)

# Wait for all messages to be sent
producer.flush()

# Get current date and time
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

print(f"HTML text pushed to Kafka topic 'phongdinhcs-test-topic' at {now}")
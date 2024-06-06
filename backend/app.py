# backend/app.py
from kafka import KafkaProducer, KafkaConsumer
import json

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Kafka consumer
consumer = KafkaConsumer(
    'example_topic',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    group_id='example_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Example: Sending a message
message = {'key': 'value'}
producer.send('example_topic', value=message)

# Example: Receiving messages
for message in consumer:
    print(message.value)


from hdfs import InsecureClient

# Initialize HDFS client
client = InsecureClient('http://namenode:8020', user='hadoop')

# Define the string to be written to HDFS
data = "Hello, world!"

# Write the string to HDFS
with client.write('/raw/example.txt', encoding='utf-8', overwrite=True) as writer:
    writer.write(data)

import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer
import time
import pandas as pd
import datetime
import pyarrow.parquet as pq
import subprocess

# # Kafka broker address
# bootstrap_servers = '172.18.0.10:9092'
# # bootstrap_servers = 'kafka:9092'

# # Kafka topic
# topic = 'html_topic'

# # Create KafkaProducer instance
# producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# try:
#     while True:
#         # Fetch the webpage content
#         currency_page = requests.get('https://www.dailyfx.com/forex-rates#currencies')

#         # Parse the HTML content
#         currency_content = BeautifulSoup(currency_page.text, 'html.parser')

#         # Find the specific divs
#         currency_content_div = currency_content.find_all('div', {'class': 'dfx-instrumentTiles row'})

#         # Convert the content of divs to a string
#         div_content = ''.join(str(div) for div in currency_content_div)

#         # Get the first 50,000 characters
#         div_content_2000 = div_content[:2000]

#         # Message to send
#         message = "this will be html data"

#         # Add timestamp to the message
#         timestamp = time.time()
#         message_with_timestamp = f"{message} - Timestamp: {timestamp}\n\n{div_content_2000}"

#         # Convert message to bytes
#         message_bytes = message_with_timestamp.encode('utf-8')

#         # Produce message to Kafka topic
#         producer.send(topic, value=message_bytes)

#         # Print the message sent
#         print("Message sent successfully to Kafka topic:", topic, "- Timestamp:", timestamp)
#         print("First 2000 characters of div content:\n", div_content_2000[:2000], "...")  # Print first 500 characters of the first 50,000 characters

#         # Sleep for 10 seconds before sending the next message
#         time.sleep(10)

# except KeyboardInterrupt:
#     print("Keyboard interrupt detected. Stopping the Kafka producer.")
#     # Close producer
#     producer.close()

# Example usage:
source_url = 'https://www.dailyfx.com/forex-rates#currencies'
div_class = 'dfx-tabs__content'
output_dir = '/home/ubuntu2020/phongdinhcs_project/phongdt_data_lakehouse_architecture_research/backend/output/'


def scrape_and_save_as_parquet(source_url, div_class, output_dir):
    # Send a GET request to the URL
    currency_page = requests.get(source_url)

    # Check if the request was successful (status code 200)
    if currency_page.status_code == 200:
        # Parse the HTML content of the page
        currency_content = BeautifulSoup(currency_page.content, 'html.parser')

        # Find the specific div containing the currency data
        currency_content_div = currency_content.find('div', class_=div_class)

        # Check if the div is found
        if currency_content_div:
            # Convert the currency_content_div to a string and calculate its length
            div_string = str(currency_content_div)
            num_characters = len(div_string)

            # Get the current timestamp
            timestamp = int(time.time())

            # Convert timestamp to human-readable format
            timestamp_readable = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d_%H-%M-%S')

            # Create the filename using timestamp and source URL
            filename = f"{timestamp_readable}_{source_url.replace('://', '_').replace('/', '_')}.parquet"
            output_path = output_dir + filename

            # Create a DataFrame with the metadata
            metadata_df = pd.DataFrame({
                'source_url': [source_url],
                'div_class': [div_class],
                'timestamp': [timestamp],
                'timestamp_readable': [timestamp_readable],
                'filename': [filename],
                'div_string': [div_string],
                'num_characters': [num_characters]
            })

            # Save the DataFrame as a Parquet file
            metadata_df.to_parquet(output_path)
            print("Data saved as Parquet file:", output_path)

            # Return the path to the saved Parquet file
            return output_path

        else:
            print("Unable to find the div with class:", div_class)
            return None  # Indicate scraping failed (no div found)
    else:
        print("Failed to retrieve the webpage:", source_url)
        return None  # Indicate scraping failed (no div found)




# Kafka connection details
bootstrap_servers = 'kafka:9092'
# Uncomment the line below if your Kafka broker is named "kafka" and uses default port 9092
# bootstrap_servers = 'kafka:9092'
topic = 'html_topic'

# Create KafkaProducer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

try:
  while True:
    # Call the scrape function and get the Parquet file path
    parquet_file_path = scrape_and_save_as_parquet(source_url, div_class, output_dir)

    if parquet_file_path:
      # Read the Parquet file content as bytes
      with open(parquet_file_path, 'rb') as f:
        parquet_data = f.read()

      # Send the Parquet data as a message to Kafka topic
      producer.send(topic, value=parquet_data)

      # Print confirmation message
      print("Parquet data sent to Kafka topic:", topic)

    else:
      print("Scraping failed!")

    # Sleep for 10 seconds before the next scrape
    time.sleep(10)

except KeyboardInterrupt:
  print("Keyboard interrupt detected. Stopping the Kafka producer.")
  # Close producer
  producer.close()
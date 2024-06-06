import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import datetime
import pyarrow.parquet as pq

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

# Example usage:
source_url = 'https://www.dailyfx.com/forex-rates#currencies'
div_class = 'dfx-tabs__content'
output_dir = './output/'

scrape_and_save_as_parquet(source_url, div_class, output_dir)

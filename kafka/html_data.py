import requests
from bs4 import BeautifulSoup
import time

# Fetch the webpage content
url='https://www.dailyfx.com/forex-rates#currencies'

#url= 'https://www.investing.com/currencies/streaming-forex-rates-majors'

currency_page = requests.get(url)

# Get the original HTML content
original_html = currency_page.text

# Print the original HTML content
print(original_html)
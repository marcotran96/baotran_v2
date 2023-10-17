#Import library

import glob
import shutil
from sqlalchemy import create_engine
from datetime import datetime
import os
import pandas as pd
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from googleapiclient.discovery import build
from google.oauth2 import service_account
import warnings
import pygsheets as pg
import sys
warnings.filterwarnings("ignore")
import regex as re
warnings.filterwarnings("ignore")
from sqlalchemy import create_engine
abs_path = os.path.dirname(__file__)
main_cwd = re.sub('Y4A_BA_Team.*','Y4A_BA_Team',abs_path)
os.chdir(main_cwd)
sys.path.append(main_cwd)
from Shared_Lib import pcconfig
from Shared_Lib import shared_function as sf

pcconfig = pcconfig.Init()
host = pcconfig['serverip_postgre']
name = pcconfig['postgre_name']
passwd = pcconfig['postgre_passwd']
db = pcconfig['postgre_db']
json_path = pcconfig['json_path']

path = r'C:\Users\baotd\Documents\GitHub\Y4A_BA_Team\BA_Team\BaoTran\BOT\LSQ\download_report'
# options = Options()
options = webdriver.ChromeOptions()
# options.add_argument("--window-size=1920,1080")
# options.add_argument("--headless")
# options.add_argument("--disable-gpu")
options.add_argument('--no-sandbox')  # Add this line to fix sandbox issues (if any)
options.add_argument('--disable-dev-shm-usage')  # Add this line to fix issues with /dev/shm
options.add_argument("start-maximized")
options.add_argument("--incognito")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
options.add_experimental_option("prefs", {
  "download.default_directory": path
  })
# s = ChromiumService(ChromeDriverManager().install())
# driver = webdriver.Chrome(service=s, options=options)

chrome_driver_path = r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BI_Team/DinhNP/Airflow_chromedriver/chromedriver.exe'
# chrome_driver_path=pcconfig['selenium_chrome_driver']
# Create the Chrome WebDriver with the specified options and executable path
driver = webdriver.Chrome(options=options
    # options=options
                          )
wait = WebDriverWait(driver, 10)
print('Open Chrome')

driver.get('https://www.scrapethissite.com/pages/simple/')
time.sleep(2)


# Wait for the data to load (adjust the time as needed)
WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'country-name')))

# Get the page source and parse it with BeautifulSoup
page_source = driver.page_source
soup = BeautifulSoup(page_source, 'html.parser')

# Initialize lists to store data
country_names = []
capitals = []
populations = []
areas = []

# Find all the country elements
country_elements = soup.find_all('div', class_='col-md-4 country')

# Extract data from each country element
for country_element in country_elements:
    country_name = country_element.find('h3', class_='country-name').text.strip()
    capital = country_element.find('span', class_='country-capital').text.strip()
    population = country_element.find('span', class_='country-population').text.strip()
    area = country_element.find('span', class_='country-area').text.strip()

    # Append data to lists
    country_names.append(country_name)
    capitals.append(capital)
    populations.append(population)
    areas.append(area)

# Create a DataFrame from the extracted data
data = {
    'country_name': country_names,
    'capital': capitals,
    'population': populations,
    'area': areas
}

df = pd.DataFrame(data)

# Print the DataFrame
print(df)

#Task2

# Define the base URL
base_url = 'https://quotes.toscrape.com/page/{}/'

# Initialize lists to store data
quotes = []
authors = []

# Loop through the first 5 pages
for page_number in range(1, 6):
    # Construct the current page URL
    current_page_url = base_url.format(page_number)
    
    # Visit the current page
    driver.get(current_page_url)

    # Wait for the data to load (adjust the time as needed)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'quote')))

    # Get the page source and parse it with BeautifulSoup
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, 'html.parser')

    # Find all the quote elements on the current page
    quote_elements = soup.find_all('div', class_='quote')

    # Extract data from each quote element
    for quote_element in quote_elements:
        quote_text = quote_element.find('span', class_='text').text.strip()
        author_name = quote_element.find('small', class_='author').text.strip()

        # Append data to lists
        quotes.append(quote_text)
        authors.append(author_name)

# Create a DataFrame from the extracted data
data = {
    'quote': quotes,
    'author_name': authors
}

df = pd.DataFrame(data)

# Print the DataFrame
print(df)

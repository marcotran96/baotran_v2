#Import library

import glob
import shutil
from sqlalchemy import create_engine
from datetime import datetime
import os
import pandas as pd
import time
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
from bs4 import BeautifulSoup
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

driver.get('https://finance.yahoo.com/trending-tickers')


# Assuming you have already loaded the page
page_source = driver.page_source

# Parse the HTML using BeautifulSoup
soup = BeautifulSoup(page_source, 'html.parser')

# Find all the relevant HTML lines
lines = soup.find_all('tr', class_='simpTblRow')

# Initialize lists to store data
symbols = []
names = []
last_prices = []
market_times = []
changes = []
percent_changes = []
volumes = []
market_caps = []

for line in lines:
    symbol = line.find('a', {'data-test': 'quoteLink'}).text
    name = line.find('td', {'aria-label': 'Name'}).text
    
    # Check if the element exists before accessing its text attribute
    last_price_elem = line.find('fin-streamer', {'data-field': 'regularMarketPrice'})
    if last_price_elem:
        last_price = last_price_elem.text
    else:
        last_price = None

    market_time_elem = line.find('fin-streamer', {'data-field': 'regularMarketTime'})
    if market_time_elem:
        market_time = market_time_elem.text
    else:
        market_time = None
    
    change_elem = line.find('fin-streamer', {'data-field': 'regularMarketChange'})
    if change_elem:
        change = change_elem.text
    else:
        change = None
    
    percent_change_elem = line.find('fin-streamer', {'data-field': 'regularMarketChangePercent'})
    if percent_change_elem:
        percent_change = percent_change_elem.text
    else:
        percent_change = None
    
    volume_elem = line.find('fin-streamer', {'data-field': 'regularMarketVolume'})
    if volume_elem:
        volume = volume_elem.text
    else:
        volume = None
    
    market_cap_elem = line.find('fin-streamer', {'data-field': 'marketCap'})
    if market_cap_elem:
        market_cap = market_cap_elem.text
    else:
        market_cap = None
    
    symbols.append(symbol)
    names.append(name)
    last_prices.append(last_price)
    market_times.append(market_time)
    changes.append(change)
    percent_changes.append(percent_change)
    volumes.append(volume)
    market_caps.append(market_cap)

# Create a DataFrame to store the data
data = {
    'Symbol': symbols,
    'Name': names,
    'Last Price': last_prices,
    'Market Time': market_times,
    'Change': changes,
    '% Change': percent_changes,
    'Volume': volumes,
    'Market Cap': market_caps
}

df = pd.DataFrame(data)
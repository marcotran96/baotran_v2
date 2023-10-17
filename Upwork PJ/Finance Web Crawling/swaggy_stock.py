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

driver.get('https://swaggystocks.com/dashboard/stocks/market-sentiment')
time.sleep(10)

page_source = driver.page_source

# Parse the HTML using BeautifulSoup
soup = BeautifulSoup(page_source, 'html.parser')

# Find all the relevant HTML containers
containers = soup.find_all('div', class_='styles_container__IuRgX')

# Initialize lists to store data
rankings = []
symbols = []
company_names = []
change_percentages = []
buying_percentages = []

for container in containers:
    ranking_elem = container.find('p', class_='styles_name__M_BGb')
    symbol_and_name_elem = container.find('p', class_='styles_name__M_BGb')
    info_div = container.find('div', class_='styles_info__8BsWp')

    if ranking_elem:
        ranking = ranking_elem.text
    else:
        ranking = None
    
    if symbol_and_name_elem:
        symbol_and_name = symbol_and_name_elem.text
    else:
        symbol_and_name = None
    
    if info_div:
        change_percentage_elem = info_div.find('div', class_='styles_negative__oPzne' if '-' in info_div.text else 'styles_positive__J17Tp')
        buying_percentage_elem = info_div.find_all('div', class_='styles_info__8BsWp')[2]
    
        if change_percentage_elem:
            change_percentage = change_percentage_elem.text
        else:
            change_percentage = None
    
        if buying_percentage_elem:
            buying_percentage = buying_percentage_elem.text
        else:
            buying_percentage = None
    else:
        change_percentage = None
        buying_percentage = None

    # Split symbol and name
    if symbol_and_name:
        symbol, name = symbol_and_name.split('TSLA')
    else:
        symbol, name = None, None

    rankings.append(ranking)
    symbols.append(symbol)
    company_names.append(name)
    change_percentages.append(change_percentage)
    buying_percentages.append(buying_percentage)

# Create a DataFrame to store the data
data = {
    'Ranking': rankings,
    'Symbol': symbols,
    'Company Name': company_names,
    'Change Percentage': change_percentages,
    'Buying Percentage': buying_percentages
}

df = pd.DataFrame(data)
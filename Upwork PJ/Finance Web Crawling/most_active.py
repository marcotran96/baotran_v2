import requests
from bs4 import BeautifulSoup
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

driver.get('https://finance.yahoo.com/most-active?offset=0&count=100')

page_source = driver.page_source

soup = BeautifulSoup(page_source, "html.parser")


table_rows = soup.find_all("tr", class_="simpTblRow")


symbols = []
names = []
prices = []
changes = []
percent_changes = []
volumes = []
market_caps = []
pe_ratios = []


for row in table_rows:
    columns = row.find_all("td")
    symbols.append(columns[0].text)
    names.append(columns[1].text)
    prices.append(columns[2].text)
    changes.append(columns[3].text)
    percent_changes.append(columns[4].text)
    volumes.append(columns[5].text)
    market_caps.append(columns[6].text)
    pe_ratios.append(columns[7].text)



data = {
    "Symbol": symbols,
    "Name": names,
    "Price (Intraday)": prices,
    "Change": changes,
    "% Change": percent_changes,
    "Volume": volumes,
    "Market Cap": market_caps,
    "PE Ratio (TTM)": pe_ratios,
}

df = pd.DataFrame(data)





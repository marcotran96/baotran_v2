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
# options.add_argument("disable-infobars");
# options.add_argument("--window-size=1920,1080")
# options.add_argument("--headless")
# options.add_argument("--disable-gpu")
# options.add_argument('--no-sandbox')  # Add this line to fix sandbox issues (if any)
# options.add_argument('--disable-dev-shm-usage')  # Add this line to fix issues with /dev/shm
# options.add_argument("start-maximized")
# options.add_argument("--incognito")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
options.add_experimental_option("prefs", {
  "download.default_directory": path
  })

custom_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36"
options.add_argument(f"user-agent={custom_user_agent}")
# s = ChromiumService(ChromeDriverManager().install())
# driver = webdriver.Chrome(service=s, options=options)
time.sleep(3) 
chrome_driver_path = r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BI_Team/DinhNP/Airflow_chromedriver/chromedriver.exe'
# chrome_driver_path=pcconfig['selenium_chrome_driver']
# Create the Chrome WebDriver with the specified options and executable path
driver = webdriver.Chrome(options=options
    # options=options
                  )
time.sleep(3) 
wait = WebDriverWait(driver, 10)
print('Open Chrome')



driver.get('https://www.realestate.com.au/sold/in-brisbane+-+greater+region,+qld/list-1')

driver.get('https://www.realestate.com.au/sold/in-brisbane+-+greater+region,+qld/list-1')

page_source = driver.page_source

soup = BeautifulSoup(page_source, "html.parser")

property_cards = soup.find_all(class_="residential-card__content")

# Initialize lists to store the extracted data
prices = []
addresses = []
bedrooms = []
bathrooms = []
parking_spaces = []
land_sizes = []
property_types = []
sold_dates = []

# Iterate through each property card and extract the desired information
for card in property_cards:
    # Extract price
    price = card.find(class_="property-price").text.strip()
    prices.append(price)

    # Extract address
    address = card.find(class_="residential-card__details-link").text.strip()
    addresses.append(address)

    # Extract bedroom, bathroom, and parking space counts
    details = card.find_all(class_="View__PropertyDetail-sc-11ysrk6-0 eSRWKr")
    bedroom = details[0].find('p').text.strip()
    bathroom = details[1].find('p').text.strip()
    parking_space = details[2].find('p').text.strip()
    bedrooms.append(bedroom)
    bathrooms.append(bathroom)
    parking_spaces.append(parking_space)

    # Extract land size
    land_size = card.find(class_="property-size-group").text.strip()
    land_sizes.append(land_size)

    # Extract property type
    property_type = card.find(class_="residential-card__property-type").text.strip()
    property_types.append(property_type)

    # Extract sold date
    sold_date = card.find(text="Sold on")
    sold_dates.append(sold_date)

# Create a DataFrame from the extracted data
data = {
    "Price": prices,
    "Address": addresses,
    "Bedrooms": bedrooms,
    "Bathrooms": bathrooms,
    "Parking Spaces": parking_spaces,
    "Land Size": land_sizes,
    "Property Type": property_types,
    "Sold Date": sold_dates,
}
df = pd.DataFrame(data)

# Print the DataFrame
print(df)
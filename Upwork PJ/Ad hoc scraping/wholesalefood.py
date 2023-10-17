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

driver.get('https://www.wholefoodsmarket.co.uk/lowprice-highquality')
time.sleep(2)
# Locate the "Accept All" button by its ID
accept_button = driver.find_element(By.ID, "onetrust-accept-btn-handler")

# Click the button
accept_button.click()
Z
image_url_list = []
# Initialize a list to store the extracted text
all_texts = []

# Define a JavaScript function to scroll the page to the bottom
scroll_script = "window.scrollTo(0, document.body.scrollHeight);"

# Scroll the page to the bottom multiple times (adjust the number of times as needed)
for _ in range(5):  # You can adjust the number of times you want to scroll
    driver.execute_script(scroll_script)
    time.sleep(1)  # Add a short delay to allow content to load (if needed)

image_elements = WebDriverWait(driver, 10).until(
    EC.presence_of_all_elements_located((By.CSS_SELECTOR, '.content-fit img'))
)
image_urls = [image_element.get_attribute('src') for image_element in image_elements]

for i, image_url in enumerate(image_urls, start=1):
    print(f"Image {i} Source URL: {image_url}")
    image_url_list.append(image_url)

image_url_list = image_url_list[:-3]

elements = driver.find_elements(By.CSS_SELECTOR, '.preFade.fadeIn')

# Loop through the elements and extract text
for element in elements:
    text = element.text
    all_texts.append(text)

# Print all the extracted texts
for text in all_texts:
    print("Extracted Text:", text)

all_texts_filtered = [text for text in all_texts if text.strip()]

all_texts_filtered = all_texts_filtered[6:]
index_of_shopping = all_texts_filtered.index('Shopping') if 'Shopping' in all_texts_filtered else -1

# Create a new list containing only the elements before the 'Shopping' element
if index_of_shopping != -1:
    all_texts_filtered = all_texts_filtered[:index_of_shopping]


df = pd.DataFrame({'image_url': image_url_list, 'product_name': all_texts_filtered})

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
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from googleapiclient.discovery import build
from google.oauth2 import service_account
import warnings
import pygsheets as pg
from bs4 import BeautifulSoup
import sys
import requests
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

chrome_driver_path =  r'C:\Users\baotd\Documents\GitHub\Y4A_BA_Team\BA_Team\BaoTran\chrome_driver\chromedriver117.exe'
driver = webdriver.Chrome( executable_path=chrome_driver_path)
                          
wait = WebDriverWait(driver, 10)
print('Open Chrome')

driver.get('https://finance.vietstock.vn/')
driver.get('https://finance.vietstock.vn/')
# Find the element by its class name
final_result = pd.DataFrame()

list_code = [
            'https://finance.vietstock.vn/ACB-ngan-hang-tmcp-a-chau.htm?tab=BCTN',
             'https://finance.vietstock.vn/BID-ngan-hang-tmcp-dau-tu-va-phat-trien-viet-nam.htm?tab=BCTN',
             'https://finance.vietstock.vn/CTG-ngan-hang-tmcp-cong-thuong-viet-nam.htm?tab=BCTN',
             'https://finance.vietstock.vn/EIB-ngan-hang-tmcp-xuat-nhap-khau-viet-nam.htm?tab=BCTN',
              'https://finance.vietstock.vn/HDB-ngan-hang-tmcp-phat-trien-tp-hcm.htm?tab=BCTN',
              'https://finance.vietstock.vn/LPB-ngan-hang-tmcp-buu-dien-lien-viet.htm?tab=BCTN',
              'https://finance.vietstock.vn/MBB-ngan-hang-tmcp-quan-doi.htm?tab=BCTN',
              'https://finance.vietstock.vn/MSB-ngan-hang-tmcp-hang-hai-viet-nam.htm?tab=BCTN',
              'https://finance.vietstock.vn/OCB-ngan-hang-tmcp-phuong-dong.htm?tab=BCTN',
              'https://finance.vietstock.vn/SHB-ngan-hang-tmcp-sai-gon-ha-noi.htm?tab=BCTN',
              'https://finance.vietstock.vn/SSB-ngan-hang-tmcp-dong-nam-a.htm?tab=BCTN',
              'https://finance.vietstock.vn/STB-ngan-hang-tmcp-sai-gon-thuong-tin.htm?tab=BCTN',
              'https://finance.vietstock.vn/TCB-ngan-hang-tmcp-ky-thuong-viet-nam.htm?tab=BCTN',
              'https://finance.vietstock.vn/TPB-ngan-hang-tmcp-tien-phong.htm?tab=BCTN',
              'https://finance.vietstock.vn/VCB-ngan-hang-tmcp-ngoai-thuong-viet-nam.htm?tab=BCTN',
              'https://finance.vietstock.vn/VIB-ngan-hang-tmcp-quoc-te-viet-nam.htm?tab=BCTN',
              'https://finance.vietstock.vn/VPB-ngan-hang-tmcp-viet-nam-thinh-vuong.htm?tab=BCTN']

for code in list_code:
    driver.get(code)
    time.sleep(4)
    page_source = driver.page_source

    soup = BeautifulSoup(page_source, "html.parser")


    # Find the table element by its ID
    table = soup.find('table', {'id': 'table-2'})

    # Extract column headers
    headers = [header.text.strip() for header in table.find_all('th')]

    # Create an empty DataFrame with the extracted headers
    df = pd.DataFrame(columns=headers)

    # Extract data rows
    data_rows = table.find_all('tr')
    for row in data_rows:
        data = row.find_all('td')
        if data:
            row_data = [cell.text.strip() for cell in data]
            df = pd.concat([df, pd.DataFrame([row_data], columns=headers)], ignore_index=True)

    # Print the DataFrame
    print(df)


    driver.get(code)
    time.sleep(3)
    # Find the table by its ID attribute
    table = driver.find_element_by_id("stock-transactions")

    # Use JavaScript to scroll to the table
    driver.execute_script("arguments[0].scrollIntoView();", table)
    #2015-2018

    # Find the button element by its class name
    button = driver.find_element(By.CLASS_NAME, "btn.btn-default.m-l")

    # Click the button
    button.click()

    time.sleep(2)
    page_source_2 = driver.page_source

    soup_2 = BeautifulSoup(page_source_2, "html.parser")


    # Find the table element by its ID
    table_2 = soup_2.find('table', {'id': 'table-2'})

    # Extract column headers
    headers_2 = [header.text.strip() for header in table_2.find_all('th')]

    # Create an empty DataFrame with the extracted headers
    df_2 = pd.DataFrame(columns=headers_2)

    # Extract data rows
    data_rows = table_2.find_all('tr')
    for row in data_rows:
        data = row.find_all('td')
        if data:
            row_data = [cell.text.strip() for cell in data]
            df_2 = pd.concat([df_2, pd.DataFrame([row_data], columns=headers_2)], ignore_index=True)

    df = df.drop(df.columns[0], axis=1)

    result_df = pd.concat([df_2, df], axis=1)
    result_df['code'] = code

    # Display the concatenated DataFrame

    final_result = pd.concat([final_result, result_df], ignore_index=True)

    
final_result.to_excel('C:\\Users\\baotd\\Desktop\\vietstock_2.xlsx')

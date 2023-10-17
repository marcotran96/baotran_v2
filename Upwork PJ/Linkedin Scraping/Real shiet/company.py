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
abs_path = os.path.dirname(os.getcwd())
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

# chrome_driver_path = r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BI_Team/DinhNP/Airflow_chromedriver/chromedriver.exe'
# # chrome_driver_path=pcconfig['selenium_chrome_driver']
# # Create the Chrome WebDriver with the specified options and executable path
# driver = webdriver.Chrome(options=options
#     # options=options
#                           )
# wait = WebDriverWait(driver, 10)
# print('Open Chrome')


from linkedin_scraper import Person, actions, Company
from selenium import webdriver
import pandas as pd

driver = webdriver.Chrome()
email = "marcotran96@gmail.com"
password = "Ftu5718@"
actions.login(driver, email, password) # if email and password isnt given, it'll prompt in terminal


# Define the base search URL
base_url = 'https://www.linkedin.com/search/results/companies/?companyHqGeo=%5B%22101165590%22%5D&companySize=%5B%22B%22%2C%22C%22%5D&industryCompanyVertical=%5B%2247%22%5D&keywords=Company&origin=GLOBAL_SEARCH_HEADER&sid=!Lp'

# Create empty lists to store company information
all_data = []

# Loop through the first 10 pages
for page_number in range(1, 11):
    # Construct the URL for the current page
    page_url = f'{base_url}&page={page_number}'
    
    # Navigate to the LinkedIn search results page
    driver.get(page_url)
    
    # Wait for the results to load (adjust the timeout as needed)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'entity-result')))
    
    # Extract page source and parse with BeautifulSoup
    page_source = driver.page_source
    soup = BeautifulSoup(page_source, "html.parser")

    # Find all company elements in the search results
    company_elements = soup.find_all('div', class_='entity-result')

    # Iterate through the company elements and extract information
    for company_element in company_elements:
        company_name = company_element.find('span', class_='entity-result__title-text').get_text(strip=True)
        location = company_element.find('div', class_='entity-result__primary-subtitle').get_text(strip=True)
        followers = company_element.find('div', class_='entity-result__secondary-subtitle').get_text(strip=True)

        # Find the correct company URL
        a_tags = company_element.find_all('a', class_='app-aware-link')
        company_url = None

        for a_tag in a_tags:
            href = a_tag.get('href')
            if href and '/company/' in href:
                company_url = href
                break

        # Append the extracted data to the list
        all_data.append([company_name, location, followers, company_url])

# Create a DataFrame from the extracted data
columns = ['Company Name', 'Location', 'Followers', 'Href']
df = pd.DataFrame(all_data, columns=columns)


# Display the DataFrame
print(df)

#Create a list
company_urls = df['Href'].to_list()

# Create empty DataFrames to store company and employee data
# Create empty lists to store company and employee data
company_data_list = []
employees_data_list = []
# Create an empty list to store error cases
error_data_list = []


# Loop through each company URL
for company_url in company_urls:
    try:
    # Create a Company object
        company = Company(company_url, driver=driver)

        # Extract attributes for the company
        name = company.name
        about_us = company.about_us  
        website = company.website
        industry = company.industry
        headquarters = company.headquarters
        company_size = company.company_size
        founded = company.founded
        specialties = company.specialties

        # Filter employees
        employees = [{k: v for k, v in emp.items() if emp} for emp in company.employees if emp]

        # Create a DataFrame for the company
        # Append data to the respective lists
        company_data_list.append([
            name, about_us, website, industry, headquarters, company_size, founded, specialties
        ])

        for employee in employees:
            employee['Company'] = name  # Add a column for the company name
            employees_data_list.append(employee)

    except Exception as e:
        # Handle errors for this company URL
        error_data_list.append([company_url, str(e)])


# Convert lists to DataFrames
company_columns = ['Name', 'About Us', 'Website', 'Industry', 'Headquarters', 'Company Size', 'Founded', 'Specialties']
company_data = pd.DataFrame(company_data_list, columns=company_columns)
employees_data = pd.DataFrame(employees_data_list)

# Create an error DataFrame
error_columns = ['Company_URL', 'Error_Message']
error_data = pd.DataFrame(error_data_list, columns=error_columns)
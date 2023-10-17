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

gc = pg.authorize(service_file=json_path)

spreadsheet_key = '1paClpbDr76Js3YwLv8GYS8axHXjNZw8pv0zhIzNdPYI'
sh = gc.open_by_key(spreadsheet_key)

# GET DATA FROM INPUT_SHEET
sheet_name = 'Sheet1'
wks_input = sh.worksheet_by_title(sheet_name)

sheetAll = wks_input.get_all_records(empty_value='', head=1, majdim='ROWS', numericise_data=True)

df_code = pd.DataFrame.from_dict(sheetAll)
backupcode = df_code['CODE'][5]
# /airflow/data/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/download_report/*
# 'C:/Users/baotd/ocuments/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/download_report/*'
#Delete old file, make sure folder is empty
path_report_downloaded = r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/download_report/*'
files = glob.glob(path_report_downloaded)
for f in files:
    os.remove(f)
print("Deleted all files in download folder")

#Login to LSQ
def site_login():
    id_login = 'actg.a2zcg@gmail.com'
    password_login = 'HyVong_100tr2'
    wait.until(EC.element_to_be_clickable((By.ID,"emailAddress")))
    time.sleep(20)
    driver.find_elements(By.ID, 'emailAddress')[0].send_keys(id_login)
    driver.find_elements(By.ID, 'password')[0].send_keys(password_login)
    driver.find_elements(By.CLASS_NAME,'lsq-button-block')[0].click()

driver.get('https://dashboard.lsq.com/login')
site_login()

wait.until(EC.element_to_be_clickable((By.CLASS_NAME,"lsq-button-block")))
time.sleep(5)
driver.find_elements(By.CLASS_NAME,'lsq-button-block')[1].click()
time.sleep(30)

print('Code used : ',backupcode)
print("Login to LSQ")

#Nhap backupcode
wait.until(EC.element_to_be_clickable((By.ID,"backupCode")))
time.sleep(5)
driver.find_elements(By.ID, 'backupCode')[0].send_keys(backupcode)
print("Entered Backup Code")
#Click dang nhap sau khi nhap backup code
driver.find_elements(By.CLASS_NAME,'lsq-button-block')[0].click()
time.sleep(5)
print("Clicked Log in")
#LLC = US

time.sleep(15)
driver.find_elements(By.XPATH,"//td[@class='icon login-arrow']")[0].click()

print("Chose US market")
#Click cancel update phone number
wait.until(EC.element_to_be_clickable((By.XPATH,"//button[@class='button-secondary button-size-auto cancel-btn pull-left']"))).click()
print("Clicked cancelled updating phone number")

#Function to find date columns and transform to datetime64
def find_and_transform_date(df): 
    date_cols = [col for col in df.columns if re.search('DATE',col.upper())]
    df[date_cols] = df[date_cols].astype('datetime64[ns]')
    return df

#Define function to download LSQ reports
def download_csv_lsq(link):
    driver.get (str(link))
    time.sleep(5)
    text_nodata = len(driver.find_elements(By.XPATH, "//p[@class='text-text-gray']"))

    if text_nodata == 1:
        if link == purchase :
            wait.until(EC.element_to_be_clickable((By.XPATH,
                                                   "//div[@class='form-control input-group lsq-select-viewport align-content-center viewportClass inputClass']"))).click()
            wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@id='lsq-date-select-year-to-date']"))).click()
            wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[@class='button-primary button-size-block']"))).click()
            time.sleep(2)
            wait.until(
                EC.element_to_be_clickable((By.XPATH, "//li[@class='banner-icon export ng-star-inserted']"))).click()
            driver.find_elements(By.XPATH, "//div[@class='radio-icon']")[1].click()
            wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[@class='button-primary button-size-sm']"))).click()
            time.sleep(10)
        elif link == collections :
            shutil.copy(r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/dummy_report/collections.csv',
                    r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/download_report/collections.csv')
        elif link == funding :
            shutil.copy(r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/dummy_report/fundings.csv',
                    r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/download_report/fundings.csv')
        else : shutil.copy(r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/dummy_report/overview-by-date.csv',
                    r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/download_report/overview-by-date.csv')

    else:
        wait.until(EC.element_to_be_clickable((By.XPATH,"//div[@class='form-control input-group lsq-select-viewport align-content-center viewportClass inputClass']"))).click()
        wait.until(EC.element_to_be_clickable((By.XPATH,"//button[@id='lsq-date-select-year-to-date']"))).click()
        wait.until(EC.element_to_be_clickable((By.XPATH,"//button[@class='button-primary button-size-block']"))).click()
        time.sleep(2)
        wait.until(EC.element_to_be_clickable((By.XPATH,"//li[@class='banner-icon export ng-star-inserted']"))).click()
        driver.find_elements(By.XPATH,"//div[@class='radio-icon']")[1].click()
        wait.until(EC.element_to_be_clickable((By.XPATH,"//button[@class='button-primary button-size-sm']"))).click()
        time.sleep(10)

def download_csv_lsq_aging():
    driver.get ('https://dashboard.lsq.com/reports/aging')
    
    wait.until(EC.element_to_be_clickable((By.XPATH,"//li[@class='banner-icon export ng-star-inserted']"))).click()
    wait.until(EC.element_to_be_clickable((By.XPATH,"//div[@class='checkbox-icon unchecked']"))).click()
    driver.find_elements(By.XPATH,"//div[@class='radio-icon']")[1].click()
    wait.until(EC.element_to_be_clickable((By.XPATH,"//button[@class='button-primary button-size-sm']"))).click()
    time.sleep(5)
def download_csv_lsq_ineligibles():
    driver.get ('https://dashboard.lsq.com/reports/ineligibles')
    time.sleep(5)
    text_nodata = len(driver.find_elements(By.XPATH,"//p[@class='text-text-gray']"))
    if text_nodata == 1 :
        shutil.copy(r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/dummy_report/ineligibles.csv',
                    r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/download_report/ineligibles.csv')
    else :
        wait.until(EC.element_to_be_clickable((By.XPATH,"//li[@class='banner-icon export ng-star-inserted']"))).click()
        driver.find_elements(By.XPATH,"//div[@class='radio-icon']")[1].click()
        wait.until(EC.element_to_be_clickable((By.XPATH,"//button[@class='button-primary button-size-sm']"))).click()
    time.sleep(10)

def download_available_fund_summary():
    driver.get('https://dashboard.lsq.com/reports/available-funds-summary')
    # click dropdown data
    time.sleep(200)
    driver.find_elements(By.XPATH, "//div[@class='expandable-container-header row']")[0].click()
    time.sleep(10)
    driver.find_elements(By.XPATH, "//div[@class='expandable-container-header row']")[1].click()
    time.sleep(10)
    driver.find_elements(By.XPATH, "//div[@class='expandable-container-header row']")[2].click()
    time.sleep(10)
    driver.find_elements(By.XPATH, "//div[@class='expandable-container-header row']")[3].click()
    time.sleep(2)
    num_list = []
    backup_code = driver.find_elements(By.XPATH,
                                       "//div[@class='expandable-container-right col-5 col-md-3 ng-star-inserted']")
    get_code = [x.text for x in backup_code if len(x.text) > 0]
    num_list.append(get_code)
    num_list = num_list[0]
    gross_available = float(num_list[0].replace('$', '').replace(',', ''))
    fees = float(num_list[1].replace('$', '').replace(',', ''))
    unpaid_invoices = float(num_list[2].replace('$', '').replace(',', ''))
    total_reserves = float(num_list[3].replace('$', '').replace(',', ''))
    num_list_2 = []
    backup_code = driver.find_elements(By.XPATH, "//p[@class='summary-row-value ng-star-inserted']")
    get_code = [x.text for x in backup_code if len(x.text) > 0]
    num_list_2.append(get_code)
    num_list_2 = num_list_2[0]
    num_list_3 = []
    for i in num_list_2:
        i = float(i.replace('$', '').replace(',', '').replace('%', ''))
        num_list_3.append(i)
    ineligible_invoices = num_list_3[1]
    eligible_invoices = num_list_3[2]
    advance_rate = num_list_3[3]
    accrued_collection_fees = num_list_3[4]
    accrued_funds_outstanding_fees = num_list_3[5]
    funds_outstanding = num_list_3[6]
    available_funds = num_list_3[7]
    pending_funds_request = num_list_3[8]
    beginning_balance = num_list_3[9]
    purchases = num_list_3[10]
    collections = num_list_3[11]
    chargebacks = num_list_3[12]
    non_factored = num_list_3[13]
    required_reserves = num_list_3[18]
    date_now = [datetime.now()]
    df = pd.DataFrame(date_now, columns=['RUN_TIME'])
    df['INELIGIBLE_INVOICES'] = ineligible_invoices
    df['ELIGIBLE_INVOICES'] = eligible_invoices
    df['ADVANCE_RATE'] = advance_rate * 0.01
    df['ACCRUED_COLLECTION_FEES'] = accrued_collection_fees
    df['ACCRUED_FUNDS_OUTSTANDING_FEES'] = accrued_funds_outstanding_fees
    df['FUNDS_OUTSTANDING'] = funds_outstanding
    df['AVAILABLE_FUNDS'] = available_funds
    df['PENDING_FUNDS_REQUEST'] = pending_funds_request
    df['BEGINNING_BALANCE'] = beginning_balance
    df['PURCHASES'] = purchases
    df['COLLECTIONS'] = collections
    df['CHARGEBACKS'] = chargebacks
    df['NON_FACTORED'] = non_factored
    df['REQUIRED_RESERVES'] = required_reserves
    df['GROSS_AVAILABLE'] = gross_available
    df['FEES'] = fees
    df['UNPAID_INVOICES'] = unpaid_invoices
    df['TOTAL_RESERVES'] = total_reserves

    return df
def upload_history_download():

    driver.get ('https://dashboard.lsq.com/invoice-upload-history')
    wait.until(EC.element_to_be_clickable((By.XPATH,"//div[@class='form-control input-group lsq-select-viewport align-content-center viewportClass inputClass']"))).click()
    wait.until(EC.element_to_be_clickable((By.XPATH,"//button[@id='lsq-date-select-year-to-date']"))).click()
    wait.until(EC.element_to_be_clickable((By.XPATH,"//button[@class='button-primary button-size-block']"))).click()
    time.sleep(2)
    text_nodata = len(driver.find_elements(By.XPATH, "//p[@class='text-text-gray']"))
    if text_nodata == 1: return
    wait.until(EC.element_to_be_clickable((By.XPATH,"//li[@class='banner-icon export ng-star-inserted']"))).click()
    driver.find_elements(By.XPATH,"//div[@class='radio-icon']")[1].click()
    wait.until(EC.element_to_be_clickable((By.XPATH,"//button[@class='button-primary button-size-sm']"))).click()
    time.sleep(5)

def download_upload_history_and_preprocess():
    upload_history_download()
    path = r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/download_report'

    text_nodata = len(driver.find_elements(By.XPATH, "//p[@class='text-text-gray']"))
    if text_nodata == 1: print('LLC Foreign have no upload today');return

    attempts = 0
    while attempts < 5:

        list_file_download_1 = os.listdir(path)
        for i in range(0, len(list_file_download_1)):
            list_file_download_1[i] = list_file_download_1[i].split('_')[0].lower()

        # Download upload history
        if 'upload-history' in list_file_download_1:
            break
        else:
            upload_history_download();
            attempts += 1
            if attempts > 5 : print('Download fail')
    # Double check

    list_file_download_2 = os.listdir(path)
    for i in range(0, len(list_file_download_2)):
        if list_file_download_2[i].split('_')[0].lower() == 'upload-history':
            upload_history = list_file_download_2[i]
            print('Upload history file: ', upload_history)

            list_dl = os.listdir(path)
            # Load csv file into DF
            df_upload_history = pd.read_csv(path + '/' + upload_history, skiprows=4)
            df_upload_history.columns = df_upload_history.columns.str.strip().str.replace(' ', '_').str.replace('_#',
                                                                                                                '').str.upper()
            df_upload_history['RUN_TIME'] = datetime.now()
            df_upload_history['DATE'] = df_upload_history['DATE'].astype('datetime64[ns]')
            current_channel = driver.find_elements(By.XPATH, "//span[@class='ng-star-inserted']")[0].text
            if current_channel == 'Yes4All LLC':
                df_upload_history['CHANNEL'] = 'US'
            else:
                df_upload_history['CHANNEL'] = 'INT'
            print('Download succeed')
            return df_upload_history
        else:
            pass


#Directory of each LSQ report
purchase = 'https://dashboard.lsq.com/reports/purchases'
overview_by_date = 'https://dashboard.lsq.com/reports/account-overview'
collections = 'https://dashboard.lsq.com/reports/collections'
funding = 'https://dashboard.lsq.com/reports/fundings'

# Check if Reports are downloaded to default folder, if no -> download report, if yes -> do nothing

attempts = 0
while attempts < 5:

    list_file_download = os.listdir(r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/download_report')
    for i in range(0, len(list_file_download)):
        list_file_download[i] = list_file_download[i].split('_')[0].lower()

    # Download purchases rp
    if 'purchases' in list_file_download:
        pass
    else:
        download_csv_lsq(purchase)
    # Download overview rp
    if 'overview-by-date' in list_file_download:
        pass
    else:
        download_csv_lsq(overview_by_date)
    # Download collection rp
    if 'collections' in list_file_download:
        pass
    else:
        download_csv_lsq(collections)
    # Download funding rp
    if 'fundings' in list_file_download:
        pass
    else:
        download_csv_lsq(funding)
    # Download aging rp
    if 'aging' in list_file_download:
        pass
    else:
        download_csv_lsq_aging()
    # Download ineligibles rp
    if 'ineligibles' in list_file_download:
        pass
    else:

        download_csv_lsq_ineligibles()
    if len(list_file_download) == 6:
        break
    else:
        attempts += 1

print("Downloaded reports")

#Move downloaded reports to other directory

path = r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/download_report'
list_dl = os.listdir(path)
for file in list_dl :
    if file.split('_')[0].lower().replace('.csv','') == 'aging' : file_aging = file
    elif file.split('_')[0].lower().replace('.csv','') == 'collections' : file_collection = file
    elif file.split('_')[0].lower().replace('.csv','') == 'fundings' : file_funding = file
    elif file.split('_')[0].lower().replace('.csv','') == 'ineligibles' : file_ineligibles = file
    elif file.split('_')[0].lower().replace('.csv','') == 'overview-by-date' : file_overview = file
    elif file.split('_')[0].lower().replace('.csv','') == 'purchases' : file_purchases = file
    else : pass
print("File names: ",'\n' ,file_aging,'\n' ,file_collection,'\n' ,file_funding,'\n' ,file_ineligibles,'\n' ,file_overview,'\n' ,file_purchases)

#Function to transform Collections, Fundings, Account Overview, Purchases report
def read_files_transform_data_us_market():
    file_prefix = ["Collections__","Fundings__","Overview-by-Date__","Purchases__"]
    df_dict = {i:None for i in file_prefix}
    for file in os.listdir(path):
        for key in df_dict:
            if file.startswith(key):
                #Read csv and skip top 4 rows
                df_dict[key] = pd.read_csv(os.path.join(path,file),skiprows=4)
                print("Load {} into dataframe".format(file))
                #Remove last row if first cell of the last row contains 'Totals'
                if df_dict[key].iloc[-1,0] == 'Totals':
                    df_dict[key] = df_dict[key][:-1]
                print("Remove Totals row")
                #Transform column name
                df_dict[key].columns = [re.sub('[^A-Z0-9\s\-]','',new_col_name.upper()).strip().replace(' ','_').replace('-','_') for new_col_name in df_dict[key].columns]
                print("Tranform columns names")
                #Transform all columns containing 'DATE' to datetime
                df_dict[key] = find_and_transform_date(df_dict[key])
                print("Convert data time column to datetime data type")
                #Add Channel
                df_dict[key]['CHANNEL'] = 'US'
                print("Add US Channel")
                #Add run time
                df_dict[key]['RUN_TIME'] = datetime.now()
                print("Add run_time")
                #Transform Purchases
                if file.startswith('Purchases__'):
                    df_dict[key]['DATE'].fillna(inplace=True, method = 'ffill')
                    df_dict[key].dropna(inplace=True, subset=['CUSTOMER'])
                    df_dict[key]['REQUIRED_RESERVES'] = df_dict[key]['AMOUNT']*0.15
                    df_dict[key]['AVAILABLE_FUNDS'] = df_dict[key]['AMOUNT'] - df_dict[key]['REQUIRED_RESERVES'] - df_dict[key]['FEES']
                print("Tranform special cases for Purhcase report")    
    return df_dict, file_prefix
#Call function to read and transform Collections, Fundings, Account Overview, Purchases report
df_dict, file_prefix = read_files_transform_data_us_market()

#Load csv file into DF
df_aging = pd.read_csv(path + '/' + file_aging, skiprows=5)
df_ineligibles = pd.read_csv(path + '/' + file_ineligibles, skiprows=4)


print("Loaded to aging to df: ", df_aging.shape)

print("Loaded to ineligibles to df: ", df_ineligibles.shape)
#Rename columns and add run_time column
# Aging
df_aging.columns = [re.sub('[^A-Z0-9\s\-]','',new_col_name.upper()).strip().replace(' ','_').replace('-','_') for new_col_name in df_aging.columns]
df_aging['CHANNEL'] = 'US'
df_aging['RUN_TIME'] = datetime.now()
df_aging_final = df_aging.iloc[:-2]
df_aging_final = find_and_transform_date(df_aging_final)


# Ineligibles
df_ineligibles.columns = [re.sub('[^A-Z0-9\s\-\_]','',new_col_name.upper()).strip().replace(' ','_').replace('-','_') for new_col_name in df_ineligibles.columns]
df_ineligibles['CHANNEL'] = 'US'
df_ineligibles['RUN_TIME'] = datetime.now()
df_ineligibles_final = df_ineligibles.iloc[:-1]
df_ineligibles_final = find_and_transform_date(df_ineligibles_final)

# Available fund summary
# df_afs = download_available_fund_summary()
# df_afs['CHANNEL'] = 'US'



# print('Collection shape ',df_dict['Collections__'].shape)
# print('Funding shape ',df_dict['Fundings__'].shape)
# print('Aging shape ',df_aging_final.shape)
# print('Ineligibles shape ',df_ineligibles_final.shape)
# print('Overview shape ',df_dict['Overview-by-Date__'].shape)
# print('Purchases shape ',df_dict['Purchases__'].shape)
# print('Available fund summary ',df_afs.shape)


# Upload history
df_upload_history = download_upload_history_and_preprocess()
#Create new dataframe to split file_names into multiple rows
if df_upload_history is None:
    df_upload_history_dtl = None
else:
    df_upload_history_dtl = df_upload_history[['DATE','UPLOAD','FILE_NAMES','RUN_TIME','CHANNEL']]
    df_upload_history_dtl['FILE_NAMES'] = df_upload_history_dtl['FILE_NAMES'].str.split(r",")
    df_upload_history_dtl = df_upload_history_dtl.explode(['FILE_NAMES'])
    df_upload_history_dtl['INVOICE_NUMBER']=df_upload_history_dtl['FILE_NAMES'].str.upper().str.split(r"INVOICE_").str[1].str.split(r".PDF").str[0]
    df_upload_history_dtl['IS_INVOICE_DOCUMENT'] = df_upload_history_dtl['INVOICE_NUMBER'].isna().astype(int)
    df_upload_history_dtl = df_upload_history_dtl[['DATE','UPLOAD','FILE_NAMES','IS_INVOICE_DOCUMENT','INVOICE_NUMBER','RUN_TIME','CHANNEL']]

# Store data into MySQL

# dict_tables_us = {'LSQ_REPORT_AGING': df_aging_final,'LSQ_REPORT_COLLECTIONS':df_dict['Collections__'],\
#     'LSQ_REPORT_FUNDINGS':df_dict['Fundings__'],'LSQ_REPORT_INELIGIBLES':df_ineligibles_final\
#         ,'LSQ_REPORT_OVERVIEW':df_dict['Overview-by-Date__'], 'LSQ_REPORT_PURCHASES':df_dict['Purchases__']\
#             ,'LSQ_REPORT_AFS':df_afs, 'LSQ_REPORT_INVOICE_UPLOAD_HISTORY':df_upload_history,\
#                 'LSQ_REPORT_INVOICE_UPLOAD_HISTORY_DTL':df_upload_history_dtl}

dict_tables_us = {'LSQ_REPORT_AGING': df_aging_final,'LSQ_REPORT_COLLECTIONS':df_dict['Collections__'],\
    'LSQ_REPORT_FUNDINGS':df_dict['Fundings__'],'LSQ_REPORT_INELIGIBLES':df_ineligibles_final\
        ,'LSQ_REPORT_OVERVIEW':df_dict['Overview-by-Date__'], 'LSQ_REPORT_PURCHASES':df_dict['Purchases__']\
            , 'LSQ_REPORT_INVOICE_UPLOAD_HISTORY':df_upload_history,\
                'LSQ_REPORT_INVOICE_UPLOAD_HISTORY_DTL':df_upload_history_dtl}
engine_msql = create_engine('mysql+mysqlconnector://y4a_str_ro:HJ1s92!adznd@str-db.yes4all.internal:3306/y4a_analyst', echo=False)
usrnme_postgre = 'y4a_str_baoqt'
passwd_postgre = 'Jasd71!dn98h'
hstnme_postgre = 'datamart.yes4all.internal'
dbname_postgre = 'data_warehouse'
engine_postgre = create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(usrnme_postgre,passwd_postgre,hstnme_postgre,dbname_postgre))

def ingest_data_to_db(table_name,df):
    #ingest to MySQL
    if df is None: pass
    else:
        result = df.to_sql(name=table_name,con=engine_msql,index=False,if_exists='append')
        time.sleep(3)
        print("Ingest data to table {} in MySQL. Row affect: {}".format(table_name,result))
        #ingest to Postgre
        df_postgre = df.copy()
        df_postgre.columns = [i.lower() for i in df_postgre.columns]
        result = df_postgre.to_sql(name = table_name.lower(),con=engine_postgre,schema='y4a_fin',if_exists='append',index=False)
        print("Ingest data to table {} in Postgre. Row affect: {}".format(table_name.lower(),result))
        time.sleep(3)

for key in dict_tables_us:
    ingest_data_to_db(key,dict_tables_us[key])
print("Ingested data to DB - US")

#Define GGS connection & GGS APIs to be used

service_account_file = r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/analytics-data-update-97a3f7a00e57.json'
c = pg.authorize(service_account_file=service_account_file)
#Prepare input to call APIs
input_for_api = {
    'aging_final':{'spreadsheet_id':'1Zce2nUtyDdwX_nNvAvDDuhWtcfKxsPeg87SmuY3SP3I','body':df_aging_final},
    'collection_final':{'spreadsheet_id':'1YijOsFMQmuUHL_bv4Ov_c2SkwdSEGAvkTRo1-CgHqJs','body':df_dict['Collections__']},
    'funding_final':{'spreadsheet_id':'16KrJVna7VoBSiLmJYH7cgZ9ar4dCJrbhf-_RcCLArsQ','body':df_dict['Fundings__']},
    'ineligibles_final':{'spreadsheet_id':'1dkGxjWNT9Of2dmPJHIF97imyyQ-dTBi300euKnmqtJw','body':df_ineligibles_final},
    'overview_final':{'spreadsheet_id':'1FbW6j_Zt7niVsf2lBf1mGxaPoZDIp8_QkYNKOhE5VxE','body':df_dict['Overview-by-Date__']},
    'purchases_final':{'spreadsheet_id':'1lC4YglqdogyGfocRJKA1Z3WaL8yd99mNXnJC7RWymiI','body':df_dict['Purchases__']}
}

for report in input_for_api:
    #Open spreadsheet
    spreadsheet = c.open_by_key(input_for_api[report]['spreadsheet_id'])
    ws = spreadsheet.worksheet()
    #Clear existing data
    ws.clear(start='A2')
    print("Clear data from report: ")
    print("Result of cleaning data: ")
    #Convert datetime into string, because API does not allow datetime data type
    date_columns = input_for_api[report]['body'].select_dtypes(include=['datetime64[ns]']).columns.tolist()
    input_for_api[report]['body'][date_columns] =input_for_api[report]['body'][date_columns].astype(str)
    input_for_api[report]['body'] = input_for_api[report]['body'].fillna('')
    #Convert df into list of lists
    if len(input_for_api[report]['body'].values.tolist()) == 0:
        input_for_api[report]['body'] = ['' for value in input_for_api[report]['body'].columns]
    else:
        input_for_api[report]['body'] = input_for_api[report]['body'].values.tolist()
    #Add data to GGS
    ws.append_table(input_for_api[report]['body'])
    print("Insert data to report: ")
    print("Result of Inserting data: ")

#upload upload_history to google sheet
if df_upload_history is None:
    pass
else:
    spreadsheet = c.open_by_url('https://docs.google.com/spreadsheets/d/1K9cM6qSP3dhLUl8cHn0_0XqlsA93OPgoF9vGQgv6Qfk/edit#gid=0')
    ws = spreadsheet.worksheet()
    #Change upload_history datatype to fit with GGSheet requirements
    df_upload_history['DATE']=df_upload_history['DATE'].dt.strftime('%m-%d-%Y %H:%M:%S')
    df_upload_history['RUN_TIME']=df_upload_history['RUN_TIME'].dt.strftime('%m-%d-%Y %H:%M:%S')
    df_upload_history.fillna('',inplace=True)
    print("transform upload history to upload ggs")
    ws.clear(start='A2')
    print("clear all rows")
    #upload upload_history to google sheet
    ws.append_table(df_upload_history.values.tolist())
    print("Add new rows")


#xoa file pdf da detect thanh cong

path_report_downloaded = r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/download_report/*'
files = glob.glob(path_report_downloaded)
for f in files:
    os.remove(f)
print("Deleted files")


#_________________________________________________________FOREIGN PART__________________________________________
#Double check download folder
if len(os.listdir(r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/download_report')) != 0 :
    files = glob.glob(path_report_downloaded)
    for f in files:
        os.remove(f)
else : pass

def login_llc_foreign():
    current_channel = ''
    print(current_channel)
    while current_channel != 'Yes4All LLC - Foreign' :
        #go to select company
        driver.get ('https://dashboard.lsq.com/select-company')
        time.sleep(5)
        print("Go back to select company")
        #Yes4All LLC - Foreign
        wait.until(EC.element_to_be_clickable((By.XPATH,"//td[@class='icon login-arrow']")))
        driver.find_elements(By.XPATH,"//td[@class='icon login-arrow']")[1].click()
        print("Clicked International market")
        time.sleep(5)
        current_channel = driver.find_elements(By.XPATH, "//span[@class='ng-star-inserted']")[0].text
        print('In while loop: ',current_channel)

login_llc_foreign()
print('Succeed login LLC Foreign channel')

#Directory of each LSQ report
purchase = 'https://dashboard.lsq.com/reports/purchases'
overview_by_date = 'https://dashboard.lsq.com/reports/account-overview'
collections = 'https://dashboard.lsq.com/reports/collections'
funding = 'https://dashboard.lsq.com/reports/fundings'

# Check if Reports are downloaded to default folder, if no -> download report, if yes -> do nothing

attempts = 0
while attempts < 5:

    list_file_download = os.listdir(r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/download_report')
    for i in range(0, len(list_file_download)):
        list_file_download[i] = list_file_download[i].split('_')[0].lower()

    # Download purchases rp
    if 'purchases' in list_file_download:
        pass
    else:
        download_csv_lsq(purchase)
    # Download overview rp
    if 'overview-by-date' in list_file_download:
        pass
    else:
        download_csv_lsq(overview_by_date)
    # Download collection rp
    if 'collections' in list_file_download:
        pass
    else:
        download_csv_lsq(collections)
    # Download funding rp
    if 'fundings' in list_file_download:
        pass
    else:
        download_csv_lsq(funding)
    # Download aging rp
    if 'aging' in list_file_download:
        pass
    else:
        download_csv_lsq_aging()
    # Download ineligibles rp
    if 'ineligibles' in list_file_download:
        pass
    else:

        download_csv_lsq_ineligibles()
    if len(list_file_download) == 6:
        break
    else:
        attempts += 1

print("Downloaded reports")
#Move downloaded reports to other directory
# file_aging_int = ''
# file_collection_int = ''
# file_funding_int = ''
# file_ineligibles_int = ''
# file_overview_int= ''
# file_purchases_int = ''

path = r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/download_report'
list_dl = os.listdir(path)
for file in list_dl :
    if file.split('_')[0].lower() == 'aging' : file_aging_int = file
    elif file.split('_')[0].lower().replace('.csv','') == 'collections' : file_collection_int = file
    elif file.split('_')[0].lower().replace('.csv','') == 'fundings' : file_funding_int = file
    elif file.split('_')[0].lower().replace('.csv','') == 'ineligibles' : file_ineligibles_int = file
    elif file.split('_')[0].lower().replace('.csv','') == 'overview-by-date' : file_overview_int = file
    elif file.split('_')[0].lower().replace('.csv','') == 'purchases' : file_purchases_int = file
    else : pass

def read_files_transform_data_int_market():
    file_prefix = ["Collections__","Fundings__","Overview-by-Date__","Purchases__"]
    df_dict = {i:None for i in file_prefix}
    for file in os.listdir(path):
        for key in df_dict:
            if file.startswith(key):
                #Read csv and skip top 4 rows
                df_dict[key] = pd.read_csv(os.path.join(path,file),skiprows=4)
                print("Load {} into dataframe".format(file))
                #Remove last row if first cell of the last row contains 'Totals'
                if df_dict[key].iloc[-1,0] == 'Totals':
                    df_dict[key] = df_dict[key][:-1]
                print("Remove Totals row")
                #Transform column name
                df_dict[key].columns = [re.sub('[^A-Z0-9\s\-]','',new_col_name.upper()).strip().replace(' ','_').replace('-','_') for new_col_name in df_dict[key].columns]
                print("Tranform columns names")
                #Transform all columns containing 'DATE' to datetime
                df_dict[key] = find_and_transform_date(df_dict[key])
                print("Convert data time column to datetime data type")
                #Add Channel
                df_dict[key]['CHANNEL'] = 'INT'
                print("Add US Channel")
                #Add run time
                df_dict[key]['RUN_TIME'] = datetime.now()
                print("Add run_time")
                #Transform Purchases
                if file.startswith('Purchases__'):
                    df_dict[key]['DATE'].fillna(inplace=True, method = 'ffill')
                    df_dict[key].dropna(inplace=True, subset=['CUSTOMER'])
                    df_dict[key]['REQUIRED_RESERVES'] = df_dict[key]['AMOUNT']*0.15
                    df_dict[key]['AVAILABLE_FUNDS'] = df_dict[key]['AMOUNT'] - df_dict[key]['REQUIRED_RESERVES'] - df_dict[key]['FEES']
                print("Tranform special cases for Purhcase report")    
    return df_dict, file_prefix

print("File names: ",'\n',file_aging_int,'\n', file_collection_int,'\n',file_funding_int,'\n',file_ineligibles_int,'\n',file_overview_int,'\n',file_purchases_int)
df_dict, file_prefix = read_files_transform_data_int_market()
#Load csv file into DF
df_aging = pd.read_csv(path + '/' + file_aging_int, skiprows=5)
df_ineligibles = pd.read_csv(path + '/' + file_ineligibles_int, skiprows=4)

print("Loaded to aging to df: ", df_aging.shape)
print("Loaded to collection to df: ", df_dict['Collections__'].shape)
print("Loaded to funding to df: ", df_dict['Fundings__'].shape)
print("Loaded to ineligibles to df: ", df_ineligibles.shape)
print("Loaded to overview to df: ", df_dict['Overview-by-Date__'].shape)
print("Loaded to purchases to df: ", df_dict['Purchases__'].shape)


#Rename columns and add run_time column
# Aging
df_aging.columns = [re.sub('[^A-Z0-9\s\-]','',col.upper()).strip().replace(' ','_').replace('-','_') for col in df_aging.columns]
df_aging['CHANNEL'] = 'INT'
df_aging['RUN_TIME'] = datetime.now()
df_aging_final = df_aging.iloc[:-2]
df_aging_final =find_and_transform_date(df_aging_final)


# Ineligibles
df_ineligibles.columns = [re.sub('[^A-Z0-9\s\-\_]','',col.upper()).strip().replace(' ','_').replace('-','_') for col in df_ineligibles.columns]
df_ineligibles['CHANNEL'] = 'INT'
df_ineligibles['RUN_TIME'] = datetime.now()
df_ineligibles_final = df_ineligibles.iloc[:-1]
df_ineligibles_final = find_and_transform_date(df_ineligibles_final)

# Available fund summary
# df_afs_int = download_available_fund_summary()
# df_afs_int['CHANNEL'] = 'INT'

print('Collection shape ',df_dict['Collections__'].shape)
print('Funding shape ',df_dict['Fundings__'].shape)
print('Aging shape ',df_aging_final.shape)
print('Ineligibles shape ',df_ineligibles_final.shape)
print('Overview shape ',df_dict['Overview-by-Date__'].shape)
print('Purchases shape ',df_dict['Purchases__'].shape)
# print('Available fund summary ',df_afs_int.shape)

#Upload history
df_upload_history_int = download_upload_history_and_preprocess()
if df_upload_history_int is None:
    df_upload_history_dtl_int = None
else:
    df_upload_history_dtl_int = df_upload_history_int[['DATE','UPLOAD','FILE_NAMES','RUN_TIME','CHANNEL']]
    df_upload_history_dtl_int['FILE_NAMES'] = df_upload_history_dtl_int['FILE_NAMES'].str.split(r",")
    df_upload_history_dtl_int = df_upload_history_dtl_int.explode(['FILE_NAMES'])
    df_upload_history_dtl_int['INVOICE_NUMBER']=df_upload_history_dtl_int['FILE_NAMES'].str.upper().str.split(r"INVOICE_").str[1].str.split(r".PDF").str[0]
    df_upload_history_dtl_int['IS_INVOICE_DOCUMENT'] = df_upload_history_dtl_int['INVOICE_NUMBER'].isna().astype(int)
    df_upload_history_dtl_int = df_upload_history_dtl_int[['DATE','UPLOAD','FILE_NAMES','IS_INVOICE_DOCUMENT','INVOICE_NUMBER','RUN_TIME','CHANNEL']]

# Store data into MySQL


# dict_tables_int = {'LSQ_REPORT_AGING': df_aging_final,'LSQ_REPORT_COLLECTIONS':df_dict['Collections__'],\
#     'LSQ_REPORT_FUNDINGS':df_dict['Fundings__'],'LSQ_REPORT_INELIGIBLES':df_ineligibles_final\
#         ,'LSQ_REPORT_OVERVIEW':df_dict['Overview-by-Date__'], 'LSQ_REPORT_PURCHASES':df_dict['Purchases__']\
#             ,'LSQ_REPORT_AFS':df_afs_int, 'LSQ_REPORT_INVOICE_UPLOAD_HISTORY':df_upload_history_int,\
#                 'LSQ_REPORT_INVOICE_UPLOAD_HISTORY_DTL':df_upload_history_dtl_int}
dict_tables_int = {'LSQ_REPORT_AGING': df_aging_final,'LSQ_REPORT_COLLECTIONS':df_dict['Collections__'],\
    'LSQ_REPORT_FUNDINGS':df_dict['Fundings__'],'LSQ_REPORT_INELIGIBLES':df_ineligibles_final\
        ,'LSQ_REPORT_OVERVIEW':df_dict['Overview-by-Date__'], 'LSQ_REPORT_PURCHASES':df_dict['Purchases__']\
            , 'LSQ_REPORT_INVOICE_UPLOAD_HISTORY':df_upload_history_int,\
                'LSQ_REPORT_INVOICE_UPLOAD_HISTORY_DTL':df_upload_history_dtl_int}
engine_msql = create_engine('mysql+mysqlconnector://y4a_str_ro:HJ1s92!adznd@str-db.yes4all.internal:3306/y4a_analyst', echo=False)
usrnme_postgre = 'y4a_str_baoqt'
passwd_postgre = 'Jasd71!dn98h'
hstnme_postgre = 'datamart.yes4all.internal'
dbname_postgre = 'data_warehouse'
engine_postgre = create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(usrnme_postgre,passwd_postgre,hstnme_postgre,dbname_postgre))

for key in dict_tables_int:
    ingest_data_to_db(key,dict_tables_int[key])    
# df_aging_final.to_sql(name='LSQ_REPORT_AGING', con=engine, if_exists = 'append', index=False)
# print('Upload aging report - INT ')
# time.sleep(3)
# df_dict['Collections__'].to_sql(name='LSQ_REPORT_COLLECTIONS', con=engine, if_exists = 'append', index=False)
# print('Upload collection report - INT ')
# time.sleep(3)
# df_dict['Fundings__'].to_sql(name='LSQ_REPORT_FUNDINGS', con=engine, if_exists = 'append', index=False)
# print('Upload funding report - INT ')
# time.sleep(3)
# df_ineligibles_final.to_sql(name='LSQ_REPORT_INELIGIBLES', con=engine, if_exists = 'append', index=False)
# print('Upload ineligibles report - INT ')
# time.sleep(3)
# df_dict['Overview-by-Date__'].to_sql(name='LSQ_REPORT_OVERVIEW', con=engine, if_exists = 'append', index=False)
# print('Upload overview report - INT ')
# time.sleep(3)
# df_dict['Purchases__'].to_sql(name='LSQ_REPORT_PURCHASES', con=engine, if_exists = 'append', index=False)
# print('Upload purchases report - INT ')
# time.sleep(3)
# df_afs_int.to_sql(name='LSQ_REPORT_AFS', con=engine, if_exists = 'append', index=False)
# print('Upload available fund summary report - INT ')
# time.sleep(3)
# if df_upload_history_int is None: pass
# else :
#     df_upload_history_int.to_sql(name='LSQ_REPORT_INVOICE_UPLOAD_HISTORY', con=engine, if_exists = 'append', index=False)
#     print('Upload invoice upload history report - INT ')
#     time.sleep(3)

# if df_upload_history_dtl_int is None: pass
# else :
#     df_upload_history_dtl_int.to_sql(name='LSQ_REPORT_INVOICE_UPLOAD_HISTORY_DTL', con=engine, if_exists = 'append', index=False)
#     print('Upload invoice upload history report - INT ')
    # time.sleep(3)
    

print("Ingested data to DB - INT")
#Insert International channel to GGS

#Define GGS connection & GGS APIs to be used
service_account_file = r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/analytics-data-update-97a3f7a00e57.json'
c = pg.authorize(service_account_file=service_account_file)
#Prepare input to call APIs
input_for_api = {
    'aging_final':{'spreadsheet_id':'1Zce2nUtyDdwX_nNvAvDDuhWtcfKxsPeg87SmuY3SP3I','body':df_aging_final},
    'collection_final':{'spreadsheet_id':'1YijOsFMQmuUHL_bv4Ov_c2SkwdSEGAvkTRo1-CgHqJs','body':df_dict['Collections__']},
    'funding_final':{'spreadsheet_id':'16KrJVna7VoBSiLmJYH7cgZ9ar4dCJrbhf-_RcCLArsQ','body':df_dict['Fundings__']},
    'ineligibles_final':{'spreadsheet_id':'1dkGxjWNT9Of2dmPJHIF97imyyQ-dTBi300euKnmqtJw','body':df_ineligibles_final},
    'overview_final':{'spreadsheet_id':'1FbW6j_Zt7niVsf2lBf1mGxaPoZDIp8_QkYNKOhE5VxE','body':df_dict['Overview-by-Date__']},
    'purchases_final':{'spreadsheet_id':'1lC4YglqdogyGfocRJKA1Z3WaL8yd99mNXnJC7RWymiI','body':df_dict['Purchases__']}
}

for report in input_for_api:
    #Open spreadsheet
    spreadsheet = c.open_by_key(input_for_api[report]['spreadsheet_id'])
    ws = spreadsheet.worksheet()
    #Convert datetime into string, because API does not allow datetime data type
    date_columns = input_for_api[report]['body'].select_dtypes(include=['datetime64[ns]']).columns.tolist()
    input_for_api[report]['body'][date_columns] =input_for_api[report]['body'][date_columns].astype(str)
    input_for_api[report]['body'] = input_for_api[report]['body'].fillna('')
    #Convert df into list of lists
    if len(input_for_api[report]['body'].values.tolist()) == 0:
        input_for_api[report]['body'] = ['' for value in input_for_api[report]['body'].columns]
    else:
        input_for_api[report]['body'] = input_for_api[report]['body'].values.tolist()
    #Add data to GGS
    ws.append_table(input_for_api[report]['body'])
    print("Insert data to report: ")
    print("Result of Inserting data: ")

#upload upload_history to google sheet
if df_upload_history_int is None:
    pass
else:
    spreadsheet = c.open_by_url('https://docs.google.com/spreadsheets/d/1K9cM6qSP3dhLUl8cHn0_0XqlsA93OPgoF9vGQgv6Qfk/edit#gid=0')
    ws = spreadsheet.worksheet()
    #Change upload_history datatype to fit with GGSheet requirements
    df_upload_history_int['DATE']=df_upload_history_int['DATE'].dt.strftime('%m-%d-%Y %H:%M:%S')
    df_upload_history_int['RUN_TIME']=df_upload_history_int['RUN_TIME'].dt.strftime('%m-%d-%Y %H:%M:%S')
    df_upload_history_int.fillna('',inplace=True)
    print("Transform upload history to upload ggs")
    #upload upload_history to google sheet
    ws.append_table(df_upload_history_int.values.tolist())

#xoa file pdf da detect thanh cong

path_report_downloaded = r'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/download_report/*'
files = glob.glob(path_report_downloaded)
for f in files:
    os.remove(f)
print("Delete files ")
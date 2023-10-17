from sqlalchemy import create_engine
from datetime import datetime
import pandas as pd
import os
import re
import sys
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from googleapiclient.discovery import build
from selenium.webdriver.chrome.service import Service as ChromiumService
from google.oauth2 import service_account
from sqlalchemy.orm import sessionmaker
import pygsheets 
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


gc = pygsheets.authorize(service_file=json_path)

spreadsheet_key = '1paClpbDr76Js3YwLv8GYS8axHXjNZw8pv0zhIzNdPYI'
sh = gc.open_by_key(spreadsheet_key)

# GET DATA FROM INPUT_SHEET
sheet_name = 'Sheet1'
wks_input = sh.worksheet_by_title(sheet_name)

sheetAll = wks_input.get_all_records(empty_value='', head=1, majdim='ROWS', numericise_data=True)

df_code = pd.DataFrame.from_dict(sheetAll)
options = webdriver.ChromeOptions()
# options.add_argument('--headless')
options.add_argument('--no-sandbox')  # Add this line to fix sandbox issues (if any)
options.add_argument('--disable-dev-shm-usage')  # Add this line to fix issues with /dev/shm
options.add_argument("start-maximized")
options.add_argument("--incognito")
options.add_argument("--window-size=1920,1080")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
# options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")

# s = ChromiumService(ChromeDriverManager().install())
# driver = webdriver.Chrome(service=s, options=options)

# chrome_driver_path = '/airflow/data/Documents/GitHub/Y4A_BA_Team/BI_Team/DinhNP/Airflow_chromedriver/chromedriver.exe'
chrome_driver_path= 'C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/chrome_driver/chromedriver.exe'
# Create the Chrome WebDriver with the specified options and executable path
driver = webdriver.Chrome(options=options
    # options=options
                          )
wait = WebDriverWait(driver, 10)

print('Open Chrome')
def site_login():
    id_login = 'actg.a2zcg@gmail.com'
    password_login = 'HyVong_100tr2'
    wait.until(EC.element_to_be_clickable((By.ID,"emailAddress")))
    time.sleep(6)
    driver.find_elements(By.ID, 'emailAddress')[0].send_keys(id_login)
    driver.find_elements(By.ID, 'password')[0].send_keys(password_login)
    driver.find_elements(By.CLASS_NAME, 'lsq-button-block')[0].click()

driver.get('https://dashboard.lsq.com/login')
print('Go to website')
time.sleep(20)

# driver.fullscreen_window()
site_login()
time.sleep(5)

wait.until(EC.element_to_be_clickable((By.CLASS_NAME,"lsq-button-block")))
time.sleep(5)
driver.find_elements(By.CLASS_NAME,'lsq-button-block')[1].click()
print('Login succeed')

# df_code = pd.read_csv('C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/code.csv')
backupcode = df_code['CODE'][0]
print('Use backupcode: ',backupcode)

wait.until(EC.element_to_be_clickable((By.ID,"backupCode")))
#Nhap backupcode
driver.find_elements(By.ID, 'backupCode')[0].send_keys(backupcode)
time.sleep(30)
print('Input backupcode succeed ' )

#Click dang nhap sau khi nhap backup code
driver.find_elements(By.CLASS_NAME,'lsq-button-block')[0].click()
print('Click succeed : Confirm backupcode ')

time.sleep(20)

driver.find_element(By.CLASS_NAME, 'icon.login-arrow').click()

# print('Join channel succeed')
# wait.until(EC.element_to_be_clickable((By.XPATH,"//*[@id='company-selection-page']/div/div/card/div/div[2]/div/div[2]/table/tbody/tr[1]/td[4]/lsq-icon").click()
print('Join channel succeed')

time.sleep(10)

#Click cancel update phone number
wait.until(EC.element_to_be_clickable((By.XPATH,"//button[@class='button-secondary button-size-auto cancel-btn pull-left']"))).click()
print('Click succeed : cancel update phone number ')

time.sleep(10)

#dropdown profile
wait.until(EC.element_to_be_clickable((By.ID,"account-dropdown-toggle-icon"))).click()
print('Dropdown profile')

#select profile setting
wait.until(EC.element_to_be_clickable((By.ID,"profile-settings-navigation-item"))).click()
print('Select profile setting')
time.sleep(6)
wait.until(EC.element_to_be_clickable((By.XPATH,"//lsq-button[@id='generate-code-button']"))).click()
print('Generate Code')
#select "Generate New Backup Codes" button
wait.until(EC.element_to_be_clickable((By.XPATH,"//lsq-button[@class='modal-footer-right lsq-button-block ng-star-inserted']"))).click()
print('Select "Confirm" button')

time.sleep(6)
#select "Confirm" button



time.sleep(6)
#Tick vào ô saved code
wait.until(EC.element_to_be_clickable((By.XPATH,"//div[@class='checkbox-icon unchecked']"))).click()
print('Click saved code')



time.sleep(5)
code_list= []
hash_list = []
backup_code = driver.find_elements(By.XPATH,"//div[@class='ng-star-inserted']")
get_code = [x.text for x in backup_code if len(x.text) > 0]
code_list.append(get_code)
print('Code list: \n',code_list)
print('Code qty: ',len(code_list[0]))
for code in code_list[0]:
    print(code)
    hash_val = hash(code)
    hash_list.append(hash_val)

#Click save and Continue
wait.until(EC.element_to_be_clickable((By.XPATH,"//button[@class='button-primary button-size-block ok-btn']"))).click()
print('Click Save and Continue')


#Create Dataframe
df = pd.DataFrame(code_list[0],columns =['CODE'])
list_user = ['BOT1','BOT2','USER1','USER2','USER3','USER4','USER5','USER6','USER7','USER8']
df['USER'] = list_user
df['HASH_CODE'] = hash_list
df['RUN_TIME'] = datetime.now()
df.to_csv('C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/code.csv',index = False )
df






#Trigger authentication in GG
# creds = service_account.Credentials.from_service_account_file(r'C:\Users\research-str\PycharmProjects\pythonProject\LSQ\analytics-data-update-97a3f7a00e57.json')
client = pygsheets.authorize(service_file="C:/Users/baotd/Documents/GitHub/Y4A_BA_Team/BA_Team/BaoTran/BOT/LSQ/analytics-data-update-97a3f7a00e57.json")
#Input to call spreadsheet append api
#Value to input to append spreadsheet; must be list of list data type, each child list represents a row
df['RUN_TIME'] = df['RUN_TIME'].astype(str)
values = df.values.tolist()

#ID of the GG sheet; you can get it from GG sheet URL
spreadsheet = client.open_by_url(r"https://docs.google.com/spreadsheets/d/1paClpbDr76Js3YwLv8GYS8axHXjNZw8pv0zhIzNdPYI/edit#gid=0")
ws = spreadsheet.worksheet_by_title("Sheet1")
ws.append_table(values)
ws.sort_range("A2","D100000",3,'DESCENDING')



#MySQL connection string
mysql_user = 'y4a_str_ro_6'
mysql_password = 'PQ!2klnsd22'
mysql_host = 'str-db.yes4all.internal'
mysql_port = '3306'
mysql_db = 'y4a_analyst'
connection = 'mysql://'+mysql_user+':'+mysql_password+'@'+mysql_host+':'+mysql_port+'/'+mysql_db
connection
print('Connected to Mysql DB')

#Postgresql connection string
usrnme_postgre = 'y4a_str_baoqt'
passwd_postgre = 'Jasd71!dn98h'
hstnme_postgre = 'datamart.yes4all.internal'
dbname_postgre = 'data_warehouse'
con_postgre = create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(usrnme_postgre,passwd_postgre,hstnme_postgre,dbname_postgre))
print('Connected to Postgre DB')


#Connect to MySQL DB - schema y4a_analyst
engine = create_engine(connection,echo = True)
df2 = df.drop('CODE',axis=1)
print('Drop column CODE ')

result = df2.to_sql(name='LSQ_BACKUP_CODE_LOG', con=engine, if_exists = 'append', index=False)
print('Ingest data to MySQL DB. Row affected: {}'.format(result))
df2.columns = [i.lower() for i in df2.columns]
result= df2.to_sql(name='lsq_backup_code_log',con=con_postgre,schema='y4a_fin', if_exists='append',index=False )
print('Ingest data to Postgresql DB. Row affected: {}'.format(result))





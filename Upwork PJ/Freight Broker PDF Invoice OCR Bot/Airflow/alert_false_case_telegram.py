# =========================================================================
# Bat buoc phai copy doan code nay truoc khi xai
import pandas as pd
from datetime import datetime
from pandas import DataFrame
from sqlalchemy import create_engine
import psycopg2
from googleapiclient.discovery import build
from sqlalchemy.orm import declarative_base
import pygsheets
import warnings
import os
import re
import urllib
import datetime
import sys
import requests
warnings.filterwarnings("ignore")
from sqlalchemy import create_engine
abs_path = os.path.dirname(__file__)
main_cwd = re.sub('Y4A_BA_Team.*','Y4A_BA_Team',abs_path)
os.chdir(main_cwd)
sys.path.append(main_cwd)
from Shared_Lib import pcconfig
from Shared_Lib import shared_function as sf

# =========================================================================`    `


pcconfig = pcconfig.Init()


host = pcconfig['serverip_postgre']
name = pcconfig['postgre_name']
passwd = pcconfig['postgre_passwd']
db = pcconfig['postgre_db']
json_path = pcconfig['json_path']
###


gc = pygsheets.authorize(service_file=json_path)

spreadsheet_key = '1TGBp_S9jt68gu3ohrQh5CjKio1SYYguFOMpeCOjRMZs'
sh = gc.open_by_key(spreadsheet_key)

# GET DATA FROM INPUT_SHEET
sheet_name = 'Broker Freight Error'
wks_input = sh.worksheet_by_title(sheet_name)

sheetAll = wks_input.get_all_records(empty_value='', head=1, majdim='ROWS', numericise_data=True)
ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

df = pd.DataFrame.from_dict(sheetAll)

query = '''SELECT 
    distinct pdf_file_path from y4a_erp.y4a_erp_actual_broker'''
actual_broker_db = sf.QueryPostgre(sql= query
, host=host, passwd=passwd, name=name, db=db)

current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

filtered_df = df[~df['pdf_file_path'].isin(actual_broker_db['pdf_file_path'])]

# alert_message = f"[ERPxDE] QA Data Alert !!! - {current_time} \n"
# alert_message += f"[REGION]: USA, ESP, ITA, FRA, DEU, MEX \n\n"
# alert_message += f"Error Type\n"
# alert_message += f"1. Not Exist in DE tables  {bot_error_count_1} invoices \n"
# alert_message += f"2. Sum vs Dtl Mismatch:  {bot_error_count_2} invoices \n"

# alert_message += f"\nPlease access this view to see the errors:"
# alert_message += f"\ny4a_qa.view_amz_avc_mismatch_inv_final \n\n"
# alert_message += f"3. Deleted Invoice  {bot_error_count_3} invoices \n"
# alert_message += f"\nPlease access this view to see the errors: "
# alert_message += f"\ny4a_analyst.amz_avc_deleted_inv_sum \n"
long_url = "https://docs.google.com/spreadsheets/d/1TGBp_S9jt68gu3ohrQh5CjKio1SYYguFOMpeCOjRMZs/edit#gid=1808355559"
short_url = requests.get(f"https://tinyurl.com/api-create.php?url={urllib.parse.quote(long_url)}").text



def send_to_telegram(message):
    apiToken = '6024721868:AAE0xbOBcyyxvEh-zldukGZSkPZyxVAgY48'
    chatID = '-954814693'
    apiURL = f'https://api.telegram.org/bot{apiToken}/sendMessage'
    try:
        response = requests.post(apiURL, json={'chat_id': chatID, 'text': message})
        print(response.text)
    except Exception as e:
        print(e)

# Count distinct pdf_file_path values that are marked as FALSE
false_pdf_count = len(filtered_df)

# Create a notification message
notification_message = f"📢 Notification: {false_pdf_count} PDF files have FALSE cases. Please review and fix."
notification_message += short_url
send_to_telegram(notification_message)
print('Notification sent to Telegram!')



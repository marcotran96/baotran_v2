# =========================================================================
# Bat buoc phai copy doan code nay truoc khi xai
import os
import re
import sys
import pandas as pd
import pygsheets
import datetime
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


gc = pygsheets.authorize(service_file=json_path)

spreadsheet_key = '1TGBp_S9jt68gu3ohrQh5CjKio1SYYguFOMpeCOjRMZs'
sh = gc.open_by_key(spreadsheet_key)

# GET DATA FROM INPUT_SHEET
sheet_name = 'Actual Freight, Broker'
wks_input = sh.worksheet_by_title(sheet_name)

sheetAll = wks_input.get_all_records(empty_value='', head=1, majdim='ROWS', numericise_data=True)

df = pd.DataFrame.from_dict(sheetAll)
df.drop(columns='y4alink', inplace=True)
df.drop(columns='freight_check', inplace=True)
df.drop(columns='broker_check', inplace=True)




fixed_join_key_in_db =  sf.QueryPostgre(sql="select join_key from y4a_erp.erp_actual_landing_cost_manual_input;"
                  , host=host, passwd=passwd, name=name, db=db)

fixed_join_keys = set(fixed_join_key_in_db['join_key'])

# Perform left anti-join to get rows from df where 'invoice_number' is not in fixed_pi_invoice_numbers
filtered_df = df[~df['list_access_key'].isin(fixed_join_keys)]

filtered_df.rename(columns={'list_access_key': 'join_key'}, inplace=True)

def insertDfToPostgre(df, schema_name, table_name, if_exists_method = 'append'):
    ### PostgreSQL Config ####
    # Credentials to Postgre SQL database connection
    hostname = host
    dbname = db
    uname = name
    pwd = passwd

    engine = create_engine(f'postgresql://{uname}:{pwd}@{hostname}/{dbname}')
    # Insert the dataframe into the PostgreSQL table using to_sql() method
    df.to_sql(schema = schema_name, name = table_name, con = engine, if_exists = if_exists_method, index=False)
import psycopg2

ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
filtered_df['data_updated_time'] = ct


date_columns = ['broker_invoice_date', 'freight_invoice_date']
filtered_df[date_columns] = filtered_df[date_columns].apply(pd.to_datetime, errors='coerce')
number_columns = ['duty', 'freight', 'broker']

# Convert the columns to numeric
filtered_df[number_columns] = filtered_df[number_columns].apply(pd.to_numeric, errors='coerce')


        

insertDfToPostgre(filtered_df,'y4a_erp','erp_actual_landing_cost_manual_input',if_exists_method='append')

print('Job done')
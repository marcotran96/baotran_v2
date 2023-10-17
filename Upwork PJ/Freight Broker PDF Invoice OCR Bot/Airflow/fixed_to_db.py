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
import numpy as np
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
sheet_name = 'Broker, Freight Fixed'
wks_input = sh.worksheet_by_title(sheet_name)

sheetAll = wks_input.get_all_records(empty_value='', head=1, majdim='ROWS', numericise_data=True)
ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

df = pd.DataFrame.from_dict(sheetAll)

df['run_time'] = ct

def insert_unique_rows(df, schema_name, table_name):
    # PostgreSQL Credentials
    hostname = host
    dbname = db
    uname = name
    pwd = passwd

    # Create a connection to the database using SQLAlchemy
    engine = create_engine(f'postgresql://{uname}:{pwd}@{hostname}/{dbname}')

    # Get the list of existing pdf_file_path values in the table
    existing_paths_query = f"SELECT distinct pdf_file_path FROM {schema_name}.{table_name};"
    existing_paths_df = pd.read_sql(existing_paths_query, con=engine)

    # Filter df to only include rows with pdf_file_path not in the existing_paths_df
    unique_df = df[~df['pdf_file_path'].isin(existing_paths_df['pdf_file_path'])]

    # Insert the filtered dataframe into the PostgreSQL table using to_sql()
    if not unique_df.empty:
        unique_df.to_sql(schema=schema_name, name=table_name, con=engine, if_exists='append', index=False)
        print("Data inserted successfully.")



insert_unique_rows(df,'y4a_erp','y4a_erp_actual_broker')


sheet_name_2 = 'Broker, Freight DTL Fixed'
wks_input_2 = sh.worksheet_by_title(sheet_name_2)

sheetAll_2 = wks_input_2.get_all_records(empty_value='', head=1, majdim='ROWS', numericise_data=True)
ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

df2 = pd.DataFrame.from_dict(sheetAll_2)

df2['run_time'] = ct


insert_unique_rows(df2,'y4a_erp','y4a_erp_actual_broker_detail')
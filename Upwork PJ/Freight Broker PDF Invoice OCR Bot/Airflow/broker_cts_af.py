# =========================================================================
# Bat buoc phai copy doan code nay truoc khi xai
import datetime 
import os
import re
import sys
import numpy as np
import datetime
import pandas as pd
import datetime
from sqlalchemy import create_engine
import PyPDF2
import warnings
import psycopg2
import pandas as pd
from io import StringIO
warnings.filterwarnings("ignore")
from sqlalchemy import create_engine
abs_path = os.path.dirname(__file__)
main_cwd = re.sub('Y4A_BA_Team.*','Y4A_BA_Team',abs_path)
os.chdir(main_cwd)
sys.path.append(main_cwd)
from Shared_Lib import pcconfig
from Shared_Lib import shared_function as sf
def insertDfToPostgre(df, schema_name, table_name, if_exists_method = 'append'):
            ### PostgreSQL Config ####
            # Credentials to Postgre SQL database connection
            hostname = host
            dbname = db
            uname = name
            pwd = passwd

            # Establish a connection to the database
            # conn = psycopg2.connect(
            #     host=hostname,
            #     database=dbname,
            #     user=uname,
            #     password=pwd
            # )
            
            engine = create_engine(f'postgresql://{uname}:{pwd}@{hostname}/{dbname}')
            # Insert the dataframe into the PostgreSQL table using to_sql() method
            df.to_sql(schema = schema_name, name = table_name, con = engine, if_exists = if_exists_method, index=False)
# =========================================================================`    `
ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("current time: ", ct)

pcconfig = pcconfig.Init()


host = pcconfig['serverip_postgre']
name = pcconfig['postgre_name']
passwd = pcconfig['postgre_passwd']
db = pcconfig['postgre_db']
json_path = pcconfig['json_path']

query = '''select
	access_key,
	attach_file_9
from
	rawdata.po_notice_form_list pnfl
where
	1 = 1
	and us_broker = 242
	and etd >= CURRENT_DATE - INTERVAL '6 months'
	and published = 1 
    and attach_file_9 is not null;'''
cts = sf.QueryPostgre(sql= query
, host=host, passwd=passwd, name=name, db=db)


# Check if the cts DataFrame is empty
if cts.empty:
    print("No data found in the cts DataFrame. Exiting the process.")
    sys.exit()  # Terminate the script

# Function to extract PDF file names
def extract_pdf_file_name(attach_file_9):
    if attach_file_9 and '.pdf' in attach_file_9:
        match = re.search(r'"(.*?\.pdf)"', attach_file_9)
        if match:
            return match.group(1)
    return 'NULL'

cts_list = cts['attach_file_9'].to_list()
# Create a DataFrame from cts_list (replace with your actual data)

# Apply the function to the 'attach_file_9' column and create a new 'file_name' column
cts['file_name'] = cts['attach_file_9'].apply(extract_pdf_file_name)

# Function to create file paths
def create_file_path(row):
    if row['file_name'] != 'NULL':
        return f'Y:/po_notice/{row["access_key"]}/_9/{row["file_name"]}'
    return None

# Apply the function to create the file_path column
cts['file_path'] = cts.apply(create_file_path, axis=1)

# Display the updated DataFrame with the new file_path column
print(cts)


# Define the regular expressions to capture the required information
invoice_date_pattern = r"INVOICE DATE (\d{2}-[A-Za-z]{3}-\d{2})"
ocean_bill_pattern = r"OCEAN BILL OF LADING ([A-Z\d]+) HOUSE BILL OF LADING"
all_data_details = pd.DataFrame()

data_details_by_line = pd.DataFrame()

pdf_file_paths = cts['file_path'].to_list()
pdf_file_paths = [path.replace(' ', '') if path else None for path in pdf_file_paths]
pdf_file_paths = [path.replace(' ', '') for path in pdf_file_paths if path]
for i in range(len(pdf_file_paths)):
    pdf_file_paths[i] = pdf_file_paths[i].replace('Y:/', '/airflow/finance/')
# Loop through the PDF file paths
for pdf_file_path in pdf_file_paths:
    if pdf_file_path:
    # Initialize an empty string to store the extracted text
        extracted_text = ""

        # Open the PDF file in read-binary mode
        with open(pdf_file_path, 'rb') as pdf_file:
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            # Loop through all pages in the PDF and extract text
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                extracted_text += page.extract_text()

    else:
        print("PDF file path is None. Skipping processing.")

    invoice_date_match = re.search(invoice_date_pattern, extracted_text)
    ocean_bill_match = re.search(ocean_bill_pattern, extracted_text)

    # Extract the matched information
    if invoice_date_match:
        invoice_date = invoice_date_match.group(1)
    else:
        invoice_date = "Not found"

    ocean_bill = "Not found"
    if ocean_bill_match:
        ocean_bill = ocean_bill_match.group(1)

    # Print the extracted information
    print("INVOICE DATE:", invoice_date)
    print("OCEAN BILL OF LADING:", ocean_bill)

    # text_lines = lines
    # # Join the lines to create the complete text
    # complete_text = ''.join(text_lines)

    # Find the starting point for processing
    start_index = extracted_text.find("DESCRIPTION CHARGES")

    # Find the ending point for processing
    end_index = extracted_text.find("TOTAL")

    # Get the relevant portion of the text
    relevant_text = extracted_text[start_index:end_index]

    # Split the relevant text into lines
    lines = relevant_text.split('\n')

    # Filter out the unwanted line
    filtered_lines = [line for line in lines if 'DESCRIPTION' not in line]

    # Join the filtered lines back into text
    filtered_text = '\n'.join(filtered_lines)
    # # Save the updated relevant text to a file
    # with open("C:\\Users\\baotd\\Desktop\\updated_relevant_text.txt", "w") as f:
    #     f.write(filtered_text)

    # Split the text into lines
    lines = filtered_text.split('\n')

    # Find the index of the line containing "Stop off fee waived"
    start_index = None
    for i, line in enumerate(lines):
        if "*Stop off fee waived *" in line:
            start_index = i
            break

    # If the line is found, remove the lines from start_index to the end
    if start_index is not None:
        lines = lines[:start_index]


    items = []
    prices = []

    # Regular expression pattern to match items and numeric values in the context of prices
    item_price_pattern = r'^(.*?)(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*$'

    # Loop through the filtered lines to extract items and prices
    for line in lines:
        # Try to match the item and price pattern in the line
        match = re.match(item_price_pattern, line)
        
        if match:
            item = match.group(1).strip()
            price = match.group(2).replace(',', '')  # Remove commas from the numeric value
            
            items.append(item)
            prices.append(price)

    # Create a dictionary from the lists
    data = {
        'description': items,
        'total_amount': prices
    }

    # Create a DataFrame from the dictionary
    df = pd.DataFrame(data)

    number_columns = ['total_amount']
    df[number_columns] = df[number_columns].apply(lambda a : a.str.replace('$','')
                                                                    .str.replace(',',''))
    #replace blank to NaN
    df[number_columns] = df[number_columns].apply(lambda x: x.str.strip()).replace('', np.nan)
    df[number_columns] = df[number_columns].apply(pd.to_numeric, errors='coerce')

    # List of keywords to filter
    keywords = ['Drayage Fee','CHASSIS SPLIT','CHASSIS FEE', 'Pier Pass Charges', 'International Freight'
                , 'Delivery Cartage', 'Port Waiting Time', 'Yard', 'US CUSTOMS', 'ISF CHARGE', 'Handling Charges'
                , 'Prepull fees', 'DROP FEE','Waiting Time','Yard Sotrage','Drop off','Delivery','yard storage'
                ,'Drop to Anaheim','drop off','YARD STORAGE','PER DIEM CHARGES','DROP TO ANAHEIM','DROP OFF TO ANAHEIM'
                ,'Drop Off', 'drop to Anaheim','DROP OFF','CLEAN TRUCK FEE']


    # Filter the DataFrame
    filtered_df = df[df['description'].str.contains('|'.join(keywords))]
    
    def custom_grouping(item):
        if 'Freight' in item:
            return 'FREIGHT'
        elif 'Duty' in item:
            return 'DUTY'
        else:
            return 'BROKER'

    # Apply the custom grouping function and sum the 'Total Amount'
    grouped_df = filtered_df.groupby(filtered_df['description'].apply(custom_grouping))['total_amount'].sum().reset_index()
    grouped_df['invoice_date'] = invoice_date
    grouped_df['master_bl'] = ocean_bill
    grouped_df['pdf_file_path'] = pdf_file_path

    all_data_details = pd.concat([all_data_details, grouped_df], ignore_index=True)

    filtered_df['pdf_file_path'] = pdf_file_path
    filtered_df['invoice_date'] = invoice_date
    data_details_by_line = pd.concat([data_details_by_line, filtered_df], ignore_index=True)

all_data = []

# Loop through the PDF file paths
for pdf_file_path in pdf_file_paths:
    if pdf_file_path:
        # Initialize an empty string to store the extracted text
        extracted_text = ""

        # Open the PDF file in read-binary mode
        with open(pdf_file_path, 'rb') as pdf_file:
            pdf_reader = PyPDF2.PdfReader(pdf_file)

            # Loop through all pages in the PDF and extract text
            for page_num in range(len(pdf_reader.pages)):
                page = pdf_reader.pages[page_num]
                extracted_text += page.extract_text()

        # Find the line containing "TOTAL USD"
        total_usd_line = None
        lines = extracted_text.split('\n')
        for line in lines:
            if "TOTAL USD" in line:
                total_usd_line = line.replace("TOTAL USD", "").strip()  # Replace "TOTAL USD" with blank and strip spaces
                break

        # Append the data to the list
        all_data.append({'pdf_file_path': pdf_file_path, 'total_usd_line': total_usd_line})

# Create a DataFrame from the list of dictionaries
all_data_sum = pd.DataFrame(all_data)
number_columns = ['total_usd_line']
all_data_sum[number_columns] = all_data_sum[number_columns].apply(lambda a : a.str.replace('$','')
                                                                .str.replace(',',''))
#replace blank to NaN
all_data_sum[number_columns] = all_data_sum[number_columns].apply(lambda x: x.str.strip()).replace('', np.nan)
all_data_sum[number_columns] = all_data_sum[number_columns].apply(pd.to_numeric, errors='coerce')

# Calculate the sum of 'Amount' values for each unique access_key in all_data_details
sum_amount_by_access_key = all_data_details.groupby('pdf_file_path')['total_amount'].sum().reset_index()
sum_amount_by_access_key.rename(columns={'total_amount': 'sum_amount'}, inplace=True)

# Merge the calculated sums back into the all_data_sum DataFrame based on 'access_key'
all_data_sum = all_data_sum.merge(sum_amount_by_access_key, on='pdf_file_path', how='left')

# Create a new column 'flag' to indicate if total_line_usd matches sum_amount
all_data_sum['flag'] = all_data_sum.apply(lambda row: row['total_usd_line'] == row['sum_amount'], axis=1)

all_data_details = all_data_details.merge(all_data_sum[['pdf_file_path', 'flag']], on='pdf_file_path', how='left')
# all_data_details.drop(['access_key'], axis=1, inplace=True)
all_data_details['channel'] = 'NON-DI'
all_data_details['name'] = 'CTS'

# Display the updated all_data_sum DataFrame with the 'flag' column
print(all_data_details)


false_data = all_data_details[all_data_details['flag'] == False]
actual_broker = sf.QueryPostgre(sql= 'select distinct pdf_file_path from y4a_erp.y4a_erp_actual_broker'
, host=host, passwd=passwd, name=name, db=db)
false_data = false_data[~false_data['pdf_file_path'].isin(actual_broker['pdf_file_path'])]
false_data['run_time'] = ct

spreadsheet_key_1 = '1TGBp_S9jt68gu3ohrQh5CjKio1SYYguFOMpeCOjRMZs'

false_data_to_gg = sf.Gsheet_working(spreadsheet_key_1, 'Broker Freight Error', json_file = json_path)
false_data_to_gg.Append_Current_Sheet(false_data)


true_data = all_data_details[all_data_details['flag'] == True]
true_data['invoice_date'] = pd.to_datetime(true_data['invoice_date'], format='%d-%b-%y')
true_data['invoice_date'] = true_data['invoice_date'].dt.strftime('%Y-%m-%d')

true_data['run_time'] = ct


def delete_data_from_table(table_name, condition):
    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(
            host=host,
            database=db,
            user=name,
            password=passwd
        )
        # Create a cursor
        cursor = connection.cursor()
        # Build the DELETE query
        delete_query = f"DELETE FROM {table_name} WHERE {condition};"
        # Execute the DELETE query
        cursor.execute(delete_query)
        # Commit the transaction
        connection.commit()
        print("Data deleted successfully.")
    except (Exception, psycopg2.Error) as error:
        print("Error while deleting data:", error)
    finally:
        # Close the cursor and connection
        if connection:
            cursor.close()
            connection.close()
            print("Database connection closed.")

condition = "invoice_date >= date_trunc('month',(select max(invoice_date) from y4a_erp.y4a_erp_actual_broker where channel = 'NON-DI' and name ='CTS') - interval '1 month') and channel = 'NON-DI' and name ='CTS'"
delete_data_from_table(table_name = 'y4a_erp.y4a_erp_actual_broker', condition=condition)

actual_broker = sf.QueryPostgre(sql="select distinct pdf_file_path from y4a_erp.y4a_erp_actual_broker", host=host, passwd=passwd, name=name, db=db)


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

# Assuming you have a 'true_data' DataFrame with the desired data
schema_name = 'y4a_erp'  # Replace with your schema name
table_name = 'y4a_erp_actual_broker'    # Replace with your table name

# Call the function to insert unique rows from true_data
insert_unique_rows(true_data, schema_name, table_name)



data_details_by_line = data_details_by_line.merge(all_data_sum[['pdf_file_path', 'flag']], on='pdf_file_path', how='left')
# all_data_details.drop(['access_key'], axis=1, inplace=True)
data_details_by_line['channel'] = 'NON-DI'
data_details_by_line['name'] = 'CTS'

true_data_by_line = data_details_by_line[data_details_by_line['flag'] == True]

true_data_by_line['invoice_date'] = pd.to_datetime(true_data_by_line['invoice_date'], format='%d-%b-%y')
true_data_by_line['invoice_date'] = true_data_by_line['invoice_date'].dt.strftime('%Y-%m-%d')

true_data_by_line['run_time'] = ct


condition2 = "invoice_date >= date_trunc('month',(select max(invoice_date) from y4a_erp.y4a_erp_actual_broker_detail where channel = 'NON-DI' and name ='CTS') - interval '1 month') and channel = 'NON-DI' and name ='CTS'"
delete_data_from_table(table_name = 'y4a_erp.y4a_erp.y4a_erp_actual_broker_detail', condition=condition2)


actual_broker_dtl = sf.QueryPostgre(sql="select distinct pdf_file_path from y4a_erp.y4a_erp_actual_broker_detail", host=host, passwd=passwd, name=name, db=db)

schema_name = 'y4a_erp'  # Replace with your schema name
table_name = 'y4a_erp_actual_broker_detail'    # Replace with your table name

# Call the function to insert unique rows from true_data
insert_unique_rows(true_data_by_line, schema_name, table_name)

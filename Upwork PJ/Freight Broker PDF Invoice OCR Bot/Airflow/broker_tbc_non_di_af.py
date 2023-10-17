# =========================================================================
# Bat buoc phai copy doan code nay truoc khi xai
import datetime 
import os
import re
import sys
import numpy as np
import datetime
import warnings
import pandas as pd
import datetime
from sqlalchemy import create_engine
import PyPDF2
import pandas as pd
from io import StringIO
import psycopg2
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

query = '''SELECT 
    access_key,
    attach_file_9
FROM rawdata.po_notice_form_list pnfl 
WHERE 1=1 
    AND us_broker = 231 
    AND etd >= CURRENT_DATE - INTERVAL '6 months'
    AND published = 1 
    and attach_file_9 is not null;'''
tbc_non_di = sf.QueryPostgre(sql= query
, host=host, passwd=passwd, name=name, db=db)
if tbc_non_di.empty:
    print("No data found in the cts DataFrame. Exiting the process.")
    sys.exit()  # Terminate the script

pattern = r'"([^"]*)"'
tbc_non_di['file_name'] = tbc_non_di['attach_file_9'].apply(lambda x: re.search(pattern, x).group(1) if re.search(pattern, x) else None)
# Assuming tbc_non_di is your DataFrame
tbc_non_di['file_path'] = 'Y:/po_notice/' + tbc_non_di['access_key'] + '/_9/' + tbc_non_di['file_name']
file_path_list= tbc_non_di['file_path'].to_list()
# Remove spaces from each element in the list
pdf_file_paths = [path.replace(' ', '') for path in file_path_list]

for i in range(len(pdf_file_paths)):
    pdf_file_paths[i] = pdf_file_paths[i].replace('Y:/', '/airflow/finance/')

all_data_details = pd.DataFrame()

data_details_by_line = pd.DataFrame()


for pdf_file_path in pdf_file_paths:
    # pdf_file_path = 'Y:/po_notice/1d64a1e01774485462a760a81667d6f8/_9/Y4A-OILAX38177NPSCSHSEL2300607ICAKINVODC.PDF'
# Initialize an empty string to store the extracted text
    extracted_text = ""

    # Open the PDF file in read-binary mode
    with open(pdf_file_path, 'rb') as pdf_file:
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        
        # Loop through all pages in the PDF and extract text
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            extracted_text += page.extract_text()

    # Write the extracted text to a text file
    # output_text_path = "C:\\Users\\baotd\\Desktop\\output_text.txt"
    # with open(output_text_path, "w", encoding="utf-8") as f:
    #     f.write(extracted_text)

    # Define a regex pattern to match the date format
    date_pattern = r'Invoice Date\s+(\w+\s+\d{1,2},\s+\d{4})'

    # Search for the date pattern in the extracted text
    date_match = re.search(date_pattern, extracted_text)

    # If a match is found, get the matched date value
    if date_match:
        invoice_date = date_match.group(1)
    else:
        print("Invoice Date not found")


    start_index = extracted_text.find("Container No")
    # Find the ending point for processing
    end_index = extracted_text.rfind("Remit To")
    # Get the relevant portion of the text
    relevant_text = extracted_text[start_index:end_index]
    # Remove the line containing 'Container No'
    relevant_text = "\n".join(line for line in relevant_text.split('\n') if 'Container No' not in line)
    # Write the extracted text to a text file
    # output_text_path = "C:\\Users\\baotd\\Desktop\\output_text.txt"
    # with open(output_text_path, "w", encoding="utf-8") as f:
    #     f.write(relevant_text)



    # Split the extracted text into lines
    lines = relevant_text.strip().split('\n')

    # Create an empty list to store data for the DataFrame
    data = []

    # Iterate through each line and extract data
    for line in lines:
        parts = line.split()  # Split the line into words
        if len(parts) >= 2:
            description = ' '.join(parts[:-1])  # Join words as description
            
            # Attempt to convert the last part to a float
            try:
                amount = float(parts[-1].replace(',', ''))  # Remove commas and convert to float
            except ValueError:
                amount = 0  # Replace non-numeric values with 0
            
            data.append({'Description': description, 'Amount': amount})

    # Create a DataFrame from the extracted data
    df = pd.DataFrame(data)
    df['Amount'] = df['Amount'].apply(lambda x: x if pd.to_numeric(x, errors='coerce') == x else str(x).replace(',', ''))

    # List of keywords to filter
    keywords = ['IN CHARGE','EXAM FEE','WAITING TIME','DRAYAGE CHARGE','DROP CHARGE','CHASSIS SPLIT'
                ,'STORAGE CHARGE','PIERPASS','HANDLING CHARGE','TRUCKING CHRAGE','ISF FEE','CUSTOMS CLEARANCE FEE'
                , 'FUEL SURCHARGE','MISCLLANEOUS','CLEAN TRUCK FEE','CHASSIS RENTAL','PORT CONGESTION FEE'
                ,'PRE-PULL','OCEAN FREIGHT CHARGE']

    # Filter the DataFrame
    filtered_df = df[df['Description'].str.contains('|'.join(keywords))]

    # Define a custom function to group descriptions
    def custom_grouping(description):
        if 'FREIGHT' in description:
            return 'FREIGHT'
        elif 'DUTY' in description:
            return 'DUTY'
        else:
            return 'BROKER'

    # Apply the custom grouping function and sum the 'Total Amount'
    grouped_df = filtered_df.groupby(filtered_df['Description'].apply(custom_grouping))['Amount'].sum().reset_index()
    
    # Rename the columns for clarity
    grouped_df.columns = ['description', 'total_amount']
    grouped_df['master_bl'] = 'not found'
    grouped_df['invoice_date'] = invoice_date
    grouped_df['pdf_file_path'] = pdf_file_path
    all_data_details = pd.concat([all_data_details, grouped_df], ignore_index=True)


    filtered_df.rename(columns = {'Description':'description','Amount':'total_amount'}, inplace=True)
    filtered_df['pdf_file_path'] = pdf_file_path
    filtered_df['invoice_date'] = invoice_date

    data_details_by_line= pd.concat([data_details_by_line, filtered_df], ignore_index=True)

    

    


all_data_sum = pd.DataFrame()
all_data = []

for pdf_file_path in pdf_file_paths:
    # pdf_file_path = 'Y:/po_notice/7c773bdde55be132521a83bafada3d3b/_9/Y4A-OILAX12100CTYOSHA01108641ICA1KINVODC.PDF'
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

        total_usd_line = None
        lines = extracted_text.split('\n')
        count_due_amount = 0
        for line in reversed(lines):  # Loop in reverse to find the second-to-last occurrence
            if "DUE AMOUNT" in line:
                count_due_amount += 1
                if count_due_amount == 2:  # Check if this is the second-to-last occurrence
                    total_usd_line = line.replace("DUE AMOUNT", "").strip()
                    break  # Stop after finding the second-to-last occurrence
            
        # Append the data to the list
        all_data.append({'pdf_file_path': pdf_file_path, 'total_usd_line': total_usd_line})

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

# Display the updated all_data_sum DataFrame with the 'flag' column
print(all_data_sum)

all_data_details = all_data_details.merge(all_data_sum[['pdf_file_path', 'flag']], on='pdf_file_path', how='left')
all_data_details['channel'] = 'NON-DI'
all_data_details['name'] = 'TBC'


false_data = all_data_details[all_data_details['flag'] == False]
actual_broker = sf.QueryPostgre(sql= 'select distinct pdf_file_path from y4a_erp.y4a_erp_actual_broker'
, host=host, passwd=passwd, name=name, db=db)
false_data = false_data[~false_data['pdf_file_path'].isin(actual_broker['pdf_file_path'])]
false_data['run_time'] = ct

spreadsheet_key_1 = '1TGBp_S9jt68gu3ohrQh5CjKio1SYYguFOMpeCOjRMZs'

false_data_to_gg = sf.Gsheet_working(spreadsheet_key_1, 'Broker Freight Error', json_file = json_path)
false_data_to_gg.Append_Current_Sheet(false_data)


true_data = all_data_details[all_data_details['flag'] == True]
# Convert invoice_date column to datetime format
true_data['invoice_date'] = pd.to_datetime(true_data['invoice_date'], format='%b %d, %Y')
# Convert invoice_date to YYYY-MM-DD format
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

condition = "invoice_date >= date_trunc('month',(select max(invoice_date) from y4a_erp.y4a_erp_actual_broker where channel = 'NON-DI' and name ='TBC') - interval '1 month') and channel = 'NON-DI' and name ='TBC'"
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
data_details_by_line['name'] = 'TBC'

true_data_by_line = data_details_by_line[data_details_by_line['flag'] == True]

true_data_by_line['invoice_date'] = pd.to_datetime(true_data_by_line['invoice_date'], format='%b %d, %Y')
# Convert invoice_date to YYYY-MM-DD format
true_data_by_line['invoice_date'] = true_data_by_line['invoice_date'].dt.strftime('%Y-%m-%d')

true_data_by_line['run_time'] = ct


condition2 = "invoice_date >= date_trunc('month',(select max(invoice_date) from y4a_erp.y4a_erp_actual_broker_detail where channel = 'NON-DI' and name ='TBC') - interval '1 month') and channel = 'NON-DI' and name ='TBC'"
delete_data_from_table(table_name = 'y4a_erp.y4a_erp.y4a_erp_actual_broker_detail', condition=condition2)


actual_broker_dtl = sf.QueryPostgre(sql="select distinct pdf_file_path from y4a_erp.y4a_erp_actual_broker_detail", host=host, passwd=passwd, name=name, db=db)

schema_name = 'y4a_erp'  # Replace with your schema name
table_name = 'y4a_erp_actual_broker_detail'    # Replace with your table name

# Call the function to insert unique rows from true_data
insert_unique_rows(true_data_by_line, schema_name, table_name)
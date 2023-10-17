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
import warnings
from sqlalchemy import create_engine
import PyPDF2
import pandas as pd
import psycopg2
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

query = '''SELECT 
    access_key,
    attach_file_9,
    attach_file_11
FROM rawdata.po_notice_form_list pnfl 
WHERE 1=1 
    AND us_broker = 230 
    AND etd >= CURRENT_DATE - INTERVAL '6 months'
    AND published = 1 ;'''
real = sf.QueryPostgre(sql= query
, host=host, passwd=passwd, name=name, db=db)

if real.empty:
    print("No data found in the cts DataFrame. Exiting the process.")
    sys.exit()  # Terminate the script

data_list = []

# Assuming real is your DataFrame containing the query result
for index, row in real.iterrows():
    attach_file_9_value = row['attach_file_9']
    attach_file_11_value = row['attach_file_11']
    
    # Function to extract values enclosed in double quotes
    def extract_values(data):
        return re.findall(r'"(.*?)"', data)
    
    # Extract values from attach_file_9
    if attach_file_9_value is not None and attach_file_9_value != "":
        attach_file_9_values = [value for value in extract_values(attach_file_9_value) if value.endswith('.pdf')]
    else:
        attach_file_9_values = []
    
    # Extract values from attach_file_11
    if attach_file_11_value is not None and attach_file_11_value != "":
        attach_file_11_values = [value for value in extract_values(attach_file_11_value) if value.endswith('.pdf')]
    else:
        attach_file_11_values = []
    
    # Append the extracted values to the data_list
    data_list.append({'attach_file_9_values': attach_file_9_values,
                      'attach_file_11_values': attach_file_11_values})

# Create a DataFrame from the list of dictionaries
new_real = pd.DataFrame(data_list)


# Create an empty list to store dictionaries
final_data_list = []

# Assuming real and new_real are your DataFrames
for index, (real_row, new_real_row) in enumerate(zip(real.iterrows(), new_real.iterrows())):
    real_index, real_row_data = real_row
    new_real_index, new_real_row_data = new_real_row
    
    # Extract the access_key from the real DataFrame
    access_key = real_row_data['access_key']
    
    # Split combined values back into attach_file_9 and attach_file_11
    attach_file_9_values = []
    attach_file_11_values = []
    for value in new_real_row_data['attach_file_9_values']:
        if value.endswith('.pdf'):
            attach_file_9_values.append(value)
    for value in new_real_row_data['attach_file_11_values']:
        if value.endswith('.pdf'):
            attach_file_11_values.append(value)
    
    # Create a dictionary with access_key and split values
    final_data_list.append({'access_key': access_key,
                            'attach_file_9_values': attach_file_9_values,
                            'attach_file_11_values': attach_file_11_values})

# Create a DataFrame from the list of dictionaries
final_data = pd.DataFrame(final_data_list)

# Define the base directory
base_directory = 'Y:/po_notice/'

# Create an empty list to store the file paths
file_paths = []

# Assuming final_data is your DataFrame
for index, final_data_row in final_data.iterrows():
    access_key = final_data_row['access_key']
    
    # Loop through attach_file_9_values and create paths
    for file_name in final_data_row['attach_file_9_values']:
        file_path = os.path.join(base_directory, access_key, '_9', file_name)
        file_paths.append(file_path)
        
    # Loop through attach_file_11_values and create paths
    for file_name in final_data_row['attach_file_11_values']:
        file_path = os.path.join(base_directory, access_key, '_11', file_name)
        file_paths.append(file_path)

# Clean up file paths by replacing extra spaces

for i in range(len(file_paths)):
    file_paths[i] = file_paths[i].replace('Y:/', '/airflow/finance/')
file_paths = [path.replace(' ', '') for path in file_paths]
all_data_details = pd.DataFrame()
data_details_by_line = pd.DataFrame()
mb_l_pattern = r'M-B/ L No\s*\.?[\s:]+([A-Za-z0-9\-]+)'
issue_date_pattern = r'Issue Date\s*:\s*([A-Za-z]{3}\s\d{2},\s\d{4})'
# Display the list of file paths
print(file_paths)

for pdf_file_path in file_paths:
# Define the PDF file path
    # pdf_file_path = 'Y:\\po_notice\\137c36e01e190014b85290c90ed53000\\_11\\221671-OF-DN-RLLGB2306007.pdf'

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

    # print("Text extracted and saved to:", output_text_path)


    # with open("C:\\Users\\baotd\\Desktop\\output_text.txt", 'r') as file:
    #     lines = file.readlines()
    # print(lines)

    # extracted_text = ''.join(lines)

    # Define the regular expressions to capture the required information


    # Find matches using the regular expressions
    mb_l_match = re.search(mb_l_pattern, extracted_text)
    issue_date_match = re.search(issue_date_pattern, extracted_text)

    # Extract the matched information
    if mb_l_match:
        mb_l_no = mb_l_match.group(1)
    else:
        mb_l_no = "Not found"

    if issue_date_match:
        issue_date = issue_date_match.group(1)
    else:
        issue_date = "Not found"

    # Print the extracted information
    print("M-B/ L No:", mb_l_no)
    print("Issue Date:", issue_date)
    

    # text_lines = lines
    # # Join the lines to create the complete text
    # complete_text = ''.join(text_lines)

    # Find the starting point for processing
    start_index = extracted_text.find("Description")

    # Find the ending point for processing
    end_index = extracted_text.find("TOTAL")

    # Get the relevant portion of the text
    relevant_text = extracted_text[start_index:end_index]


    # Split the relevant text into lines
    lines = relevant_text.split('\n')

    # Filter out the unwanted lines
    filtered_lines = [line for line in lines if '+' not in line and 'Description' not in line and 'Total' not in line and 'page 1/2' not in line]

    # Join the filtered lines back into text
    filtered_text = '\n'.join(filtered_lines)


    # Split the filtered text into lines
    lines = filtered_text.split('\n')

    # Initialize lists to store data
    quantities = []
    prices = []
    total_prices = []
    units = []
    descriptions = []

    # Loop through each line and extract the required data
    for line in lines:
        parts = line.split()
        if len(parts) >= 4:
            quantities.append(parts[0])
            prices.append(parts[1])
            total_prices.append(parts[2])
            units.append(' '.join(parts[3:-1]))
            descriptions.append(parts[-1])

    # Create a dictionary from the extracted data
    data = {
        'Quantity': quantities,
        'Currency': prices,
        'Total Amount': total_prices,
        'Unit': units,
        'Description': descriptions
    }

    # Create a DataFrame from the dictionary
    df = pd.DataFrame(data)
    number_columns = ['Total Amount']
    df[number_columns] = df[number_columns].apply(lambda a : a.str.replace('$','')
                                                                    .str.replace(',',''))
    #replace blank to NaN
    df[number_columns] = df[number_columns].apply(lambda x: x.str.strip()).replace('', np.nan)
    df[number_columns] = df[number_columns].apply(pd.to_numeric, errors='coerce')

    # Define a custom function to group descriptions
    def custom_grouping(description):
        if 'FREIGHT' in description:
            return 'FREIGHT'
        elif 'DUTY' in description:
            return 'DUTY'
        else:
            return 'BROKER'

    # Apply the custom grouping function and sum the 'Total Amount'
    grouped_df = df.groupby(df['Description'].apply(custom_grouping))['Total Amount'].sum().reset_index()

    # Rename the columns for clarity
    grouped_df.columns = ['description', 'total_amount']
    grouped_df['master_bl'] = mb_l_no
    grouped_df['invoice_date'] = issue_date
    grouped_df['pdf_file_path'] = pdf_file_path
    print(grouped_df)
    all_data_details = pd.concat([all_data_details, grouped_df], ignore_index=True)
    new_df = pd.DataFrame({
    'description': df['Unit'] + ' ' + df['Description'],
    'total_amount': df['Total Amount']
        })
    new_df['pdf_file_path'] = pdf_file_path
    new_df['invoice_date'] = issue_date
    data_details_by_line = pd.concat([data_details_by_line, new_df], ignore_index=True)


all_data_sum = pd.DataFrame()
all_data = []

# Loop through the PDF file paths
for pdf_file_path in file_paths:
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

        # Find the line containing "USD TOTAL" or "TOTAL USD"
        total_usd_line = None
        lines = extracted_text.split('\n')
        for line in lines:
            if "USD TOTAL" in line:
                total_usd_line = line.replace("USD TOTAL", "").strip()
                break
            
        # Append the data to the list
        all_data.append({'pdf_file_path': pdf_file_path, 'total_usd_line': total_usd_line})
       
# Convert the list of dictionaries to a DataFrame
all_data_sum = pd.DataFrame(all_data)
all_data_sum['total_usd_line'] = all_data_sum['total_usd_line'].str[:10]
all_data_sum['total_usd_line'] = all_data_sum['total_usd_line'].apply(lambda x: 0 if x == 'None' else x)

number_columns = ['total_usd_line']
all_data_sum['total_usd_line'] = all_data_sum['total_usd_line'].str.replace('[^\d.]', '', regex=True)
all_data_sum['total_usd_line'] = pd.to_numeric(all_data_sum['total_usd_line'], errors='coerce')

# Clean and format the total_amount values
all_data_sum['total_usd_line'] = all_data_sum['total_usd_line'].fillna(0)  # Fill NaN with 0
all_data_sum['total_usd_line'] = all_data_sum['total_usd_line'].astype(float)  # Convert to float
# Calculate the sum of total_amount from all_data_details by pdf_file_path
total_amount_by_path = all_data_details.groupby('pdf_file_path')['total_amount'].sum().reset_index()

# Merge the calculated total_amount with all_data_sum
all_data_sum = all_data_sum.merge(total_amount_by_path, how='left', left_on='pdf_file_path', right_on='pdf_file_path')
all_data_sum['difference'] = all_data_sum['total_usd_line'] - all_data_sum['total_amount']
all_data_sum['difference'] = all_data_sum['difference'].round(2)  # Rounding to 2 decimal places

# Create a flag to compare total_usd_line and total_amount_sum
all_data_sum['flag'] = all_data_sum['difference'] == 0
all_data_details = all_data_details.merge(all_data_sum[['pdf_file_path', 'flag']], on='pdf_file_path', how='left')
all_data_details['channel'] = 'NON-DI'
all_data_details['name'] = 'REAL'
# Print the updated all_data_sum
print(all_data_sum)


false_data = all_data_details[all_data_details['flag'] != True]
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

condition = "invoice_date >= date_trunc('month',(select max(invoice_date) from y4a_erp.y4a_erp_actual_broker where channel = 'NON-DI' and name ='REAL') - interval '1 month') and channel = 'NON-DI' and name ='REAL'"
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
data_details_by_line['name'] = 'REAL'

true_data_by_line = data_details_by_line[data_details_by_line['flag'] == True]

true_data_by_line['invoice_date'] = pd.to_datetime(true_data_by_line['invoice_date'], format='%b %d, %Y')
# Convert invoice_date to YYYY-MM-DD format
true_data_by_line['invoice_date'] = true_data_by_line['invoice_date'].dt.strftime('%Y-%m-%d')

true_data_by_line['run_time'] = ct


condition2 = "invoice_date >= date_trunc('month',(select max(invoice_date) from y4a_erp.y4a_erp_actual_broker_detail where channel = 'NON-DI' and name ='REAL') - interval '1 month') and channel = 'NON-DI' and name ='REAL'"
delete_data_from_table(table_name = 'y4a_erp.y4a_erp.y4a_erp_actual_broker_detail', condition=condition2)


actual_broker_dtl = sf.QueryPostgre(sql="select distinct pdf_file_path from y4a_erp.y4a_erp_actual_broker_detail", host=host, passwd=passwd, name=name, db=db)

schema_name = 'y4a_erp'  # Replace with your schema name
table_name = 'y4a_erp_actual_broker_detail'    # Replace with your table name

# Call the function to insert unique rows from true_data
insert_unique_rows(true_data_by_line, schema_name, table_name)

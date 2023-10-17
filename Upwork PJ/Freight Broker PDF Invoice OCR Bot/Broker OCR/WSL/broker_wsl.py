# =========================================================================
# Bat buoc phai copy doan code nay truoc khi xai
import datetime 
import os
import re
import sys
import datetime
import pandas as pd
import datetime
import warnings
from sqlalchemy import create_engine
import PyPDF2
import pandas as pd
from io import StringIO
import numpy as np
import psycopg2
warnings.filterwarnings("ignore")
from sqlalchemy import create_engine
abs_path = os.path.dirname(__file__)
main_cwd = re.sub('Y4A_BA_Team.*','Y4A_BA_Team',abs_path)
os.chdir(main_cwd)
sys.path.append(main_cwd)
from Shared_Lib import pcconfig
from Shared_Lib import shared_function as sf


# =========================================================================`    `
ct = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("current time: ", ct)

pcconfig = pcconfig.Init()


host = pcconfig['serverip_postgre']
name = pcconfig['postgre_name']
passwd = pcconfig['postgre_passwd']
db = pcconfig['postgre_db']
json_path = pcconfig['json_path']


win_sys = sf.QueryPostgre(sql="select concat('Y:/di_profile/',id,'/',access_key,'/','us_broker_invoice') as folder_path from rawdata.di_profile \
where 1=1 and us_broker_id = 6 and ship_date >= CURRENT_DATE - INTERVAL '18 months';", host=host, passwd=passwd, name=name, db=db)
if win_sys.empty:
    print("No data found in the cts DataFrame. Exiting the process.")
    sys.exit()  # Terminate the script
folder_paths = win_sys['folder_path'].tolist()
# for i in range(len(folder_paths)):
#     folder_paths[i] = folder_paths[i].replace('Y:/', '/airflow/finance/')



print(win_sys)

# Define the regular expression patterns for "INVOICE DATE" and "MASTER B/L NO."
invoice_date_pattern = r"INVOICE DATE\n(\d{2}-\d{2}-\d{4})"
master_bl_pattern = r"MASTER B/L NO\. :\n(.+)"
all_data_details = pd.DataFrame()
data_details_by_line = pd.DataFrame()

# Iterate through each folder path
for folder_path in folder_paths:
    # Loop through files in the folder
    for file_name in os.listdir(folder_path):
        pdf_file_path = os.path.join(folder_path, file_name)
        try:
            # Initialize an empty string to store the extracted text
            extracted_text = ""

            # Open the PDF file in read-binary mode
            with open(pdf_file_path, 'rb') as pdf_file:
                pdf_reader = PyPDF2.PdfReader(pdf_file)
                
                # Loop through all pages in the PDF and extract text
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    extracted_text += page.extract_text()

            # # Write the extracted text to a text file
            # output_text_path = "C:\\Users\\baotd\\Desktop\\output_text.txt"
            # with open(output_text_path, "w") as f:
            #     f.write(extracted_text)

            # print("Text extracted and saved to:", output_text_path)




            # Extract "INVOICE DATE" and "MASTER B/L NO." using regular expressions
            invoice_date_match = re.search(invoice_date_pattern, extracted_text)
            master_bl_match = re.search(master_bl_pattern, extracted_text)

            # Extract the values if matches are found
            invoice_date = invoice_date_match.group(1) if invoice_date_match else None
            master_bl_no = master_bl_match.group(1) if master_bl_match else None

            # Print the extracted values
            print("INVOICE DATE:", invoice_date)
            print("MASTER B/L NO.:", master_bl_no)


            # Find the starting point for processing
            start_index = extracted_text.find("AMOUNT")

            # Find the ending point for processing
            end_index = extracted_text.find("REMARK")

            # Get the relevant portion of the text
            relevant_text = extracted_text[start_index:end_index]

            # Remove the line containing "Amount" from relevant_text
            relevant_text_lines = relevant_text.split('\n')
            filtered_text_lines = [line for line in relevant_text_lines if "AMOUNT" not in line]
            modified_relevant_text = '\n'.join(filtered_text_lines)

            # Find the index of "TOTAL DUE" in the filtered lines
            total_due_index = filtered_text_lines.index("TOTAL DUE")

            # Remove the lines from "TOTAL DUE" to the end
            filtered_text_lines = filtered_text_lines[:total_due_index]

            # Join the modified lines back into text
            modified_relevant_text = '\n'.join(filtered_text_lines)


            # Create a StringIO object to simulate a file-like object
            text_io = StringIO(modified_relevant_text)

            # Initialize empty lists to store data
            data = []
            current_row = []

            # Iterate through lines in the text
            for line in text_io:
                line = line.strip()
                if line:
                    current_row.append(line)
                    if len(current_row) == 5:  # Each entry has 5 lines
                        data.append(current_row)
                        current_row = []

            # Create a DataFrame from the data
            columns = ["description", "type", "charge", "quantity", "total"]
            df = pd.DataFrame(data, columns=columns)
            # Add the extracted values to the DataFrame
            number_columns = ['charge','quantity','total']
            df[number_columns] = df[number_columns].apply(lambda a : a.str.replace('$','')
                                                                            .str.replace(',',''))
            #replace blank to NaN
            df[number_columns] = df[number_columns].apply(lambda x: x.str.strip()).replace('', np.nan)
            df[number_columns] = df[number_columns].apply(pd.to_numeric, errors='coerce')
            # Print the DataFrame
            print(df)
            def custom_grouping(item):
                if 'Freight' in item:
                    return 'FREIGHT'
                elif 'Duty' in item:
                    return 'DUTY'
                else:
                    return 'BROKER'
            filtered_df = df.groupby(df['description'].apply(custom_grouping))['total'].sum().reset_index()
            current_data = pd.DataFrame({
        'description': filtered_df['description'],
        'total_amount': filtered_df['total'],
        'invoice_date': [invoice_date] * len(filtered_df),
        'master_bl': [master_bl_no] * len(filtered_df),
        'pdf_file_path': [pdf_file_path] * len(filtered_df)
            })
            all_data_details = pd.concat([all_data_details, current_data], ignore_index=True)

            new_df = pd.DataFrame({
        'description': df['description'],
        'total_amount': df['total']
            })
            new_df['pdf_file_path'] = pdf_file_path
            new_df['invoice_date'] = invoice_date
            data_details_by_line = pd.concat([data_details_by_line, new_df], ignore_index=True)
        except Exception as e:
            print(f"Error processing file: {pdf_file_path}")
            print(f"Error details: {str(e)}")
        
all_data_sum = pd.DataFrame()
for folder_path in folder_paths:
    # Loop through files in the folder
    for file_name in os.listdir(folder_path):
        pdf_file_path = os.path.join(folder_path, file_name)
        try:
            # Initialize an empty string to store the extracted text
            extracted_text = ""

            # Open the PDF file in read-binary mode
            with open(pdf_file_path, 'rb') as pdf_file:
                pdf_reader = PyPDF2.PdfReader(pdf_file)
                
                # Loop through all pages in the PDF and extract text
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    extracted_text += page.extract_text()
                
                            # Find the starting point for processing
                start_index = extracted_text.find("TOTAL DUE")

                # Find the ending point for processing
                end_index = extracted_text.find("PAID AMOUNT")

                # Get the relevant portion of the text
                relevant_text = extracted_text[start_index:end_index]


                            # Split the relevant_text into lines
                relevant_lines = relevant_text.strip().split('\n')

                # Extract data from the lines
                description = relevant_lines[0]  # Assuming the first line is the description
                total_due = float(relevant_lines[2].replace(',', ''))  # Remove the comma before converting to float

                # Create a DataFrame for the current extracted data
                current_data = pd.DataFrame({
                    
                    'pdf_file_path': [pdf_file_path],
                    'total_usd_line': [total_due]
                })

                # Print the current data (optional)
                print(current_data)

                # Concatenate the current_data to all_data
                all_data_sum = pd.concat([all_data_sum, current_data], ignore_index=True)
        except Exception as e:
            print(f"Error processing file: {pdf_file_path}")
            print(f"Error details: {str(e)}")




number_columns = ['total_usd_line']

#replace blank to NaN
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
all_data_details['channel'] = 'DI'
all_data_details['name'] = 'WSL'



false_data = all_data_details[all_data_details['flag'] == False]
actual_broker = sf.QueryPostgre(sql= 'select distinct pdf_file_path from y4a_erp.y4a_erp_actual_broker_local'
, host=host, passwd=passwd, name=name, db=db)
false_data = false_data[~false_data['pdf_file_path'].isin(actual_broker['pdf_file_path'])]
false_data['run_time'] = ct

spreadsheet_key_1 = '1TGBp_S9jt68gu3ohrQh5CjKio1SYYguFOMpeCOjRMZs'

false_data_to_gg = sf.Gsheet_working(spreadsheet_key_1, 'Broker, Freight Error', json_file = json_path)
false_data_to_gg.Append_Current_Sheet(false_data, keyword = 'description')

true_data = all_data_details[all_data_details['flag'] == True]
true_data['invoice_date'] = pd.to_datetime(true_data['invoice_date'], format='%m-%d-%Y').dt.strftime('%Y-%m-%d')
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

condition = "invoice_date >= date_trunc('month',(select max(invoice_date) from y4a_erp.y4a_erp_actual_broker_local where channel = 'DI' and name ='WSL') - interval '1 month') and channel = 'DI' and name ='WSL'"
delete_data_from_table(table_name = 'y4a_erp.y4a_erp_actual_broker_local', condition=condition)

actual_broker = sf.QueryPostgre(sql="select distinct pdf_file_path from y4a_erp.y4a_erp_actual_broker_local", host=host, passwd=passwd, name=name, db=db)


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
table_name = 'y4a_erp_actual_broker_local'    # Replace with your table name

# Call the function to insert unique rows from true_data
insert_unique_rows(true_data, schema_name, table_name)


data_details_by_line = data_details_by_line.merge(all_data_sum[['pdf_file_path', 'flag']], on='pdf_file_path', how='left')
# all_data_details.drop(['access_key'], axis=1, inplace=True)
data_details_by_line['channel'] = 'DI'
data_details_by_line['name'] = 'WSL'

true_data_by_line = data_details_by_line[data_details_by_line['flag'] == True]

true_data_by_line['invoice_date'] = pd.to_datetime(true_data_by_line['invoice_date'], format='%m-%d-%Y').dt.strftime('%Y-%m-%d')

true_data_by_line['run_time'] = ct


condition2 = "invoice_date >= date_trunc('month',(select max(invoice_date) from y4a_erp.y4a_erp_actual_broker_detail_local where channel = 'DI' and name ='WSL') - interval '1 month') and channel = 'DI' and name ='WSL'"
delete_data_from_table(table_name = 'y4a_erp.y4a_erp.y4a_erp_actual_broker_detail_local', condition=condition2)


actual_broker_dtl = sf.QueryPostgre(sql="select distinct pdf_file_path from y4a_erp.y4a_erp_actual_broker_detail_local", host=host, passwd=passwd, name=name, db=db)

schema_name = 'y4a_erp'  # Replace with your schema name
table_name = 'y4a_erp_actual_broker_detail_local'    # Replace with your table name

# Call the function to insert unique rows from true_data
insert_unique_rows(true_data_by_line, schema_name, table_name)
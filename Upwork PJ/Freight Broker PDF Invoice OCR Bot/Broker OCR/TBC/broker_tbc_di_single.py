# =========================================================================
# Bat buoc phai copy doan code nay truoc khi xai
import datetime 
import os
import re
import sys
import datetime
import pandas as pd
import datetime
from sqlalchemy import create_engine
import PyPDF2
import pandas as pd
import numpy as np
from io import StringIO
abs_path = os.path.dirname(os.getcwd())
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


tbc_di = sf.QueryPostgre(sql="select concat('Y:\\di_profile\\',id,'\\',access_key,'\\','us_broker_invoice') as folder_path from rawdata.di_profile \
where 1=1 and us_broker_id = 231 and ship_date >= '2023-1-1';", host=host, passwd=passwd, name=name, db=db)

folder_paths = tbc_di['folder_path'].tolist()

all_data_details = pd.DataFrame()


extracted_text = ""
pdf_file_path = 'Y:/di_profile/di_profile/2619/472ba8eecdbd9a4e3da431b21efd1628/us_broker_invoice/230824045456-Y4A-AMZ-EXAM-OILAXAPVA8HLCUSHA2305APVA8ICAKINVODC.PDF'
# Open the PDF file in read-binary mode
with open(pdf_file_path, 'rb') as pdf_file:
    pdf_reader = PyPDF2.PdfReader(pdf_file)
    
    # Loop through all pages in the PDF and extract text
    for page_num in range(len(pdf_reader.pages)):
        page = pdf_reader.pages[page_num]
        extracted_text += page.extract_text()
# Write the extracted text to a text file
output_text_path = "C:\\Users\\baotd\\Desktop\\output_text.txt"
with open(output_text_path, "w", encoding="utf-8") as f:
    f.write(extracted_text)

date_pattern = r'Invoice Date\s+(\w+\s+\d{1,2},\s+\d{4})'

# Search for the date pattern in the extracted text
date_match = re.search(date_pattern, extracted_text)

# If a match is found, get the matched date value
if date_match:
    invoice_date = date_match.group(1)
else:
    print("Invoice Date not found")

# Find the starting point for processing
start_index = extracted_text.find("Container No")
if start_index == -1:
    # If 'Container No' is not found, try to find 'Final DestinationInfo'
    start_index = extracted_text.find("Final DestinationInfo")

# Find the ending point for processing
end_index = extracted_text.rfind("Remit To")

# Get the relevant portion of the text
relevant_text = extracted_text[start_index:end_index]

# If 'Final DestinationInfo' was used as start_index, remove lines containing it
if "Final DestinationInfo" in relevant_text:
    relevant_text = "\n".join(line for line in relevant_text.split('\n') if 'Final DestinationInfo' not in line)

# Remove the line containing 'Container No'
relevant_text = "\n".join(line for line in relevant_text.split('\n') if 'Container No' not in line)

# Write the extracted text to a text file
output_text_path = "C:\\Users\\baotd\\Desktop\\output_text.txt"
with open(output_text_path, "w", encoding="utf-8") as f:
    f.write(relevant_text)


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
keywords = ['IN CHARGE','EXAM FEE','WAITING TIME','DRAYAGE CHARGE','DROP CHARGE','CHASSIS SPLIT','STORAGE CHARGE','PIERPASS','HANDLING CHARGE','TRUCKING CHRAGE','ISF FEE','CUSTOMS CLEARANCE FEE', 'FUEL SURCHARGE',
            'MISCLLANEOUS','CLEAN TRUCK FEE','CHASSIS RENTAL','PORT CONGESTION FEE','PRE-PULL','TERMINAL TOLL FEE',
            'DEVANNING CHARGE','DOC FEE','WIRE TRANSFER FEE']

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
grouped_df.columns = ['Group', 'Total Amount']
grouped_df['ml_b_no'] = 'not found'
grouped_df['invoice_date'] = invoice_date
grouped_df['pdf_file_path'] = pdf_file_path
all_data_details = pd.concat([all_data_details, grouped_df], ignore_index=True)


all_data_sum = pd.DataFrame()
all_data = []

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
sum_amount_by_access_key = all_data_details.groupby('pdf_file_path')['Total Amount'].sum().reset_index()
sum_amount_by_access_key.rename(columns={'Total Amount': 'sum_amount'}, inplace=True)

# Merge the calculated sums back into the all_data_sum DataFrame based on 'access_key'
all_data_sum = all_data_sum.merge(sum_amount_by_access_key, on='pdf_file_path', how='left')

# Create a new column 'flag' to indicate if total_line_usd matches sum_amount
all_data_sum['flag'] = all_data_sum.apply(lambda row: row['total_usd_line'] == row['sum_amount'], axis=1)

# Display the updated all_data_sum DataFrame with the 'flag' column
print(all_data_sum)

all_data_details = all_data_details.merge(all_data_sum[['pdf_file_path', 'flag']], on='pdf_file_path', how='left')
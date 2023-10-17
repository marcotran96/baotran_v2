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
import pandas as pd
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

query = '''SELECT 
    access_key,
    attach_file_9,
    attach_file_11
FROM rawdata.po_notice_form_list pnfl 
WHERE 1=1 
    AND us_broker = 230 
    AND etd >= '2023-3-1'
    AND published = 1 ;'''
real = sf.QueryPostgre(sql= query
, host=host, passwd=passwd, name=name, db=db)

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
base_directory = 'Y:\\po_notice\\'

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
file_paths = [path.replace(' ', '') for path in file_paths]
all_data_details = pd.DataFrame()
mb_l_pattern = r'M-B/ L No\s*\.?[\s:]+([A-Za-z0-9\-]+)'
issue_date_pattern = r'Issue Date\s*:\s*([A-Za-z]{3}\s\d{2},\s\d{4})'
# Display the list of file paths
print(file_paths)

# for pdf_file_path in file_paths:
# Define the PDF file path
pdf_file_path = 'Y:\\po_notice\\78d035dbe465012e1923320307817da2\\_9\\214321-DOOR-DN-RLLGB2303520.pdf'

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

all_data_sum = pd.DataFrame()
all_data = []

# Loop through the PDF file paths
# for pdf_file_path in file_paths:
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
# all_data_sum['flag'] = all_data_sum.apply(lambda row: row['total_usd_line'] == row['sum_amount'], axis=1)

# Display the updated all_data_sum DataFrame with the 'flag' column
print(all_data_sum)

all_data_details = all_data_details.merge(all_data_sum[['pdf_file_path']], on='pdf_file_path', how='left')
all_data_details['channel'] = 'NON-DI'
all_data_details['name'] = 'REAL'

# false_data = all_data_details[all_data_details['flag'] == False]

# spreadsheet_key_1 = '1TGBp_S9jt68gu3ohrQh5CjKio1SYYguFOMpeCOjRMZs'

# false_data_to_gg = sf.Gsheet_working(spreadsheet_key_1, 'Broker, Freight Error', json_file = json_path)
# false_data_to_gg.Append_Current_Sheet(false_data, keyword = 'description')








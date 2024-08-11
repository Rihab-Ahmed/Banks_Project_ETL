# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%m-%d-%H:%M:%S'  
    now = datetime.now() 
    timestamp = now.strftime(timestamp_format) 
    with open("code_log.txt", "a") as f: 
        f.write(timestamp + ' : ' + message + '\n') 

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')
    
    data_list = []  # List to collect rows of data
    
    table = soup.find_all('table')[0]  
    
    for row in table.find('tbody').find_all('tr'):
        cols = row.find_all('td')
        if len(cols) > 1:
            name = cols[1].text.strip()
            mc_usd = float(cols[2].text.strip().replace('\n', '').replace(',', '').replace('$', ''))
            data_list.append({"Name": name, "MC_USD_Billion": mc_usd})
    
    df = pd.DataFrame(data_list, columns=table_attribs)
    
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    
    exchange_rate = pd.read_csv(csv_path).set_index('Currency')['Rate'].to_dict()

    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)
    log_progress('Data saved to CSV file')

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress('Data loaded to Database as a table, Executing queries')

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    rows = cursor.fetchall()
    print(f"Query: {query_statement}")
    for row in rows:
        print(row)
    print("\n")
    log_progress('Query executed successfully')

# Define known values
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
csv_path = './exchange_rate.csv'
output_csv_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'

# Log the completion of preliminaries
log_progress('Preliminaries complete. Initiating ETL process')

# Extract data
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

# Transform data
df = transform(df, csv_path)
log_progress('Data transformation complete. Initiating Loading process')

# Save to CSV
load_to_csv(df, output_csv_path)

# Initiate SQLite3 connection
conn = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

# Load data to database
load_to_db(df, conn, table_name)

# Execute queries and print outputs
run_query("SELECT * FROM Largest_banks", conn)
run_query("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", conn)
run_query("SELECT Name from Largest_banks LIMIT 5", conn)

# Close the connection
conn.close()
log_progress('Server Connection closed')

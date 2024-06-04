# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    df = pd.DataFrame(columns=table_attribs)
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    #print(data)

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    #print(rows[1])
    for row in rows:
        col = row.find_all('td')
        if len(col) == 3:
            # Process the data as expected
            data_dict = {"Name": str(col[1].contents[2].contents[0]), 
                         "MC_USD_Billion": float(col[2].contents[0][:-1])}
            
            #print(data_dict)
            
            df1 = pd.DataFrame([data_dict])
            df = pd.concat([df,df1], ignore_index=True)
        else:
            # Handle the case where the number of columns is different
            print(f"Unexpected number of columns: {len(col)}")

    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    rates_df = pd.read_csv(csv_path)
    exchange_rate = rates_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    #print(df['MC_EUR_Billion'][4])
    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_output = pd.read_sql(query_statement,sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
log_file = 'code_log.txt'
db_name = 'Banks.db'
table_name = 'Largest_banks'
attribute_list_e = ['Name', 'MC_USD_Billion']
attribute_list_f= ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
exchange_rates = 'exchange_rate.csv'
output_path = 'Largest_banks_data.csv'

log_progress('Preliminaries complete. Initiating ETL process')

extracted_data = extract(url, attribute_list_e)
log_progress('Data extraction complete. Initiating Transformation process')

transformed_data = transform(extracted_data, exchange_rates)
#print(transformed_data)
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(transformed_data, output_path)
log_progress('Data saved to CSV file')

conn = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

load_to_db(transformed_data, conn, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

run_query(f'SELECT * FROM Largest_banks', conn)
run_query(f'SELECT AVG(MC_GBP_Billion) FROM Largest_banks', conn)
run_query(f'SELECT Name from Largest_banks LIMIT 5', conn)
log_progress('Process Complete')

conn.close()
log_progress('Server Connection closed')

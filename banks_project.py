import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import numpy as np
from io import StringIO

import sqlite3


# -------------------------------
# Task 1: Logging Function
# -------------------------------
def log_progress(message):
    '''Logs progress with timestamp to code_log.txt'''
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("code_log.txt", "a") as f:
        f.write(f"{timestamp} : {message}\n")

log_progress("Preliminaries complete. Initiating ETL process")

# -------------------------------
# Task 2: Extract Function
# -------------------------------
def extract_marketcap(url):
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')

    # Find the correct table under "By market capitalization"
    header = soup.find(lambda tag: tag.name in ["h2", "h3"] and "By market capitalization" in tag.get_text())
    table = header.find_next('table')

    # Read table into DataFrame
    df = pd.read_html(StringIO(str(table)))[0]
    print("Extracted columns:", df.columns.tolist())  # Optional debug

    # Dynamically find relevant columns
    for col in df.columns:
        if 'Bank' in col:
            name_col = col
        if 'US$' in col or 'Market cap' in col:
            cap_col = col

    # Rename and clean
    df = df.rename(columns={name_col: 'Name', cap_col: 'MC_USD_Billion'})
    df = df[['Name', 'MC_USD_Billion']].copy()
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(str).str.replace(r'[^\d\.]', '', regex=True).astype(float)
    df = df.head(10)

    log_progress("Data extraction complete. Initiating Transformation process")
    return df

# Call extract function
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
df = extract_marketcap(url)

# -------------------------------
# Task 3: Transform Function
# -------------------------------
def transform(df, csv_path):
    # Load exchange rate CSV
    exchange_df = pd.read_csv(csv_path)
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']

    # Add converted columns
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    log_progress("Data transformation complete. Initiating Loading process")
    return df

# Call transform function
csv_path = 'exchange_rate.csv'
df = transform(df, csv_path)

# Print value for quiz (5th bank's EUR market cap)
print("5th bank MC_EUR_Billion value for quiz:", df['MC_EUR_Billion'][4])

# -------------------------------
# Task 4: Load to CSV Function
# -------------------------------
def load_to_csv(df):
    output_path = './Largest_banks_data.csv'
    df.to_csv(output_path, index=False)
    log_progress("Data saved to CSV file")

# Call load to CSV
load_to_csv(df)

def load_to_db(df, db_connection, table_name):
    '''Loads the DataFrame to the specified SQLite database table.'''
    df.to_sql(table_name, db_connection, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, Executing queries")

db_connection = sqlite3.connect('Banks.db')
log_progress("SQL Connection initiated")

# Step: Load to DB
load_to_db(df, db_connection, 'Largest_banks')

def run_queries(query_statement, db_connection):
    '''Runs a SQL query and prints both the statement and result.'''
    cursor = db_connection.cursor()
    cursor.execute(query_statement)
    result = cursor.fetchall()

    print(f"\nQuery: {query_statement}")
    print("Result:")
    for row in result:
        print(row)

    log_progress("Process Complete")

    # Task 6: Run queries
print("\n=== Task 6: SQL Query Results ===")

# 1. Print the entire table
run_queries("SELECT * FROM Largest_banks", db_connection)

# 2. Print the average market capitalization in GBP
run_queries("SELECT AVG(MC_GBP_Billion) FROM Largest_banks", db_connection)

# 3. Print only the names of the top 5 banks
run_queries("SELECT Name FROM Largest_banks LIMIT 5", db_connection)



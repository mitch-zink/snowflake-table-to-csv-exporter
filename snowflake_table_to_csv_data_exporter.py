# Description: This script exports data from a Snowflake table to CSV files
# Author: Mitch Zink
# Last Updated: 2023-01-20

import snowflake.connector
import csv
import os
import logging
from datetime import datetime, timedelta

# Setup basic logging for monitoring
logging.basicConfig(level=logging.INFO)

# Set your Snowflake account information from environment variables
ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT')
USER = os.environ.get('SNOWFLAKE_USER')
ROLE = os.environ.get('SNOWFLAKE_ROLE')  
PASSWORD = os.environ.get('SNOWFLAKE_PASSWORD') 
WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE')

# Define the date range for data extraction
START_DATE = datetime(1995, 1, 1)  # On or after | Format: YYYY, M, D
END_DATE = datetime(1995, 1, 2)    # On or before | Format: YYYY, M, D
TABLE_NAME = "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS"  # Source table | Format: DATABASE.SCHEMA.TABLE
DATE_COLUMN_NAME = "O_ORDERDATE"  # Column with date information
FILENAME_PREFIX = "orders"  # Prefix for the CSV files

# Specify the directory for storing CSV files
CSV_DIR = "csv" 

# Function to fetch and write data for a given day
def fetch_and_write_data(con, start_date, end_date, table_name, date_column_name):
    # Convert datetime objects to strings for the query
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # SQL query to fetch data for the specified date range
    query = f"""
    SELECT * 
    FROM {table_name}
    WHERE {date_column_name} >= '{start_date_str}' 
    AND {date_column_name} < '{end_date_str}' 
    """

    # Log the full query for visibility
    logging.info(f"Executing query: {query.strip()}")

    # Execute the query and write results to a CSV file
    try:
        # Execute the query and fetch all rows
        with con.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            logging.info(f"Query for {start_date_str} executed successfully")

            # Formatting the filename with date and writing data to CSV
            formatted_filename_date = start_date.strftime("%m_%d_%Y")
            csv_file = os.path.join(CSV_DIR, f"{FILENAME_PREFIX}_{formatted_filename_date}.csv")

            # Writing the data to a CSV file
            with open(csv_file, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([col[0] for col in cur.description])
                for row in rows:
                    writer.writerow(row)

            logging.info(f"Data written to {csv_file} - {len(rows)} rows")
    
    except Exception as e:
        logging.error(f"Error in fetch_and_write_data: {e}")

# Function to establish a connection to Snowflake
def create_snowflake_connection():
    # Attempt to connect to Snowflake and handle any connection errors
    try:
        con = snowflake.connector.connect(
            user=USER,
            password=PASSWORD,
            account=ACCOUNT,
            role=ROLE
        )
        logging.info("Connected to Snowflake")
        return con
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {e}")
        raise

# Main execution block
try:
    # Establishing Snowflake connection
    con = create_snowflake_connection()

    # Setting the specified warehouse as the current warehouse
    logging.info(f"Setting warehouse to {WAREHOUSE}")
    con.cursor().execute(f"USE WAREHOUSE {WAREHOUSE}")

    # Check if the CSV directory exists and clear it if it does
    if os.path.exists(CSV_DIR):
        logging.info(f"Deleting existing directory {CSV_DIR}")
        for file in os.listdir(CSV_DIR):
            os.remove(os.path.join(CSV_DIR, file))
        os.rmdir(CSV_DIR)

    # Create the CSV directory
    os.makedirs(CSV_DIR, exist_ok=True)

    # Loop through each day in the date range and fetch data
    start_date = START_DATE
    while start_date <= END_DATE:
        next_day = start_date + timedelta(days=1)
        fetch_and_write_data(con, start_date, next_day, TABLE_NAME, DATE_COLUMN_NAME)
        start_date = next_day

finally:
    # Ensuring the Snowflake connection is closed after processing
    if con:
        con.close()
        logging.info("Connection closed")
# Description: This script exports data from a Snowflake table to CSV files
# Author: Mitch Zink
# Last Updated: 2024-01-20

import csv
import logging
import os
from datetime import datetime, timedelta

import snowflake.connector

# Setup basic logging for monitoring
logging.basicConfig(level=logging.INFO)

# Set your Snowflake account information from environment variables
ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
USER = os.environ.get("SNOWFLAKE_USER")
ROLE = os.environ.get("SNOWFLAKE_ROLE")
PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")

# Define the date range for data extraction
START_DATE = datetime(1995, 1, 1)  # On or after | Format: YYYY, M, D
END_DATE = datetime(1995, 1, 2)  # On or before | Format: YYYY, M, D
TABLE_NAME = "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS"  # Source table | Format: DATABASE.SCHEMA.TABLE
DATE_COLUMN_NAME = "O_ORDERDATE"  # Column with date information
FILENAME_PREFIX = "orders"  # Prefix for the CSV files

# Specify the directory for storing CSV files
CSV_DIR = "csv"

# Function to fetch and write data for a given day
def fetch_and_write_data(connection, day_start, day_end, table_name, date_column_name):
    # Convert datetime objects to strings for the query
    start_date_str = day_start.strftime("%Y-%m-%d")
    end_date_str = day_end.strftime("%Y-%m-%d")

    # SQL query to fetch data for the specified date range
    query = f"""
    SELECT * 
    FROM {table_name}
    WHERE {date_column_name} >= '{start_date_str}' 
    AND {date_column_name} < '{end_date_str}' 
    """

    # Log the full query for visibility
    logging.info("Executing query: %s", query.strip())

    # Execute the query and write results to a CSV file
    try:
        # Execute the query and fetch all rows
        with connection.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            logging.info("Query for %s executed successfully", start_date_str)

            # Formatting the filename with date and writing data to CSV
            formatted_filename_date = day_start.strftime("%m_%d_%Y")
            csv_file_path = os.path.join(
                CSV_DIR, f"{FILENAME_PREFIX}_{formatted_filename_date}.csv"
            )

            # Writing the data to a CSV file
            with open(csv_file_path, "w", newline="") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow([col[0] for col in cur.description])
                for row in rows:
                    writer.writerow(row)

            logging.info("Data written to %s - %d rows", csv_file_path, len(rows))

    except Exception as e:
        logging.error("Error in fetch_and_write_data: %s", e)

# Function to establish a connection to Snowflake
def create_snowflake_connection():
    # Attempt to connect to Snowflake and handle any connection errors
    try:
        snowflake_conn = snowflake.connector.connect(
            user=USER, password=PASSWORD, account=ACCOUNT, role=ROLE
        )
        logging.info("Connected to Snowflake")
        return snowflake_conn
    except Exception as e:
        logging.error("Error connecting to Snowflake: %s", e)
        raise

# Main execution block
try:
    # Establishing Snowflake connection
    snowflake_connection = create_snowflake_connection()

    # Setting the specified warehouse as the current warehouse
    logging.info("Setting warehouse to %s", WAREHOUSE)
    snowflake_connection.cursor().execute(f"USE WAREHOUSE {WAREHOUSE}")

    # Check if the CSV directory exists and clear it if it does
    if os.path.exists(CSV_DIR):
        logging.info("Deleting existing directory %s", CSV_DIR)
        for filename in os.listdir(CSV_DIR):
            os.remove(os.path.join(CSV_DIR, filename))
        os.rmdir(CSV_DIR)

    # Create the CSV directory
    os.makedirs(CSV_DIR, exist_ok=True)

    # Loop through each day in the date range and fetch data
    current_date = START_DATE
    while current_date <= END_DATE:
        next_day = current_date + timedelta(days=1)
        fetch_and_write_data(snowflake_connection, current_date, next_day, TABLE_NAME, DATE_COLUMN_NAME)
        current_date = next_day

finally:
    # Ensuring the Snowflake connection is closed after processing
    if snowflake_connection:
        snowflake_connection.close()
        logging.info("Connection closed")

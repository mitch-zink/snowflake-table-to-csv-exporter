"""
Description: This script exports data from a Snowflake table to CSV files
Author: Mitch Zink
Last Updated: 2024-01-20
"""

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
TABLE_NAME = "SNOWFLAKE_SAMPLE_DATA.TPCH_SF1000.ORDERS"  # Format: DATABASE.SCHEMA.TABLE
DATE_COLUMN_NAME = "O_ORDERDATE"  # Column with date information
FILENAME_PREFIX = "orders"  # Prefix for the CSV files

# Specify the directory for storing CSV files
CSV_DIR = "csv"


# Fetch and write data to CSV files for a given date range
def fetch_and_write_data(connection, day_start, day_end, table_name, date_column_name):
    """
    Fetches data from a Snowflake table for a given date range and writes it to a CSV file.

    Args:
        connection: Snowflake connection object.
        day_start (datetime): Start date for data extraction.
        day_end (datetime): End date for data extraction.
        table_name (str): Name of the Snowflake table.
        date_column_name (str): Name of the column with date information in the table.
    """
    # Date dictionary - Convert datetime objects to strings for the query
    date_info = {
        "start_date_str": day_start.strftime("%Y-%m-%d"),
        "end_date_str": day_end.strftime("%Y-%m-%d"),
    }

    # SQL query to fetch data for the specified date range
    query = (
        f"SELECT * FROM {table_name} "
        f"WHERE {date_column_name} >= '{date_info['start_date_str']}' "
        f"AND {date_column_name} < '{date_info['end_date_str']}'"
    )

    # Log the full query for visibility
    logging.info("Executing query: %s", query.strip())

    # Execute the query and write results to a CSV file
    try:
        # Execute the query and fetch all rows
        with connection.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            logging.info(
                "Query for %s executed successfully", date_info["start_date_str"]
            )

            # Formatting the filename with date and writing data to CSV
            formatted_filename_date = day_start.strftime("%m_%d_%Y")
            csv_file_path = os.path.join(
                CSV_DIR, f"{FILENAME_PREFIX}_{formatted_filename_date}.csv"
            )

            # Write data to the CSV file
            with open(csv_file_path, "w", newline="", encoding="utf-8") as csv_file:  # Open CSV file for writing
                # Create a CSV writer object with double pipe delimiter and ensure all fields are quoted
                writer = csv.writer(csv_file, delimiter='|', quotechar='"', quoting=csv.QUOTE_ALL)
                # Write column headers
                writer.writerow([col[0] for col in cur.description])
                # Write each row of data to the CSV file
                for row in rows:
                    writer.writerow(row)

            logging.info("Data written to %s - %d rows", csv_file_path, len(rows))

    except snowflake.connector.Error as e:
        logging.error("Error in fetch_and_write_data: %s", e)


def create_snowflake_connection():
    """
    Establishes a connection to a Snowflake database.

    Returns:
        A Snowflake connection object if successful, raises an Exception otherwise.
    """
    # Attempt to connect to Snowflake and handle any connection errors
    try:
        snowflake_conn = snowflake.connector.connect(
            user=USER, password=PASSWORD, account=ACCOUNT, role=ROLE
        )
        logging.info("Connected to Snowflake")
        return snowflake_conn
    except Exception as e:
        logging.error("Error connecting to Snowflake: %s", e)
        raise RuntimeError("An error occurred while connecting to Snowflake.") from e

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
        fetch_and_write_data(
            snowflake_connection, current_date, next_day, TABLE_NAME, DATE_COLUMN_NAME
        )
        current_date = next_day

finally:
    # Ensuring the Snowflake connection is closed after processing
    if snowflake_connection:
        snowflake_connection.close()
        logging.info("Connection closed")

"""
Description: This script exports data from a Snowflake table to CSV files using Streamlit for the interface.
Author: Mitch Zink
Last Updated: 3/30/2024
"""

# Import necessary libraries
import streamlit as st  # For creating the web app interface
import snowflake.connector  # For Snowflake database connection
import csv  # For writing the data to CSV files
import os  # For handling file paths and directories
from datetime import datetime, timedelta  # For handling dates
import io  # Import io to create in-memory file objects

# Setup Streamlit page layout and title
st.title('Snowflake Data Exporter')
st.sidebar.header('Configuration')

# Streamlit widgets to input Snowflake connection details
ACCOUNT = st.sidebar.text_input("Snowflake Account")
USER = st.sidebar.text_input("User")
ROLE = st.sidebar.text_input("Role")
WAREHOUSE = st.sidebar.text_input("Warehouse")

# Widget to choose the authentication method: password or external browser
use_external_auth = st.sidebar.checkbox("Use External Browser Authentication")

# Conditional widget for entering password if not using external authentication
PASSWORD = ""
if not use_external_auth:
    PASSWORD = st.sidebar.text_input("Password", type="password")
    authenticator = 'snowflake'  # Default authentication method for password authentication
else:
    authenticator = 'externalbrowser'  # Use external browser for SSO authentication

# Widgets to input the data export configuration: date range, table details
START_DATE = st.sidebar.date_input("Start Date", datetime.now().date() - timedelta(days=1))
END_DATE = st.sidebar.date_input("End Date", datetime.now().date())
TABLE_NAME = st.sidebar.text_input("Table Name", "DATABASE.SCHEMA.TABLE")
DATE_COLUMN_NAME = st.sidebar.text_input("Date Column Name", "COLUMN_NAME")
FILENAME_PREFIX = st.sidebar.text_input("Filename Prefix", "exported_data")
CSV_DIR = "csv"  # Directory where CSV files will be saved

# Fetches data from Snowflake for a given date range and table, then returns CSV content
def fetch_and_write_data(connection, day_start, day_end, table_name, date_column_name):
    """
    Fetches data from Snowflake for a given date range and table, then returns CSV content.
    """
    st.write(f"Fetching data for {day_start.strftime('%Y-%m-%d')}...")
    progress_bar = st.progress(0)  # Initialize progress bar

    # Format dates for SQL query
    date_info = {
        "start_date_str": day_start.strftime("%Y-%m-%d"),
        "end_date_str": day_end.strftime("%Y-%m-%d"),
    }

    # SQL query to fetch data
    query = (
        f"SELECT * FROM {table_name} "
        f"WHERE {date_column_name} >= '{date_info['start_date_str']}' "
        f"AND {date_column_name} < '{date_info['end_date_str']}'"
    )

    try:
        with connection.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()  # Fetch all rows for the date range
            progress_bar.progress(50)  # Update progress

            # Create an in-memory CSV file
            csv_file = io.StringIO()
            writer = csv.writer(csv_file, delimiter='|', quotechar='"', quoting=csv.QUOTE_ALL)
            writer.writerow([col[0] for col in cur.description])  # Write column headers
            writer.writerows(rows)  # Write data rows

            progress_bar.progress(100)  # Complete progress
            st.success(f"Data ready for download - {len(rows)} rows")

            # Reset the pointer of the in-memory file to the beginning
            csv_file.seek(0)

            return csv_file

    except Exception as e:
        st.error(f"Error in fetch_and_write_data: {e}")
        return None


# Function to establish a connection to Snowflake
def create_snowflake_connection(user, account, role, warehouse, password=None, authenticator='externalbrowser'):
    """
    Establishes a connection to Snowflake using the provided credentials and authentication method.
    """
    try:
        if authenticator == 'externalbrowser':
            snowflake_conn = snowflake.connector.connect(
                user=user, account=account, role=role, warehouse=warehouse,
                authenticator=authenticator
            )
        else:
            snowflake_conn = snowflake.connector.connect(
                user=user, account=account, password=password, role=role, warehouse=warehouse
            )
        st.success("Connected to Snowflake using selected authentication method.")
        return snowflake_conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None

# Export data button to trigger the data export process
if st.sidebar.button('Export Data'):
    # Validate configuration inputs
    if not all([ACCOUNT, USER, ROLE, WAREHOUSE]) or (not use_external_auth and not PASSWORD):
        st.error("Please fill in all the configuration fields.")
    else:
        with st.spinner('Connecting to Snowflake...'):
            # Establish connection to Snowflake
            snowflake_connection = create_snowflake_connection(USER, ACCOUNT, ROLE, WAREHOUSE, PASSWORD, authenticator)
        if snowflake_connection:
            try:
                st.write("Starting data export...")
                # Calculate total number of days to export
                total_days = (END_DATE - START_DATE).days + 1
                for i, single_date in enumerate((START_DATE + timedelta(n) for n in range(total_days)), start=1):
                    current_date = datetime.combine(single_date, datetime.min.time())
                    next_day = current_date + timedelta(days=1)
                    csv_content = fetch_and_write_data(
                        snowflake_connection, current_date, next_day, TABLE_NAME, DATE_COLUMN_NAME
                    )
                    if csv_content:
                        formatted_date = current_date.strftime("%Y_%m_%d")
                        file_name = f"{FILENAME_PREFIX}_{formatted_date}.csv"
                        # Convert StringIO content to string, then encode to bytes
                        csv_string = csv_content.getvalue()
                        csv_bytes = csv_string.encode()
                        # Create a download button for the CSV file
                        st.download_button(
                            label="Download data as CSV",
                            data=csv_bytes,  # Pass the encoded bytes
                            file_name=file_name,
                            mime='text/csv',
                        )

            finally:
                if snowflake_connection:
                    snowflake_connection.close()  # Close the Snowflake connection
                    st.info("Connection closed")
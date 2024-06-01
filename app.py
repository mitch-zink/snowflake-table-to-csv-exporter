"""
Description: This script exports data from a Snowflake table to CSV files using Streamlit for the interface.
Author: Mitch Zink
Last Updated: 6/1/2024
"""

# Import necessary libraries
import streamlit as st  # For creating the web app interface
import snowflake.connector  # For Snowflake database connection
import csv  # For writing the data to CSV files
import os  # For handling file paths and directories
from datetime import datetime, timedelta  # For handling dates
import io  # For in-memory file handling
from zipfile import ZipFile  # For creating ZIP archives
import sqlparse
from concurrent.futures import ThreadPoolExecutor, as_completed  # For parallel execution

# Setup Streamlit page layout and title
st.title("Snowflake Data Exporter")
st.sidebar.header("Configuration")

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
    authenticator = (
        "snowflake"  # Default authentication method for password authentication
    )
else:
    authenticator = "externalbrowser"  # Use external browser for SSO authentication

# Widgets to input the data export configuration: date range, table details
START_DATE = st.sidebar.date_input(
    "Start Date", datetime.now().date() - timedelta(days=1)
)
END_DATE = st.sidebar.date_input("End Date", datetime.now().date())
TABLE_NAME = st.sidebar.text_input("Table Name", "DATABASE.SCHEMA.TABLE")
DATE_COLUMN_NAME = st.sidebar.text_input("Date Column Name", "COLUMN_NAME")
FILENAME_PREFIX = st.sidebar.text_input("Filename Prefix", "exported_data")
GROUP_BY = st.sidebar.selectbox("Group By", ["None", "Day", "Month", "Year"])
CSV_DIR = "csv"  # Directory where CSV files will be saved

def create_snowflake_connection(
    user, account, role, warehouse, password=None, authenticator="externalbrowser"
):
    """
    Establishes a connection to Snowflake using the provided credentials and authentication method.
    """
    try:
        if authenticator == "externalbrowser":
            snowflake_conn = snowflake.connector.connect(
                user=user,
                account=account,
                role=role,
                warehouse=warehouse,
                authenticator=authenticator,
            )
        else:
            snowflake_conn = snowflake.connector.connect(
                user=user,
                account=account,
                password=password,
                role=role,
                warehouse=warehouse,
            )
        st.success("Connected to Snowflake using selected authentication method.")
        return snowflake_conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {e}")
        return None


def get_next_time_interval(current, group_by):
    if group_by == "Day":
        return current + timedelta(days=1)
    elif group_by == "Month":
        next_month = current.replace(day=28) + timedelta(days=4)  # This will never fail
        return next_month - timedelta(days=next_month.day - 1)
    elif group_by == "Year":
        return current.replace(year=current.year + 1, month=1, day=1)

def fetch_and_write_data(connection, start, end, table_name, date_column_name):
    """
    Fetches data from Snowflake for a given date range and table, then returns CSV content and the formatted query.
    """
    # Format dates for SQL query
    date_info = {
        "start_date_str": start.strftime("%Y-%m-%d"),
        "end_date_str": end.strftime("%Y-%m-%d"),
    }

    # SQL query to fetch data
    query = (
        f"SELECT * FROM {table_name} "
        f"WHERE {date_column_name}::DATE >= '{date_info['start_date_str']}'::DATE "
        f"AND {date_column_name}::DATE < '{date_info['end_date_str']}'::DATE"
    )

    # Format the query to be lowercase and pretty
    formatted_query = sqlparse.format(query, reindent=True, keyword_case='lower')

    try:
        with connection.cursor() as cur:
            with st.spinner(f"Running query for {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}..."):
                cur.execute(query)
                rows = cur.fetchall()  # Fetch all rows for the date range

                # Create an in-memory CSV file
                csv_file = io.StringIO()
                writer = csv.writer(
                    csv_file, delimiter="|", quotechar='"', quoting=csv.QUOTE_ALL
                )
                writer.writerow([col[0] for col in cur.description])  # Write column headers
                writer.writerows(rows)  # Write data rows

                # Get the CSV content as a string from the StringIO object
                csv_content = csv_file.getvalue()
                return csv_content, formatted_query

    except Exception as e:
        st.error(f"Error in fetch_and_write_data: {e}")
        return None, None

def parallel_fetch(connection, date_ranges, table_name, date_column_name):
    """
    Fetches data for multiple date ranges in parallel.
    """
    memory_files = []
    queries = []
    total = len(date_ranges)
    progress_bar = st.progress(0)
    progress_text = st.empty()

    def fetch_wrapper(start_end):
        start, end = start_end
        return (start, end, fetch_and_write_data(connection, start, end, table_name, date_column_name))

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_wrapper, date_range): date_range for date_range in date_ranges}
        for i, future in enumerate(as_completed(futures)):
            start, end, (csv_content, formatted_query) = future.result()
            if csv_content:
                formatted_date = (
                    start.strftime("%Y_%m_%d") if GROUP_BY == "Day"
                    else (start.strftime("%Y_%m") if GROUP_BY == "Month"
                          else start.strftime("%Y"))
                )
                file_name = f"{FILENAME_PREFIX}_{formatted_date}.csv"
                memory_files.append((file_name, csv_content))
                queries.append(formatted_query)
                progress_text.text(f"Completed query for {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")
                st.code(formatted_query, language="sql")
            progress_bar.progress((i + 1) / total)

    return memory_files

if st.sidebar.button("Export Data"):
    required_fields = {
        "Snowflake Account": ACCOUNT,
        "User": USER,
        "Role": ROLE,
        "Warehouse": WAREHOUSE,
        "Start Date": START_DATE,
        "End Date": END_DATE,
        "Table Name": TABLE_NAME,
        "Date Column Name": DATE_COLUMN_NAME,
        "Filename Prefix": FILENAME_PREFIX,
    }

    missing_fields = [field for field, value in required_fields.items() if not value]
    if missing_fields or (not use_external_auth and not PASSWORD):
        st.error(
            f"Please fill in all the configuration fields: {', '.join(missing_fields)}"
        )
    else:
        memory_files = []  # List to store in-memory CSV contents
        with st.spinner("Connecting to Snowflake..."):
            snowflake_connection = create_snowflake_connection(
                USER, ACCOUNT, ROLE, WAREHOUSE, PASSWORD, authenticator
            )
        if snowflake_connection:
            try:
                if GROUP_BY == "None":
                    csv_content, formatted_query = fetch_and_write_data(
                        snowflake_connection,
                        START_DATE,
                        END_DATE + timedelta(days=1),
                        TABLE_NAME,
                        DATE_COLUMN_NAME,
                    )
                    if csv_content:
                        file_name = f"{FILENAME_PREFIX}_full.csv"
                        memory_files.append((file_name, csv_content))
                        st.code(formatted_query, language="sql")
                        st.success("Query completed successfully.")
                else:
                    date_ranges = []
                    current_date = datetime.combine(START_DATE, datetime.min.time())
                    end_date = datetime.combine(
                        END_DATE + timedelta(days=1), datetime.min.time()
                    )
                    while current_date < end_date:
                        next_date = get_next_time_interval(current_date, GROUP_BY)
                        if next_date > end_date:
                            next_date = end_date
                        date_ranges.append((current_date, next_date))
                        current_date = next_date

                    memory_files = parallel_fetch(snowflake_connection, date_ranges, TABLE_NAME, DATE_COLUMN_NAME)

                # Bundle all CSV contents into a single ZIP file
                if memory_files:
                    zip_buffer = io.BytesIO()
                    with ZipFile(zip_buffer, "w") as zip_file:
                        for file_name, data in memory_files:
                            zip_file.writestr(file_name, data)
                    zip_buffer.seek(0)
                    st.download_button(
                        label="Download All CSVs as ZIP",
                        data=zip_buffer.getvalue(),
                        file_name="all_csv_exports.zip",
                        mime="application/zip",
                    )
            finally:
                if snowflake_connection:
                    snowflake_connection.close()
                    st.info("Connection closed")

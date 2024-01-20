# Snowflake Table to CSV Data Exporter 📈

[![Python](https://img.shields.io/badge/-Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org/)
[![Snowflake](https://img.shields.io/badge/-Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://snowflake.com/)

This tool is designed to efficiently export data from a specified Snowflake table into CSV files 🗄️. Each CSV file contains data corresponding to a specific date, allowing for organized and incremental data extraction.

## Installation on MacOS 🍏

To set up the Snowflake Table to CSV tool on your MacOS, follow these steps:

1. **Set up a Python Virtual Environment and Install Required Packages** 🛠️:

    Open the terminal 🖥️ and run the following command to create a virtual environment, activate it, and install necessary packages:

    ```bash
    python3 -m venv venv && source venv/bin/activate && pip3 install --upgrade pip && pip3 install -r requirements.txt
    ```

## Configuration 🔧

Before running the tool, you need to set up your Snowflake credentials and other configuration variables. You can do this via environment variables in your shell.

Run these commands in the shell, replacing the placeholders with your actual Snowflake credentials:

```bash
export SNOWFLAKE_ACCOUNT='your_account_id' # Example: hf9283
export SNOWFLAKE_USER='your_username'
export SNOWFLAKE_ROLE='your_role'
export SNOWFLAKE_PASSWORD='your_password'
export SNOWFLAKE_WAREHOUSE='your_warehouse'
```

## Usage 🚀

Once the environment is set up and configured, you can run the tool using:

```bash
python3 snowflake_table_to_csv_data_exporter.py
```

The script will connect to your Snowflake database using the provided credentials, extract data from the specified table, and save it into CSV files in a designated directory 📁. The data extraction is based on the date range defined within the script.

---

**Note**: 📝 Remember to adjust the `START_DATE`, `END_DATE`, `TABLE_NAME`, and `DATE_COLUMN_NAME` variables in the `snowflake_table_to_csv_data_exporter.py` script according to your specific requirements.

---
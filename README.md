# Snowflake Table to CSV Data Exporter 📈

[![Open with Streamlit](https://img.shields.io/badge/-Open%20with%20Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://snow-table-to-csv-exporter.streamlit.app/)

[![Python](https://img.shields.io/badge/-Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org/)
[![Snowflake](https://img.shields.io/badge/-Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)](https://snowflake.com/)
[![Streamlit](https://img.shields.io/badge/-Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://streamlit.io/)

This tool is designed to efficiently export data from a specified Snowflake table into CSV files 🗄️. Each CSV file contains data corresponding to a specific date, allowing for organized and incremental data extraction.

### Creating a Virtual Environment

```bash
python3 -m venv venv && source venv/bin/activate && pip3 install --upgrade pip && pip3 install -r requirements.txt 
```

### Running the Streamlit App

```bash
streamlit run app.py
```
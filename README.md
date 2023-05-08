# Datalake_Batch_Upload

This script is a Python-based data lake CSV uploader that allows users to upload CSV files to a Google Drive data lake, and then insert the data into a specified database schema and table. It also provides options for managing batches and data sources.

Script is mandated for all manual, batch uploads to database and forces logging, datalake upload and formatting, simultaneously pushing data to raw schema with all necessary metadata. 

## Features
- Select a CSV file to upload
- Upload the CSV file to the data lake on Google Drive
- Choose a target schema and table for the data
- Clean the column names in the data
- Manage data sources (select an existing data source or create a new one)
- Manage batch IDs (create a new batch ID or use an existing one)
- Add metadata columns to the uploaded data

## Setup
To use this script, you'll need to install the following Python packages:

pandas
sqlalchemy
inquirer
tkinter
google-api-python-client

## Usage
To run the script, navigate to the directory containing the script and execute:
```
python batch_upload.py
```
The script will prompt you to:

- Select a CSV file to upload
- Choose a target schema and table for the data
- Manage data sources (select an existing data source or create a new one)
- Manage batch IDs (create a new batch ID or use an existing one)
- After completing the prompts, the script will upload the CSV file to the data lake on Google Drive, insert the data into the specified database schema and table, and manage batches and data sources as specified.

import sys
import os
sys.path.append(os.path.dirname('..'))
import datetime
import pandas as pd
from shared.db import DbConn
from google_api.auth import GoogleDriveAPI
import tkinter as tk
import inquirer
from tkinter import filedialog
from sqlalchemy import create_engine

# Serverless Version
# import boto3
# db = DbService(ddoyle_login_param_name, get_creds_from_ssm=True)

DATALAKE_URL = 'https://drive.google.com/drive/folders/1qgYMJZ2IXgmUtWyiu4HF4_ycxWb0wWRL'


def select_csv_file() -> str:
    """
    Opens a file dialog for selecting a CSV file.

    Returns:
        str: The path of the selected CSV file or None if no file was selected.
    """
    root = tk.Tk()
    root.withdraw()  # Hide the main window

    file_path = filedialog.askopenfilename(
        title="Select a CSV file",
        filetypes=(("CSV Files", "*.csv"), ("All Files", "*.*")),
    )

    if file_path:
        print(f"Selected CSV file: {file_path}")
        return file_path
    else:
        print("No file selected.")
        return None


def upload_to_drive(csv_file) -> str:
    """
    Uploads a CSV file to the data lake on Google Drive.

    Returns:
        str: The path of the uploaded file on Google Drive.
    """
    gdrive = GoogleDriveAPI(DATALAKE_URL)
    uploaded_file_path = gdrive.select_folder_and_upload(csv_file, os.path.basename(csv_file))
    return uploaded_file_path


def select_schema():
    questions = [
        inquirer.List(
            'schema_name',
            message='Select target schema:',
            choices=['raw', 'raw_third_party'],
        )
    ]
    selected_schema = inquirer.prompt(questions)['schema_name']

    return selected_schema

def batch_creation_flag():
    questions = [
        inquirer.List(
            'batch_creations',
            message='Select a batch option:',
            choices=['Create New Batch ID', 'Use Existing Batch ID'],
        )
    ]
    batch_option = inquirer.prompt(questions)['batch_creations']

    return batch_option


def select_or_create_datasource(db: DbConn) -> int:
    """
    Prompts the user to select a data source or create a new one.

    Args:
        db (DbService): The database service instance.

    Returns:
        int: The ID of the selected or created data source.
    """
    # Fetch data source names and IDs from the database
    query = '''SELECT id, name, full_name, descr FROM dwh.dim_datasource'''
    datasource_df = db.pandas_read(query)
    datasource_dict = datasource_df.set_index('name').to_dict(orient='index')

    # Add an option to create a new row
    datasource_names = list(datasource_dict.keys())
    datasource_names.insert(0, "Create a new datasource")

    # Prompt the user to select a data source name
    questions = [
        inquirer.List(
            'datasource_name',
            message='Select a data source or create a new one:',
            choices=datasource_names,
        )
    ]
    selected_datasource_name = inquirer.prompt(questions)['datasource_name']

    # If the user chooses to create a new data source, prompt for the required information
    if selected_datasource_name == "Create a new datasource":
        # Generate the next incremental ID
        new_id = datasource_df['id'].max() + 1

        name = input("Enter the name: ")
        full_name = input("Enter the full name: ")
        description = input("Enter the description: ")

        # Create a dataframe with the new data source information
        new_datasource_df = pd.DataFrame({"id": [new_id], "name": [name], "full_name": [full_name], "descr": [description]})

        # Insert the new data source into the database
        db.execute_values(new_datasource_df, "dwh.dim_datasource")
        return new_id
    else:
        selected_datasource = datasource_dict[selected_datasource_name]
        return selected_datasource['id']



def get_max_batch_id(db: DbConn) -> int:
    """
    Gets the maximum batch ID from the database.
    Args:
        db (DbService): The database service instance.

    Returns:
        int: The maximum batch ID.
    """
    query = "SELECT MAX(id) as max_id FROM config.batch"
    max_id_df = db.pandas_read(query)
    max_id = max_id_df.loc[0, 'max_id']
    return max_id


def create_batch_id(db: DbConn, source_location: str) -> None:
    """
    Creates a new batch ID in the database.
    Args:
        db (DbService): The database service instance.
        source_location (str): The location of the source of the batch.
    """
    print('CREATING BATCH...')
    max_id = get_max_batch_id(db)
    new_id = max_id + 1
    data_source_id = select_or_create_datasource(db)
    notes = input('\nPlease insert note to include in batch table entry:\n')
    data = {
        'id': [new_id],  # Set to None, so that the database can auto-generate the ID during insertion
        'datasource_id': [data_source_id],
        'notes': [notes],
        'source_location': [source_location],
        'execution_env': ['Python Automation']
    }

    batch_df = pd.DataFrame(data)
    db.execute_values(batch_df, 'config.batch')
    return new_id



def clean_columns(df):
    df.columns = df.columns.str.replace(' ', '_')
    df.columns = df.columns.str.replace('[()]', '')
    df.columns = df.columns.str.replace('%', 'percent')
    df.columns = df.columns.str.replace(':', '')
    return df


def main() -> None:
    """
    The main function.
    """
    ddoyle_login_params = {
    'username': 'ddoyle',
    'password': 'chn5nE$L4D',
    'host': 'mdc-db.marsdd.com',
    # 'dbname': 'mdc_dataops'
    'dbname': 'mdc_db'
    }

    db = DbConn(creds=ddoyle_login_params, local=True)
    csv_file = select_csv_file()
    # csv_file = os.path.join(os.getcwd(), 'datalake/test.csv')
    df = pd.read_csv(csv_file)
    df = clean_columns(df)
    # Create new file for datalake version of upload
    csv_file = f'{csv_file.strip(".csv")}_datalake.csv'
    
    #Add metadata columns prior to upload
    schema_name = select_schema()
    table_name = input('Enter desired table name to write:\n')
    df_datalake = df.copy()
    df_datalake['metadata_created'] = datetime.datetime.now()
    df_datalake['metadata_table_name'] = f'{schema_name}.{table_name}'
    df_datalake.to_csv(csv_file, index=False)
    
    # need to write to the schema name


    file_path = upload_to_drive(csv_file)
    batch_option = batch_creation_flag()

    if batch_option == 'Create New Batch ID':
        batch_id =  create_batch_id(db, file_path)
        print(f'BATCH ID {batch_id} CREATED')
    else:
        while True:
            try:
                batch_id = int(input("Enter existing batch ID: \n "))
                break
            except ValueError:
                print("That's not an integer. Try again.")
                
                
    
    df['batch_id'] = batch_id
    df['created'] = datetime.datetime.now()
    
    db.create_table_from_df(schema_name, table_name, df)
    db.db_close()


if __name__ == '__main__':
    main()




'''
TODO:

'''
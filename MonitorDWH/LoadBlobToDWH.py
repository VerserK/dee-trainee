"""
Created on Thu Jun 29 13:50:32 2023

@author: trainee.nongnaphat
"""
from datetime import datetime, timedelta
import logging
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import pandas as pd
import requests
import pyodbc 

current_date = datetime.now()
duration = timedelta(hours = 2)
logging.info(current_date)

yest_date = (current_date - duration).strftime("y=%Y/m=%m/d=%d")
logging.info("Yesterday Date: " + yest_date)

# enter credentials
account_name = 'dwhwebstorage'
account_key = 'A8aP+xOBBD5ahXo9Ch6CUvzsqkM5oyGn1/R3kcFcNSrZw4aU0nE7SQCBhHQFYif1AEPlZ4/pAoP/+AStKRerPQ=='
container_name = 'insights-logs-sqlsecurityauditevents'

# create a client to interact with blob storage
connect_str = 'DefaultEndpointsProtocol=https;AccountName=' + account_name + ';AccountKey=' + account_key + ';EndpointSuffix=core.windows.net'
blob_service_client = BlobServiceClient.from_connection_string(connect_str)

# use the client to connect to the container
container_client = blob_service_client.get_container_client(container_name)

def run():
    #get a list of all blob files in the container
    blob_list = []
    for blob_i in container_client.list_blobs():
        if blob_i.name[142:158] == yest_date:
            blob_list.append(blob_i.name)
        
    df_list = []
    # generate a shared access signiture for files and load them into Python

    for blob_i in blob_list:
        # generate a shared access signature for each blob file
        sas_i = generate_blob_sas(account_name = account_name,
                                container_name = container_name,
                                blob_name = blob_i,
                                account_key=account_key,
                                permission=BlobSasPermissions(read=True),
                                expiry=datetime.utcnow() + timedelta(hours=1))
        
        sas_url = 'https://' + account_name+'.blob.core.windows.net/' + container_name + '/' + blob_i + '?' + sas_i
        
        df = pd.read_json(sas_url, lines=True)
        df_list.append(df)
        
        logging.info("Read: " + blob_i)
        
    df_combined = pd.concat(df_list, ignore_index=True)

    # convert nested JSON into a Pandas DataFrame
    df_combined_normalized = pd.json_normalize(df_combined.to_dict(orient='records'))

    # convert string to datetime format
    df_combined_normalized["originalEventTimestamp"] = pd.to_datetime(df_combined_normalized["originalEventTimestamp"])

    # rename the columns
    df_combined_normalized = df_combined_normalized.rename(columns={"properties.action_name": "action_name",
                                                                    "properties.succeeded": "succeeded",
                                                                    "properties.session_id": "session_id",
                                                                    "properties.object_id": "object_id",
                                                                    "properties.transaction_id": "transaction_id",
                                                                    "properties.client_ip": "client_ip",
                                                                    "properties.session_server_principal_name": "session_server_principal_name",
                                                                    "properties.server_principal_name": "server_principal_name",
                                                                    "properties.database_principal_name":"database_principal_name",
                                                                    "properties.database_name": "database_name",
                                                                    "properties.object_name": "object_name",
                                                                    "properties.application_name": "application_name",
                                                                    "properties.host_name": "host_name"})

    # selected only wanted columns
    # blob_df = df_combined_normalized
    blob_df = df_combined_normalized[['originalEventTimestamp', 'action_name', 'succeeded', 'session_id', 'object_id', 'transaction_id', 'client_ip',
                                    'session_server_principal_name', 'server_principal_name', 'database_principal_name', 'database_name', 'object_name', 
                                    'application_name', 'host_name']]
    select_blob_df = blob_df[blob_df["action_name"] == "SELECT"]

    # create SQL server connection string
    server = 'dwhsqldev01.database.windows.net'
    database = 'DWHStorage'
    username = 'boon'
    password = 'DEE@DA123'
    driver = '{ODBC Driver 17 for SQL Server}'
    # table = 'dbo.Customermaster'
    connectionString = 'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password

    # create database connection instance
    try:
        conn = pyodbc.connect(connectionString)
        logging.info("Connection Success")
    except pyodbc.DatabaseError as e:
        logging.info("Database Error: ")
        logging.info(str(e.value[1]))
    except pyodbc.Error as e:
        logging.info("Connection Error: ")
        logging.info(str(e.vale[1]))
        
    # specify columns that want to import
    columns = ['originalEventTimestamp', 'action_name', 'succeeded', 'session_id', 'object_id', 'transaction_id', 'client_ip',
            'session_server_principal_name', 'server_principal_name', 'database_principal_name', 'database_name', 'object_name', 
            'application_name', 'host_name']
    records = select_blob_df[columns].values.tolist()

    date = (current_date - duration).strftime("%Y-%m-%d")
    url = 'https://notify-api.line.me/api/notify'
    token = 'IRaKin5u1mtD4Ut4PIcEJUWWQzwvEqj0m4H9ddZBEgb'
    headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer '+ token}

    # create a cursor connection for Customermaster DB
    insert = '''
        INSERT INTO DWHStorage.dbo.dwhstorage (originalEventTimestamp, action_name, succeeded, session_id, object_id, 
                                            transaction_id, client_ip, session_server_principal_name, server_principal_name, 
                                            database_principal_name, database_name, object_name, application_name, host_name) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    '''

    try:
        logging.info("Running....")
        cursor = conn.cursor()
        cursor.executemany(insert, records)
        cursor.commit();
        
        message = "Load data on date {} successfully!!".format(date)
        requests.post(url, headers = headers, data = {'message': message})
        logging.info('Notification sent successfully!')
    except Exception as e:
        cursor.rollback()
        logging.info(e)
    finally:
        logging.info("Connection close...")
        cursor.close()
        conn.close()
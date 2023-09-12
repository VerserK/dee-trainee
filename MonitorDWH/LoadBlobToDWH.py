"""
Created on Thu Jun 29 13:50:32 2023

@author: trainee.nongnaphat
"""
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import sqlalchemy as sa
from sqlalchemy import exc
import pyodbc
import urllib
import pandas as pd
import requests
import logging 

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
    current_date = datetime.now() - timedelta(days = 3)
    current_date = current_date.strftime("y=%Y/m=%m/d=%d")
    logging.info("Current date: " + current_date)

    # one_day = timedelta(days = 1, hours = 0, minutes = 0)
    # # two_day = timedelta(days = 2, hours = 0, minutes = 0)

    # day_sub_1 = current_date
    # day_sub_2 = current_date - one_day

    # # convert datetime object to string date
    # day_sub_1 = day_sub_1.strftime("y=%Y/m=%m/d=%d")
    # logging.info("Day-1: " + day_sub_1)

    # day_sub_2 = day_sub_2.strftime("y=%Y/m=%m/d=%d")
    # logging.info("Day-2: " + day_sub_2)

    # get a list of all blob files in the container
    blob_list = []
    for blob_i in container_client.list_blobs():
    
        # date = day - 2
        if blob_i.name[142:158] == current_date:
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
    # df_combined_normalized["originalEventTimestamp"] = df_combined_normalized["originalEventTimestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

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

    # choose action_name = 'SELECT'
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
    timeout_value = 60

    connection_string = (
        'DRIVER=' + driver +
        ';SERVER=' + server +
        ';PORT=1433;DATABASE=' + database +
        ';UID=' + username + ';PWD=' + password
    )

    url = 'https://notify-api.line.me/api/notify'
    token = 'IRaKin5u1mtD4Ut4PIcEJUWWQzwvEqj0m4H9ddZBEgb'
    headers = {'content-type':'application/x-www-form-urlencoded','Authorization':'Bearer '+ token}

    # create database connection instance
    try:
        param = urllib.parse.quote_plus(connection_string)
        engine = sa.create_engine('mssql+pyodbc:///?odbc_connect=%s;ConnectionTimeout=%d' % (param, timeout_value))
        conn = engine.connect()
        logging.info("Connected to the database successfully!")
    except exc.SQLAlchemyError as e:
        logging.info("An error occurred: %s", str(e))
        message = "An error occurred: {}".format(str(e))
        requests.post(url, headers = headers, data = {'message': message})
        
    # write data to a SQL database    
    try:
        logging.info("Running....")
        select_blob_df.to_sql('dwhstorage', conn, if_exists='append', index=False, chunksize=1000)

        message = "Load data on date {} successfully!!".format(current_date)
        requests.post(url, headers = headers, data = {'message': message})
        logging.info("Inserted successfully!")
    except exc.SQLAlchemyError as e:
        logging.info("An error occurred: %s", str(e))
        message = "An error occurred: {}".format(str(e))
        requests.post(url, headers = headers, data = {'message': message})
    finally:
        conn.close()
        logging.info("Database connection closed.")
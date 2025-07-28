import functions_framework
import requests, re, io, json, os, base64
import pandas as pd

from bs4 import BeautifulSoup
from google.cloud import storage
from math import log
from google.cloud import secretmanager
from google.cloud import pubsub_v1
from google.cloud import firestore
from datetime import datetime as dt


###>>>>> GCP CLIENT and GCP param <<<<<###
CS = storage.Client()
PS = pubsub_v1.PublisherClient()
DB = firestore.Client()
project_id = os.getenv("GCP_PROJECT_ID")



################################# CLOUD RUN FUNCTION #################################
@functions_framework.cloud_event
def pubsub_entry_point(cloud_event):
    print('-------------------------------------------------------------------')
    print(f'------------------------START OF THE PIPELINE---------------------')
    logging_obj = LoggingJobRun('768945')

    try:
        #--- extracting secret manager params ---#
        project_name = get_secret_manager_key(project_id, 'project_name')
        region_name = get_secret_manager_key(project_id, 'region_name')

        p1_bucket_name = get_secret_manager_key(project_id, 'p1_bucket_name')
        p1_archive_bucket_name = get_secret_manager_key(project_id, 'p1_archive_bucket_name')

        p2_bucket_name = get_secret_manager_key(project_id, 'p2_bucket_name')
        p2_archive_bucket_name = get_secret_manager_key(project_id, 'p2_archive_bucket_name')

        email_add = get_secret_manager_key(project_id, 'email_add')
        #--- extracting secret manager params ---#

        data = cloud_event.data
        # print(type(data))
        print(data)

    ###>>>>> Push Event to Pubsub for notification <<<<<###
        publish_notif(cloud_event, data)
    ###>>>>> Push Event to Pubsub for notification <<<<<###
        
    ###>>>>> Calculate the mean and standard deviation <<<<<###
        print('Calculating the Mean and Standard Deviation...')
        json_content_string = download_gcs_file(p2_bucket_name, "population_data.json")
        df_p2 = get_df_p2(json_content_string)

        filtered_df = df_p2[(df_p2['Year'] >= 2013) & (df_p2['Year'] <= 2018)]
        population_mean = filtered_df['Population'].mean()
        population_std = filtered_df['Population'].std()

        print(f"Mean Population:     {population_mean:,.0f}")
        print(f"Standard Deviation:  {population_std:,.0f}")
        print('===============================================')
    ###>>>>> Calculate the mean and standard deviation <<<<<###

    ###>>>>> Calculate the BestYear for every Series <<<<<###
        print('Calculating the best Year for every Series...')
        json_content_string = download_gcs_file(p1_bucket_name, "pub/time.series/pr/pr.data.0.Current")
        df_p1 = get_df_p1(json_content_string)

        groupedby_dfp1 = df_p1.groupby(["series_id", "year"])["value"].sum().reset_index()
        best_years = groupedby_dfp1.loc[groupedby_dfp1.groupby("series_id")["value"].idxmax()]

        print(best_years.head(10))
        print('===============================================')
    ###>>>>> Calculate the BestYear for every Series <<<<<###

    ###>>>>> Final Report <<<<<###
        print('Generating final report for a specific series...')
        filteredby = df_p1[(df_p1['series_id'] == 'PRS30006032') & (df_p1['period'] == 'Q01')]
        df_p2.columns = df_p2.columns.str.lower()
        results = pd.merge(filteredby, df_p2, on='year', how='inner')
        final_report = results[['series_id', 'year', 'period', 'value', 'population']]
        print(final_report)
    ###>>>>> Final Report <<<<<###

        print('-------------------------------------------------------------------------------------')
        print(f'--------------------------------- END OF THE PIPELINE ------------------------------')
    except Exception as e:
        err = str(e).replace('\n', ' ').replace('\n', ' ')
        err_message = f"Pipeline Error: \\n{err}"
        print(err_message)
        logging_obj.insert_logging_details(err_message)
        logging_obj.write_error_to_cloud_logging(err_message)
        print('-------------------------------------------------------------------------------------')
        print(f'--------------------------- END OF THE PIPELINE with ERRORS-------------------------')


def publish_notif(cloud_event, data):
    topic_path = PS.topic_path(project_id, "notif-gcs-bucket-finalized")

    my_dict = {"payload": {
            "Event Triggered from Bucket" : data["bucket"]
            , "Event Type" : cloud_event["type"]
            , "Event ID" : cloud_event["id"]
            , "File Name Uploaded" :  data["name"]
            , "Time Created" : data["timeCreated"]
            , "Time Updated" : data["updated"]
        }
    }

    ps_data = json.dumps(my_dict).encode("utf-8")
    future = PS.publish(topic_path, ps_data)
    print(future)

def get_secret_manager_key(project_id, secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
 
    payload = response.payload.data.decode("UTF-8")
    return payload

def download_gcs_file(p2_bucket_name, file_name):
    # Download the file content from GCS
    bucket = CS.bucket(p2_bucket_name)
    blob = bucket.blob(file_name)
    json_content_string = blob.download_as_text()

    return json_content_string

def get_df_p2(json_content_string):
    # Load the string into a Python dictionary
    data_dict = json.loads(json_content_string)

    # print(data_dict['data'])
    df_p2 = pd.DataFrame(data_dict['data'])
    # Strip whitespace from column names
    df_p2.columns = df_p2.columns.str.strip()

    return df_p2

def get_df_p1(json_content_string):
    df_p1 = pd.read_csv(io.StringIO(json_content_string), sep='\\s+')
    # Strip whitespace from column names
    df_p1.columns = df_p1.columns.str.strip()
    df_p1['value'] = pd.to_numeric(df_p1['value'], errors='coerce')

    # Strip whitespace from column values
    df_p1['series_id'] = df_p1['series_id'].str.strip()

    return df_p1



class LoggingJobRun:
    def __init__(self, de_job_id):
        self.de_job_id = de_job_id

    def insert_logging_details(self, err_message):
        pass

    def write_error_to_cloud_logging(self, err_message):
        import logging
        import google.cloud.logging
 
        # Instantiates a client
        client = google.cloud.logging.Client()
 
        # Retrieves a Cloud Logging handler
        client.setup_logging()
 
        # Emits the data using the standard logging module
        logging.error(err_message)





################################# LOCAL TEST RUN #################################
if __name__ == "__main__":
    project_id = os.getenv("GCP_PROJECT_ID")
    print(project_id)
    pipeline_name = 'rearc_data_quest_challenge'
    de_job_id = '123'
    cloud_event = {"type":"google.cloud.storage.object.v1.finalized", "id":"15648072656338516"}
    data = {"bucket": "datausa_io_landing", "name":"population_data.json", "timeCreated":"2025-07-28T04:26:52.208Z", "updated":"2025-07-28T04:26:52.208Z"}

    print('-------------------------------------------------------------------------------------')
    print(f'------------------------START OF THE PIPELINE [{pipeline_name}]---------------------')
    logging_obj = LoggingJobRun(de_job_id)

    try:
        #--- extracting secret manager params ---#
        project_name = get_secret_manager_key(project_id, 'project_name')
        region_name = get_secret_manager_key(project_id, 'region_name')

        p1_bucket_name = get_secret_manager_key(project_id, 'p1_bucket_name')
        p1_archive_bucket_name = get_secret_manager_key(project_id, 'p1_archive_bucket_name')

        p2_bucket_name = get_secret_manager_key(project_id, 'p2_bucket_name')
        p2_archive_bucket_name = get_secret_manager_key(project_id, 'p2_archive_bucket_name')

        email_add = get_secret_manager_key(project_id, 'email_add')
        #--- extracting secret manager params ---#

    ###>>>>> Push Event to Pubsub for notification <<<<<###
        publish_notif(cloud_event, data)
    ###>>>>> Push Event to Pubsub for notification <<<<<###
        
    ###>>>>> Calculate the mean and standard deviation <<<<<###
        print('Calculating the Mean and Standard Deviation...')
        json_content_string = download_gcs_file(p2_bucket_name, "population_data.json")
        df_p2 = get_df_p2(json_content_string)

        filtered_df = df_p2[(df_p2['Year'] >= 2013) & (df_p2['Year'] <= 2018)]
        population_mean = filtered_df['Population'].mean()
        population_std = filtered_df['Population'].std()

        print(f"Mean Population:     {population_mean:,.0f}")
        print(f"Standard Deviation:  {population_std:,.0f}")
        print('===============================================')
    ###>>>>> Calculate the mean and standard deviation <<<<<###

    ###>>>>> Calculate the BestYear for every Series <<<<<###
        print('Calculating the best Year for every Series...')
        json_content_string = download_gcs_file(p1_bucket_name, "pub/time.series/pr/pr.data.0.Current")
        df_p1 = get_df_p1(json_content_string)

        groupedby_dfp1 = df_p1.groupby(["series_id", "year"])["value"].sum().reset_index()
        best_years = groupedby_dfp1.loc[groupedby_dfp1.groupby("series_id")["value"].idxmax()]

        print(best_years.head(10))
        print('===============================================')
    ###>>>>> Calculate the BestYear for every Series <<<<<###

    ###>>>>> Final Report <<<<<###
        print('Generating final report for a specific series...')
        filteredby = df_p1[(df_p1['series_id'] == 'PRS30006032') & (df_p1['period'] == 'Q01')]
        df_p2.columns = df_p2.columns.str.lower()
        results = pd.merge(filteredby, df_p2, on='year', how='inner')
        final_report = results[['series_id', 'year', 'period', 'value', 'population']]
        print(final_report)
    ###>>>>> Final Report <<<<<###
        print('-------------------------------------------------------------------------------------')
        print(f'--------------------------------- END OF THE PIPELINE ------------------------------')
    except Exception as e:
        err = str(e).replace('\n', ' ').replace('\n', ' ')
        err_message = f"Pipeline Error: \\n{err}"
        print(err_message)
        logging_obj.insert_logging_details(err_message)
        logging_obj.write_error_to_cloud_logging(err_message)
        print('-------------------------------------------------------------------------------------')
        print(f'--------------------------- END OF THE PIPELINE with ERRORS-------------------------')
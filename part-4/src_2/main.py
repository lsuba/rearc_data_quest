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




################################# CLOUD RUN FUNCTION #################################
@functions_framework.cloud_event
def pubsub_entry_point(cloud_event):
    print('-------------------------------------------------------------------')
    print(f'------------------------START OF THE PIPELINE---------------------')
    # logging_obj = LoggingJobRun(de_job_id)

    try:
        data = base64.b64decode(cloud_event.data["message"]["data"])
        data_json = json.loads(data)
        print(data_json)

    # ###>>>>> Part 1 <<<<<###
    #     print('Staring [PART-1] challenge...')
    #     #NOTE: Add User-Agent with email for contact per BLS access policy
    #     # pr_base_url = "https://download.bls.gov/pub/time.series/pr/"
    #     bls_url = "https://download.bls.gov/pub/time.series"
    #     user_agent_value = f"Python Script ({email_add})"
    #     headers = {
    #         "User-Agent": user_agent_value
    #     }  

    #     unique_filenames_dir = download_bls_data(bls_url, 'pr', headers)
    #     print(f"✅ Found {len(unique_filenames_dir)} files in the directory:\n{unique_filenames_dir}")
    #     print('\n')
    #     global_bucket = CS.bucket(p1_bucket_name)
    #     archive_bucket = CS.bucket(p1_archive_bucket_name)
    #     transfer_files_to_bucket(bls_url, unique_filenames_dir, 'pr', headers, p1_bucket_name, global_bucket)
    # ###>>>>> Part 1 <<<<<###

    # ###>>>>> Part 2 <<<<<###
    #     print('Staring [PART-2] challenge...')
    #     global_bucket = CS.bucket(p2_bucket_name)
    #     fetch_data_and_upload_to_gcs("Nation", "2013", p2_bucket_name, global_bucket)
    # ###>>>>> Part 2 <<<<<###

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



# def get_secret_manager_key(project_id, secret_id):
#     client = secretmanager.SecretManagerServiceClient()
#     name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
#     response = client.access_secret_version(request={"name": name})
 
#     payload = response.payload.data.decode("UTF-8")
#     return payload

# def download_bls_data(bls_url, sub_url_dir, headers):
#     url = f'{bls_url}/{sub_url_dir}/'    

#     #--- Fetch the directory page ---#
#     response = requests.get(url, headers=headers)
#     response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

#     #--- Parse the HTML ---#
#     soup = BeautifulSoup(response.text, 'html.parser')

#     files_dir = []
#     for link in soup.find_all('a'):
#         file_link = link.get('href')
#         # file_name = file_link.split('/')[-1]
#         # if re.search('pr.', file_name): 
#         files_dir.append(file_link)
#     return files_dir

# def transfer_files_to_bucket(bls_url, unique_files_dir, suffix_fn, headers, p1_bucket_name, global_bucket):
#     for each_file in unique_files_dir:
#         if re.search(f'{suffix_fn}.', each_file):
#             # print(each_file)
#             for_cs_each_file = each_file.lstrip('/')
#             # print(for_cs_each_file)
#             extracted_fn = each_file.split('/')[-1]
#             file_url = f'{bls_url}/{suffix_fn}/{extracted_fn}'
#             print(file_url) 

#             #--- Download the file content ---#
#             file_response = requests.get(file_url, headers=headers)
#             file_response.raise_for_status()
#             file_data = file_response.content
#             fmt_file_size = sizeof_fmt(len(file_data))
               
#             # ### destination_blob_name = f"{source_folder_path}/{each_file}"
#             #NOTE: somehow GCS functionality will not upload same filename, it will always overwrite it
#             upload_to_gcs(file_data, p1_bucket_name, for_cs_each_file, extracted_fn, fmt_file_size, global_bucket)
#             print('==============================================')
 
# def upload_to_gcs(file_data, p1_bucket_name, destination_blob_name, extracted_fn, fmt_file_size, global_bucket):
#     """Upload file data to a Google Cloud Storage bucket."""      
#     blob = global_bucket.blob(destination_blob_name)    
#     blob.upload_from_string(file_data)    
#     print(f"Filename [{extracted_fn}] with {fmt_file_size} uploaded to [{p1_bucket_name}]")
 
# def sizeof_fmt(num):
#     """Human friendly file size"""
#     unit_list = list(zip(['bytes', 'kB', 'MB', 'GB', 'TB', 'PB'], [0, 0, 1, 2, 2, 2]))
 
#     if num > 1:
#         exponent = min(int(log(num, 1024)), len(unit_list) - 1)
#         quotient = float(num) / 1024**exponent
#         unit, num_decimals = unit_list[exponent]
#         format_string = '{:.%sf} {}' % (num_decimals)
#         return format_string.format(quotient, unit)
#     if num == 0:
#         return '0 bytes'
#     if num == 1:
#         return '1 byte'
    
# def fetch_data_and_upload_to_gcs(nation_or_state, year, p2_bucket_name, global_bucket):
#     # The API endpoint provided
#     url = f"https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"

#     fn = 'population_data.json'

#     #--- Fetch the data from the API ---#
#     print("Fetching data from API...")
#     response = requests.get(url)
#     response.raise_for_status()
#     api_data = response.json() # Get the data as a Python dictionary
#     fmt_file_size = sizeof_fmt(len(api_data))
#     print("✅ Data fetched successfully.")

#     #--- Convert the Python dictionary to a JSON formatted string ---#
#     json_string = json.dumps(api_data, indent=4)

#     #--- Upload to Cloud Storage bucket ---#
#     upload_to_gcs(json_string, p2_bucket_name, fn, fn, fmt_file_size, global_bucket)


class LoggingJobRun:
    def __init__(self, de_job_id):
        self.de_job_id = de_job_id

    def insert_logging_details():
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

    ###>>>>> Part 1 <<<<<###
        #NOTE: Add User-Agent with email for contact per BLS access policy
        # pr_base_url = "https://download.bls.gov/pub/time.series/pr/"
        bls_url = "https://download.bls.gov/pub/time.series"
        user_agent_value = f"Python Script ({email_add})"
        headers = {
            "User-Agent": user_agent_value
        }  

        unique_filenames_dir = download_bls_data(bls_url, 'pr', headers)
        print(f"✅ Found {len(unique_filenames_dir)} files in the directory:\n{unique_filenames_dir}")
        print('\n')
        global_bucket = CS.bucket(p1_bucket_name)
        archive_bucket = CS.bucket(p1_archive_bucket_name)
        transfer_files_to_bucket(bls_url, unique_filenames_dir, 'pr', headers, p1_bucket_name, global_bucket)
    ###>>>>> Part 1 <<<<<###

    ###>>>>> Part 2 <<<<<###
        global_bucket = CS.bucket(p2_bucket_name)
        fetch_data_and_upload_to_gcs("Nation", "2013", p2_bucket_name, global_bucket)
    ###>>>>> Part 2 <<<<<###

    ###>>>>> Part 3 <<<<<###

    ###>>>>> Part 3 <<<<<###

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
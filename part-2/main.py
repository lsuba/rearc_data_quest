import requests, re, json, os

from google.cloud import storage
from math import log
from google.cloud import secretmanager


###>>>>> GCP CLIENT and GCP param <<<<<###
CS = storage.Client()



def fetch_data_and_upload_to_gcs(nation_or_state, year):
    # The API endpoint provided
    # url = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
    url = f"https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"

    fn = 'population_data.json'

    #--- Fetch the data from the API ---#
    print("Fetching data from API...")
    response = requests.get(url)
    response.raise_for_status()
    api_data = response.json() # Get the data as a Python dictionary
    fmt_file_size = sizeof_fmt(len(api_data))
    print("✅ Data fetched successfully.")

    #--- Convert the Python dictionary to a JSON formatted string ---#
    json_string = json.dumps(api_data, indent=4)

    #--- Upload to Cloud Storage bucket ---#
    upload_to_gcs(json_string, p2_bucket_name, fn, fn, fmt_file_size)



def upload_to_gcs(file_data, p2_bucket_name, destination_blob_name, extracted_fn, fmt_file_size):
    """Upload file data to a Google Cloud Storage bucket."""      
    blob = global_bucket.blob(destination_blob_name)    
    blob.upload_from_string(
        data=file_data
        , content_type='application/json'   # Set the content type for proper handling
        )     
    print(f"Filename [{extracted_fn}] with {fmt_file_size} uploaded to [{p2_bucket_name}]")

def sizeof_fmt(num):
    """Human friendly file size"""
    unit_list = list(zip(['bytes', 'kB', 'MB', 'GB', 'TB', 'PB'], [0, 0, 1, 2, 2, 2]))
 
    if num > 1:
        exponent = min(int(log(num, 1024)), len(unit_list) - 1)
        quotient = float(num) / 1024**exponent
        unit, num_decimals = unit_list[exponent]
        format_string = '{:.%sf} {}' % (num_decimals)
        return format_string.format(quotient, unit)
    if num == 0:
        return '0 bytes'
    if num == 1:
        return '1 byte'  

def get_secret_manager_key(project_id, secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
 
    payload = response.payload.data.decode("UTF-8")
    return payload



if __name__ == "__main__":
    project_id = os.getenv("GCP_PROJECT_ID")
    try:
        # The name of the file to save the data in
        output_filename = "population_data.json"

        #--- extracting secret manager params ---#
        project_name = get_secret_manager_key(project_id, 'project_name')
        region_name = get_secret_manager_key(project_id, 'region_name')

        p2_bucket_name = get_secret_manager_key(project_id, 'p2_bucket_name')
        p2_archive_bucket_name = get_secret_manager_key(project_id, 'p2_archive_bucket_name')
        global_bucket = CS.bucket(p2_bucket_name)
        archive_bucket = CS.bucket(p2_archive_bucket_name)
        #--- extracting secret manager params ---#

        fetch_data_and_upload_to_gcs("Nation", "2013")
    except Exception as e:
        err = str(e).replace('\n', ' ').replace('\n', ' ')
        err_message = f"Pipeline Error: \\n{err}"
        print(err_message)
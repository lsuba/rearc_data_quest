import requests, re, os

from bs4 import BeautifulSoup
from google.cloud import storage
from math import log
from google.cloud import secretmanager


###>>>>> GCP CLIENT and GCP param <<<<<###
CS = storage.Client()


# Find all hyperlink (<a>) tags and extract the filenames
# This list comprehension gets the 'href' from each 'a' tag,
# ignoring parent directory links or other non-file links.
# files_dir = [ link.get('href') for link in soup.find_all('a') ]
# unique_files_dir = list(set(files_dir))

def download_bls_data(bls_url, sub_url_dir, headers):
    url = f'{bls_url}/{sub_url_dir}/'    

    #--- Fetch the directory page ---#
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)

    #--- Parse the HTML ---#
    soup = BeautifulSoup(response.text, 'html.parser')

    files_dir = []
    for link in soup.find_all('a'):
        file_link = link.get('href')
        # file_name = file_link.split('/')[-1]
        # if re.search('pr.', file_name): 
        files_dir.append(file_link)
    return files_dir

def transfer_files_to_bucket(unique_files_dir, suffix_fn, headers):
    for each_file in unique_files_dir:
        if re.search(f'{suffix_fn}.', each_file):
            # print(each_file)
            for_cs_each_file = each_file.lstrip('/')
            # print(for_cs_each_file)
            extracted_fn = each_file.split('/')[-1]
            file_url = f'{bls_url}/{suffix_fn}/{extracted_fn}'
            print(file_url) 

            #--- Download the file content ---#
            file_response = requests.get(file_url, headers=headers)
            file_response.raise_for_status()
            file_data = file_response.content
            fmt_file_size = sizeof_fmt(len(file_data))
               
            # ### destination_blob_name = f"{source_folder_path}/{each_file}"
            #NOTE: somehow GCS functionality will not upload same filename, it will always overwrite it
            upload_to_gcs(file_data, p1_bucket_name, for_cs_each_file, extracted_fn, fmt_file_size)
            print('==============================================')
 
def upload_to_gcs(file_data, p1_bucket_name, destination_blob_name, extracted_fn, fmt_file_size):
    """Upload file data to a Google Cloud Storage bucket."""      
    blob = global_bucket.blob(destination_blob_name)    
    blob.upload_from_string(file_data)    
    print(f"Filename [{extracted_fn}] with {fmt_file_size} uploaded to [{p1_bucket_name}]")
 
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
    # pass
    project_id = os.getenv("GCP_PROJECT_ID")
    try:
        #--- extracting secret manager params ---#
        project_name = get_secret_manager_key(project_id, 'project_name')
        region_name = get_secret_manager_key(project_id, 'region_name')

        p1_bucket_name = get_secret_manager_key(project_id, 'p1_bucket_name')
        p1_archive_bucket_name = get_secret_manager_key(project_id, 'p1_archive_bucket_name')
        global_bucket = CS.bucket(p1_bucket_name)
        archive_bucket = CS.bucket(p1_archive_bucket_name)

        email_add = get_secret_manager_key(project_id, 'email_add')
        #--- extracting secret manager params ---#
        
        #NOTE: Add User-Agent with email for contact per BLS access policy
        # pr_base_url = "https://download.bls.gov/pub/time.series/pr/"
        bls_url = "https://download.bls.gov/pub/time.series"
        user_agent_value = f"Python Script ({email_add})"
        headers = {
            "User-Agent": user_agent_value
        }    
        # print(response)
        # print(soup)

        unique_filenames_dir = download_bls_data(bls_url, 'pr', headers)
        print(f"✅ Found {len(unique_filenames_dir)} files in the directory:\n{unique_filenames_dir}")
        print('\n')
        transfer_files_to_bucket(unique_filenames_dir, 'pr', headers)
    except Exception as e:
        err = str(e).replace('\n', ' ').replace('\n', ' ')
        err_message = f"Pipeline Error: \\n{err}"
        print(err_message)











# if response.status_code == 200:
#     with open(filename, "wb") as f:
#         f.write(response.content)
#     print(f"{filename} downloaded successfully.")
# else:
#     print(f"Failed to download: {response.status_code}")

# with open(each_file, mode="rb") as cs_file: #-->for bigger file
#     file_data = cs_file.read()
#     fmt_file_data = sizeof_fmt(len(file_data))
#     print(f"Read {fmt_file_data} bytes from SMB file.")
import requests
from requests.structures import CaseInsensitiveDict
import pprint
from pprint import pprint
import json
import hashlib
import subprocess
import os
import etagTomd5sum
import argparse
import re
import pandas as pd
import boto3
from botocore.exceptions import ClientError
import logging
import sys
import time
import datetime
from time import sleep
import random
#import logging
#from logging import Logger
#logging.getLogger('backoff').addHandler(logging.StreamHandler())
import retry_requests_decorator
from retry_requests_decorator import request_with_retry,fatal_code
##########

def md5_checksum(filename):
    m = hashlib.md5()
    with open(filename, 'rb') as f:
        for data in iter(lambda: f.read(1024 * 1024), b''):
            m.update(data)
   
    return m.hexdigest()

def etag_checksum(filename, chunk_size=8 * 1024 * 1024):
    md5s = []
    if os.path.getsize(filename) < chunk_size:
        with open(filename, 'rb') as f:
            m = hashlib.md5(f.read())
            md5s.append(m)
        print('{}'.format(m.hexdigest()))
        my_etag = '{}'.format(m.hexdigest())
    else:
        with open(filename, 'rb') as f:
            for data in iter(lambda: f.read(chunk_size), b''):
                md5s.append(hashlib.md5(data).digest())
        m = hashlib.md5(b"".join(md5s))
        print('{}-{}'.format(m.hexdigest(), len(md5s)))
        my_etag =  '{}-{}'.format(m.hexdigest(), len(md5s))
    return my_etag


def etag_compare(filename, etag):
    if '-' in etag and etag == etag_checksum(filename):
        return True
    if '-' not in etag and etag == md5_checksum(filename):
        return True
    return False

#############
ICA_BASE_URL = "https://ica.illumina.com/ica"

def get_project_id(api_key, project_name,max_retries = 3):
    projects = []
    pageOffset = 0
    pageSize = 1000
    page_number = 0
    number_of_rows_to_skip = 0
    api_base_url = ICA_BASE_URL + "/rest"
    endpoint = f"/api/projects?search={project_name}&includeHiddenProjects=true&pageOffset={pageOffset}&pageSize={pageSize}"
    full_url = api_base_url + endpoint	############ create header
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    headers['Connection'] = 'close'
    headers['User-Agent'] = "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36"
    project_id = None
    try:
        #request_params = {"method": "get", "url": full_url,"headers": headers}
        #projectPagedList = request_with_retry(**request_params)
        #sleep(random.uniform(1, 3))
        projectPagedList = None
        response_code = 201
        num_tries = 0
        #projectPagedList = requests.get(full_url, headers=headers)
        while response_code != 200 and num_tries < max_retries:
            num_tries += 1
            #sleep(random.uniform(1, 3))
            if num_tries > 1:
                print(f"NUM_TRIES:\t{num_tries}\tLooking for project {project_name}")
            projectPagedList = requests.get(full_url, headers=headers)
            response_code = projectPagedList.status_code
            projectPagedListResponse = projectPagedList.json()
        if 'totalItemCount' in projectPagedListResponse.keys():
            totalRecords = projectPagedList.json()['totalItemCount']
            while page_number*pageSize <  totalRecords:
                endpoint = f"/api/projects?search={project_name}&includeHiddenProjects=true&pageOffset={number_of_rows_to_skip}&pageSize={pageSize}"
                full_url = api_base_url + endpoint
                #request_params = {"method": "get", "url": full_url, "headers": headers}
                #projectPagedList = request_with_retry(**request_params)
                #sleep(random.uniform(1, 3))
                projectPagedList = requests.get(full_url, headers=headers)
                for project in projectPagedList.json()['items']:
                    projects.append({"name":project['name'],"id":project['id']})
                page_number += 1
                number_of_rows_to_skip = page_number * pageSize
        else:
            for project in projectPagedList.json()['items']:
                projects.append({"name": project['name'], "id": project['id']})
    except:
        raise ValueError(f"Could not get project_id for project: {project_name}")
    if len(projects)>1:
        raise ValueError(f"There are multiple projects that match {project_name}")
    else:
        project_id = projects[0]['id']
        return project_id

def list_data(api_key,sample_query,project_id=None, project_name=None,max_retries = 3):
    filepath_search = False
    if re.search("/",sample_query[0]) is not None:
        filepath_search = True
        sample_query = [re.sub("/", "%2F", x) for x in sample_query][0]
    else:
        sample_query = sample_query[0]
    if project_id is None:
        project_id = get_project_id(api_key, project_name)
    datum = []
    pageOffset = 0
    pageSize = 1000
    page_number = 0
    number_of_rows_to_skip = 0
    api_base_url = ICA_BASE_URL + "/rest"
    if filepath_search is True:
        endpoint = f"/api/projects/{project_id}/data?filePath={sample_query}&filenameMatchMode=FUZZY&filePathMatchMode=FULL_CASE_INSENSITIVE&pageOffset={pageOffset}&pageSize={pageSize}"
    else:
        endpoint = f"/api/projects/{project_id}/data?filename={sample_query}&filenameMatchMode=FUZZY&filePathMatchMode=STARTS_WITH_CASE_INSENSITIVE&pageOffset={pageOffset}&pageSize={pageSize}"
    full_url = api_base_url + endpoint	############ create header
    #print(f"FULL_URL:\t{full_url}")
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    headers['Connection'] = 'close'
    headers['User-Agent'] = "Mozilla/5.0 (Windows NT 6.1; Win64; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36"
    try:
        #request_params = {"method": "get", "url": full_url,"headers": headers}
        #projectDataPagedList = request_with_retry(**request_params)
        projectDataPagedList = None
        response_code = 201
        num_tries = 0
        while response_code != 200 and num_tries < max_retries:
            num_tries += 1
            if num_tries > 1:
                print(f"NUM_TRIES:\t{num_tries}\tLooking for data in the project {project_name}")
            #sleep(random.uniform(1, 3))
            projectDataPagedList = requests.get(full_url, headers=headers)
            response_code = projectDataPagedList.status_code
        if 'totalItemCount' in projectDataPagedList.json().keys():
            totalRecords = projectDataPagedList.json()['totalItemCount']
            while page_number*pageSize <  totalRecords:
                if filepath_search is True:
                    endpoint = f"/api/projects/{project_id}/data?filePath={sample_query}&filenameMatchMode=FUZZY&filePathMatchMode=FULL_CASE_INSENSITIVE&pageOffset={pageOffset}&pageSize={pageSize}"
                else:
                    endpoint = f"/api/projects/{project_id}/data?filename={sample_query}&filenameMatchMode=FUZZY&filePathMatchMode=STARTS_WITH_CASE_INSENSITIVE&pageOffset={number_of_rows_to_skip}&pageSize={pageSize}"
                full_url = api_base_url + endpoint  ############ create header
                #request_params = {"method": "get", "url": full_url, "headers": headers}
                #projectDataPagedList = request_with_retry(**request_params)
                num_tries = 0
                projectDataPagedList = None
                while projectDataPagedList is None and num_tries < max_retries:
                    num_tries += 1
                    #sleep(random.uniform(1, 3))
                    projectDataPagedList = requests.get(full_url, headers=headers)
                for projectData in projectDataPagedList.json()['items']:
                        datum.append({"name":projectData['data']['details']['name'],"id":projectData['data']['id'],"path":projectData['data']['details']['path']})
                page_number += 1
                number_of_rows_to_skip = page_number * pageSize
        else:
            for projectData in projectDataPagedList.json()['items']:
                datum.append({"name": projectData['data']['details']['name'], "id": projectData['data']['id'],"path": projectData['data']['details']['path']})

    except:
        raise ValueError(f"Could not get results for project: {project_id} looking for filename: {sample_query}")
    return datum

# create data in ICA and retrieve back data ID
def create_data(api_key,project_name, filename, data_type, folder_id=None, format_code=None,project_id=None,filepath=None,max_retries = 10):
    if project_id is None:
        project_id = get_project_id(api_key, project_name)
    api_base_url = ICA_BASE_URL + "/rest"
    endpoint = f"/api/projects/{project_id}/data"
    full_url = api_base_url + endpoint
    ############ create header
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['Connection'] = 'close'
    headers['X-API-Key'] = api_key
    headers['User-Agent'] = "Mozilla/5.0 (Windows NT 6.1; Win64; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36"
    ########
    payload = {}
    payload['name'] = filename
    fullname = filename
    if filepath is not None:
        filepath_split = filepath.split('/')
        if len(filepath_split) > 1:
            payload['folderPath'] = filepath
            fullname = filepath + filename
    if folder_id is not None:
        payload['folderId'] = folder_id
    if data_type not in ["FILE", "FOLDER"]:
        raise ValueError("Please enter a correct data type to create. It can be FILE or FOLDER.Exiting\n")
    payload['dataType'] = data_type
    ######## let's not specify format type
    #if format_code is not None:
    #    payload['formatCode'] = format_code
    ################
    #request_params = {"method": "post", "url": full_url, "headers": headers,"data": json.dumps(payload)}
    #response = request_with_retry(**request_params)
    response_code = 404
    response = None
    num_tries = 0
    data_id = None
    while (response_code != 201 and num_tries < max_retries) :
        num_tries += 1
        if num_tries > 1:
            print(f"NUM_TRIES:\t{num_tries}\tTrying to create data in {project_name}")
        #sleep(random.uniform(1, 10))
        sleep(1)
        response = requests.post(full_url, headers=headers, data=json.dumps(payload),verify=False)
        response_code = response.status_code
        pprint(json.dumps(response.json()),indent=4)
    if 'data' in response.json().keys():
            data_id = response.json()['data']['id']
    elif response.status_code not in [201, 400]:
        # if filepath exists, then check
        if response.status_code in [409]:
            print(f"File exists at {fullname}.\nChecking {project_id}")
            sleep(5)
            fileresults = list_data(api_key, [ fullname ], project_id)
            for idx,i in enumerate(fileresults):
                 data_id = i['id']
        else:
            print(f"PATH:\t{filepath}\tFOLDER_ID:\t{folder_id}")
            pprint(json.dumps(response.json()),indent=4)
            raise ValueError(f"Could not create data {filename}")
    return data_id

### obtain temporary AWS credentials
def get_temporary_credentials(api_key,project_name,data_id,project_id=None,max_retries = 5):
    if project_id is None:
        project_id = get_project_id(api_key, project_name)
    api_base_url = ICA_BASE_URL + "/rest"
    endpoint = f"/api/projects/{project_id}/data/{data_id}:createTemporaryCredentials"
    full_url = api_base_url + endpoint
    ############ create header
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    headers['Connection'] = 'close'
    headers['User-Agent'] = "Mozilla/5.0 (Windows NT 6.1; Win64; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36"
    payload = {}
    payload['credentialsFormat'] = "RCLONE"
    ########
    #request_params = {"method": "post", "url": full_url, "headers": headers,"data": json.dumps(payload)}
    #response = request_with_retry(**request_params)
    response = None
    response_code = 404
    num_tries = 0
    while response_code != 200 and num_tries < max_retries:
        num_tries += 1
        if num_tries > 1:
            print(f"NUM_TRIES:\t{num_tries}\tTryint to get creds for {project_name}")
        #sleep(random.uniform(1, 3))
        sleep(1)
        response = requests.post(full_url, headers=headers, data=json.dumps(payload),verify=False)
        response_code = response.status_code
    if response.status_code != 200:
        pprint(json.dumps(response.json()),indent=4)
        raise ValueError(f"Could not get temporary credentials for {data_id}")
    return response.json()

def download_data_from_url(download_url,output_name=None):
    command_base = ["wget"]
    if output_name is not None:
        command_base.append("-O")
        command_base.append(output_name)
    command_base.append(f"{download_url}")
    command_str = " ".join(command_base)
    download_done = os.system(command_str)
    while download_done != 0:
        download_done = os.system(command_str)
    return print(f"Downloading from {download_url}")

def set_temp_credentials(credential_json):
    CREDS = credential_json
    os.environ['AWS_ACCESS_KEY_ID'] = CREDS['rcloneTempCredentials']['config']['access_key_id']
    os.environ['AWS_SESSION_TOKEN'] = CREDS['rcloneTempCredentials']['config']['session_token']
    os.environ['AWS_SECRET_ACCESS_KEY'] = CREDS['rcloneTempCredentials']['config']['secret_access_key']
    return print("Set credentials for upload")

def get_md5_sum(filename):
    command_base = ["md5sum"]
    command_base.append(filename)
    command_base.append(">")
    command_base.append(f"{filename}.md5sum")
    command_str= " ".join(command_base)
    os.system(command_str)
    return print(f"Computed md5 sum for {filename}")

def create_aws_service_object(aws_service_name,credential_json):
   required_aws_obj = boto3.client(              
       aws_service_name,
       aws_access_key_id=credential_json['rcloneTempCredentials']['config']['access_key_id'],
       aws_secret_access_key=credential_json['rcloneTempCredentials']['config']['secret_access_key'],
       aws_session_token=credential_json['rcloneTempCredentials']['config']['session_token'],
       region_name = credential_json['rcloneTempCredentials']['config']['region']
   )
   return required_aws_obj

def upload_file(filename,credential_json):
    try:
        s3 = create_aws_service_object('s3',credential_json)
        s3_uri_split = credential_json['rcloneTempCredentials']['filePathPrefix'].split('/')
        bucket = s3_uri_split[0]
        object_name = "/".join(s3_uri_split[1:(len(s3_uri_split))])
        response = s3.upload_file(filename, bucket, object_name)
    except ClientError as e:
        logging.error(e)
    #command_base = ["aws", "s3","cp"]
    #command_base.append(filename)
    #s3_uri = credential_json['rcloneTempCredentials']['filePathPrefix']
    #command_base.append(f"s3://{s3_uri}")
    #command_str = " ".join(command_base)
    #set_temp_credentials(credential_json)
    #subprocess.run(command_str, shell=True, stdout=subprocess.PIPE)
    return print(f"Uploaded {filename}")

def confirm_md5sum(filename,bucket_name,your_key,credential_json):
    s3 = create_aws_service_object('s3',credential_json)
    s3_uri_split = credential_json['rcloneTempCredentials']['filePathPrefix'].split('/')
    bucket_name = s3_uri_split[0]
    your_key = "/".join(s3_uri_split[1:(len(s3_uri_split))])
    obj_dict = s3.head_object(Bucket=bucket_name, Key=your_key)

    etag = (obj_dict['ETag']).strip("\"")
    #set_temp_credentials(credential_json)
    #aws_command = f"aws s3api head-object --bucket {bucket_name} --key {your_key} --query ETag --output text"
    #lookup = subprocess.run(aws_command, shell=True, stdout=subprocess.PIPE)
    #subprocess_return = lookup.stdout.decode('utf-8').strip('\n').strip("\"")
    #etag = subprocess_return
    print('etag', etag)

    validation = etag_compare(filename, etag)
    print(validation)
    etag_checksum(filename, chunk_size=8 * 1024 * 1024)
    return validation
################
### check folder existence
def does_folder_exist(folder_name,folder_results):
    num_hits = 0
    folder_id = None
    for result_idx,result in enumerate(folder_results):
        if folder_name == result['path']  is not None and re.search("fol", result['id']) is not None:
            num_hits = 1
            folder_id = result['id']
    return  num_hits,folder_id
def does_file_exist(file_name,file_results):
    num_hits = 0
    file_id = None
    for result_idx,result in enumerate(file_results):
        if file_name == result['path']  is not None and re.search("fil", result['id']) is not None:
            num_hits = 1
            file_id = result['id']
    return  num_hits,file_id

### rename files metadata
def load_metadata_table(metadata_table):
    df = pd.read_csv(metadata_table)
    return df
def return_filemetadata(metadata_table):
    dataframe = load_metadata_table(metadata_table)
    rename_dict = {}
    original_names = dataframe['originalFilename']
    new_names = dataframe['newFilename']
    for og_name,new_name in zip(original_names,new_names):
        rename_dict[og_name] = new_name
    return rename_dict
############################################
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id',default=None, type=str, help="ICA project id as seen in Base")
    parser.add_argument('--project_name',default=None, type=str, help="ICA project name")
    parser.add_argument('--run_id', default=None, type=str, help="Sequencing Run identifier")
    parser.add_argument('--input_json', default=None, type=str, help="input JSON listing files from BSSH to transfer")
    parser.add_argument('--metadata_table',  default=None, type=str, help="input metadata table for renaming convention")
    parser.add_argument('--api_key_file', default=None, type=str, help="file that contains API-Key")
    parser.add_argument('--api_key', default=None, type=str, help="String that is the API-Key")
    parser.add_argument('--timeout', default=5, type=int, help="timeout in hours (Default is 5 hours) ")
    args, extras = parser.parse_known_args()
#############
    time_start =  datetime.datetime.now()
    folder_name = "default"
    project_id = args.project_id
    if args.run_id is not None:
        folder_name = args.run_id
    else:
        raise ValueError("Please provide a sequencing run identifier. Will be used for uploads")
    if args.input_json is not None:
        input_json = args.input_json
    else:
        raise ValueError("Please provide an input JSON")
    if args.project_name is not None:
        project_name = args.project_name
    else:
        raise ValueError("Please provide ICA project name")
    my_api_key = None
    if args.api_key_file is not None and args.api_key is None:
        if os.path.isfile(args.api_key_file) is True:
            with open(args.api_key_file, 'r') as f:
                my_api_key = str(f.read().strip("\n"))
    if args.api_key is not None:
        my_api_key = args.api_key
    if my_api_key is None:
        raise ValueError("Need API key")

### read in input JSON of BSSH manifest to downloads
    with open(input_json) as f:
        data = json.load(f)
    if len(data) < 1:
        raise ValueError(f"No files to download for {input_json}")

    if project_id is None:
        project_id = get_project_id(my_api_key,project_name)

    data_dict = {}
    projectdata_results = list_data(my_api_key,["/" + folder_name+"/"],project_id)
    for idx,i in enumerate(projectdata_results):
        if i['path']  not in data_dict.keys():
            data_dict[i['path']] = i

# for each file in input JSON, create data + expose temporary AWS creds
    for files in data:
        time_duration = datetime.datetime.now() - time_start
        if time_duration.seconds > ( args.timeout * 3600 ):
            raise ValueError("Job is taking too long .... exiting!")
        path_split = files['path'].split('/')
        paths = []
        num_hits = 0
        folder_id = None
        #### avoid the parent directory to allow users to define new parent directory name
        for i in range(1,len(path_split)):
            prefix_list = [folder_name]
            prefix_list.extend(path_split[1:i])
            path_join = "/".join(prefix_list)
            if path_join != "":
               paths.append(path_join + "/")
        for p in paths:
            # avoid API calls if we've already looked the folder in the project
            if "/" + p  not in data_dict.keys():
                print(f"Looking for path:\t{p}\n")
                projectdata_results = list_data(my_api_key, ["/" + p], project_id)
                pprint(projectdata_results,indent = 4)
                for idx, i in enumerate(projectdata_results):
                    if i['path']  not in data_dict.keys():
                        data_dict[i['path']] = i
                num_hits,folder_id = does_folder_exist("/"+p,projectdata_results)
                print(f"NUM_HITS: {num_hits}\tFOLDER_ID: {folder_id}\n")
            else:
                print(f"Using lookup table\n")
                num_hits,folder_id = does_folder_exist("/"+p,[data_dict["/"+p]])
                print(f"NUM_HITS: {num_hits}\tFOLDER_ID: {folder_id}\n")
    # Check if folder exists in ICA project
        # if not, then create
            if folder_id is None and num_hits == 0:
                ## ensure we are adding to the appropriate parent folder
                root_folder = [""]
                new_folder_name = p.split('/')[-2]
                folder_create_split = p.split('/')
                for i in range(len(folder_create_split)-2):
                    root_folder.append(folder_create_split[i])
                root_folder_path = "/".join(root_folder)
                root_folder_path = root_folder_path  + "/"
                print(f"Generating folder for {new_folder_name} in {root_folder_path} \n")
                folder_id = create_data(my_api_key,project_name, new_folder_name, "FOLDER", filepath = root_folder_path,project_id=project_id)
                data_dict[root_folder_path + new_folder_name + "/"] = {"name":new_folder_name,"path":root_folder_path + new_folder_name + "/","id":folder_id}
            if folder_id is None:
                raise ValueError(f"Cannot find appropriate folder id for {p} in {project_name}")

        foi = files['path'].split('/')[-1]
        foi_md5 = f"{foi}.md5sum"
        download_url = files['url']
        #print(folder_id)
    # download data from BSSH
        metadata = {}
        if args.metadata_table is not None:
            metadata = return_filemetadata(args.metadata_table)
        ### add in logic to  on whether to rename FASTQs ####
        if args.metadata_table is None or foi not in metadata.keys():
            foi_id  = create_data(my_api_key,project_name, foi, "FILE", folder_id=folder_id,project_id=project_id,filepath="/" + paths[-1] )
            # foi_md5_id = create_data(my_api_key,project_name, foi_md5, "FILE", folder_id=folder_id,project_id=project_id,filepath="/" + paths[-1])
            download_data_from_url(download_url,output_name=foi)
        else:
            foi_id  = create_data(my_api_key,project_name, metadata[foi], "FILE", folder_id=folder_id,project_id=project_id,filepath="/" + paths[-1])
            # foi_md5_id = create_data(my_api_key,project_name, metadata[foi]+".md5sum", "FILE", folder_id=folder_id,project_id=project_id,filepath="/" + paths[-1])
            download_data_from_url(download_url, output_name=metadata[foi])
            foi = metadata[foi]
            foi_md5 = f"{foi}.md5sum"
    # compute md5 sum
        get_md5_sum(foi)

    # upload file and md5sum to ICA
        # creds = get_temporary_credentials(my_api_key,project_name, foi_md5_id,project_id=project_id)
        # set_temp_credentials(creds)
        # upload_file(foi_md5,creds)

        creds = get_temporary_credentials(my_api_key,project_name, foi_id,project_id=project_id)
        #set_temp_credentials(creds)
        upload_file(foi,creds)

    # confirm md5sum
        #print(creds['rcloneTempCredentials']['filePathPrefix'])
        s3_uri_split = creds['rcloneTempCredentials']['filePathPrefix'].split('/')
        bucketname = s3_uri_split[0]
        key_prefix = "/".join(s3_uri_split[1:(len(s3_uri_split))])
        #print(bucketname)
        #print(key_prefix)
        md5_confirmation = confirm_md5sum(foi, bucketname, key_prefix,creds)
        if md5_confirmation is False:
            raise ValueError(f"Issue confirming md5sum for {foi}")
        ######## add verbosity
        dt = str(datetime.datetime.now())
        sys.stderr.write(dt  + " Finished transferring: "  + files['path'] + "\n")
    # remove file and md5sum file once we've confirmed the checksums
        #ßremove_file = "rm -rf " + foi
        #os.system(remove_file)
        #remove_md5_file = "rm -rf " + foi_md5
        #os.system(remove_md5_file)
    #################
    # write out parameter options summary
    transfer_options_summary = "transfer_options_summary.json"
    transfer_options = {}
    transfer_options['project_name'] = project_name
    transfer_options['project_id'] = project_id
    transfer_options['transfer_json'] = args.input_json
    transfer_options['root_output_folder'] = folder_name
    with open(transfer_options_summary, "w") as f:
        for line in json.dumps(transfer_options, indent=4, sort_keys=True).split("\n"):
            print(line, file=f)
    print('All done!\n\n')
if __name__ == '__main__':
    main()

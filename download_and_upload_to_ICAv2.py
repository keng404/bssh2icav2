import requests
from requests.structures import CaseInsensitiveDict
import pprint
from pprint import pprint
import json
import os
import etagTomd5sum
import argparse
import re
import pandas as pd
import sys

ICA_BASE_URL = "https://ica.illumina.com/ica"


def get_project_id(api_key, project_name):
	projects = []
	pageOffset = 0
	pageSize = 30
	page_number = 0
	number_of_rows_to_skip = 0
	api_base_url = ICA_BASE_URL + "/rest"
	endpoint = f"/api/projects?search={project_name}&includeHiddenProjects=true&pageOffset={pageOffset}&pageSize={pageSize}"
	full_url = api_base_url + endpoint	############ create header
	headers = CaseInsensitiveDict()
	headers['Accept'] = 'application/vnd.illumina.v3+json'
	headers['Content-Type'] = 'application/vnd.illumina.v3+json'
	headers['X-API-Key'] = api_key	
	try:
		projectPagedList = requests.get(full_url, headers=headers)
		totalRecords = projectPagedList.json()['totalItemCount']
		while page_number*pageSize <  totalRecords:
			projectPagedList = requests.get(full_url, headers=headers)
			for project in projectPagedList.json()['items']:
				projects.append({"name":project['name'],"id":project['id']})
			page_number += 1
			number_of_rows_to_skip = page_number * pageSize
	except:
		raise ValueError(f"Could not get project_id for project: {project_name}")
	if len(projects)>1:
		raise ValueError(f"There are multiple projects that match {project_name}")
	else:
		return projects[0]['id']

def list_data(api_key,sample_query,project_id):
	datum = []
	pageOffset = 0
	pageSize = 30
	page_number = 0
	number_of_rows_to_skip = 0
	api_base_url = ICA_BASE_URL + "/rest"
	endpoint = f"/api/projects/{project_id}/data?filename={sample_query}&filenameMatchMode=FUZZY&filePathMatchMode=STARTS_WITH_CASE_INSENSITIVE&pageOffset={pageOffset}&pageSize={pageSize}"
	full_url = api_base_url + endpoint	############ create header
	headers = CaseInsensitiveDict()
	headers['Accept'] = 'application/vnd.illumina.v3+json'
	headers['Content-Type'] = 'application/vnd.illumina.v3+json'
	headers['X-API-Key'] = api_key
	try:
		projectDataPagedList = requests.get(full_url, headers=headers)
		if 'totalItemCount' in projectDataPagedList.json().keys():
			totalRecords = projectDataPagedList.json()['totalItemCount']
			while page_number*pageSize <  totalRecords:
				projectDataPagedList = requests.get(full_url, headers=headers)
				for projectData in projectDataPagedList.json()['items']:
						datum.append({"name":projectData['data']['details']['name'],"id":projectData['data']['id']})
				page_number += 1
				number_of_rows_to_skip = page_number * pageSize
	except:
		raise ValueError(f"Could not get results for project: {project_id} looking for filename: {sample_query}")
	return datum

# create data in ICA and retrieve back data ID
def create_data(api_key,project_name, filename, data_type, folder_id=None, format_code=None):
	project_id = get_project_id(api_key, project_name)
	api_base_url = ICA_BASE_URL + "/rest"
	endpoint = f"/api/projects/{project_id}/data"
	full_url = api_base_url + endpoint
	############ create header
	headers = CaseInsensitiveDict()
	headers['Accept'] = 'application/vnd.illumina.v3+json'
	headers['Content-Type'] = 'application/vnd.illumina.v3+json'
	headers['X-API-Key'] = api_key
	########
	payload = {}
	payload['name'] = filename
	if folder_id is not None:
		payload['folderId'] = folder_id
	if data_type not in ["FILE", "FOLDER"]:
		raise ValueError("Please enter a correct data type to create. It can be FILE or FOLDER.Exiting\n")
	payload['dataType'] = data_type
	if format_code is not None:
		payload['formatCode'] = format_code
	response = requests.post(full_url, headers=headers, data=json.dumps(payload))
	if response.status_code != 201:
		pprint(json.dumps(response.json()),indent=4)
		raise ValueError(f"Could not create data {filename}")
	return response.json()['data']['id']

### obtain temporary AWS credentials
def get_temporary_credentials(api_key,project_name,data_id):
	project_id = get_project_id(api_key, project_name)
	api_base_url = ICA_BASE_URL + "/rest"
	endpoint = f"/api/projects/{project_id}/data/{data_id}:createTemporaryCredentials"
	full_url = api_base_url + endpoint
	############ create header
	headers = CaseInsensitiveDict()
	headers['Accept'] = 'application/vnd.illumina.v3+json'
	headers['Content-Type'] = 'application/vnd.illumina.v3+json'
	headers['X-API-Key'] = api_key
	payload = {}
	payload['credentialsFormat'] = "RCLONE"
	########
	response = requests.post(full_url, headers=headers, data=json.dumps(payload))
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
	os.system(command_str)
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

def upload_file(filename,credential_json):
	command_base = ["aws", "s3","cp"]
	command_base.append(filename)
	s3_uri = credential_json['rcloneTempCredentials']['filePathPrefix']
	command_base.append(f"s3://{s3_uri}")
	command_str = " ".join(command_base)
	os.system(command_str)
	return print(f"Uploaded {filename}")
################
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
	args, extras = parser.parse_known_args()
#############
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
	if args.api_key_file is not None:
		if os.path.isfile(args.api_key_file) is True:
			with open(args.api_key_file, 'r') as f:
				my_api_key = str(f.read().strip("\n"))
	if my_api_key is None:
		raise ValueError("Need API key")

	#folder_name = "my_test"
	projectdata_results = list_data(my_api_key,[folder_name],project_id)
	num_hits = 0
	folder_id = None
	for result_idx,result in enumerate(projectdata_results):
		if re.search(folder_name, result['name']) is not None and re.search("fol", result['id']) is not None:
			num_hits = 1
			folder_id = result['id']
        
# Check if folder exists in ICA project
	# if not, then create
	if len(projectdata_results) == 0 or num_hits == 0:
		folder_id = create_data(my_api_key,project_name, folder_name, "FOLDER")
	if folder_id is None:
		raise ValueError(f"Cannot find appropriate folder id for {folder_name} in {project_name}")

### read in input JSON of BSSH manifest to downloads
	with open(input_json) as f:
		data = json.load(f)
	if len(data) < 1:
		raise ValueError(f"No files to download for {input_json}")

# for each file in input JSON, create data + expose temporary AWS creds
	for files in data:
		foi = files['path'].split('/')[-1]
		foi_md5 = f"{foi}.md5sum"
		download_url = files['url']

	# download data from BSSH
		metadata = {}
		if args.metadata_table is not None:
			metadata = return_filemetadata(args.metadata_table)
		### add in logic to  on whether to rename FASTQs ####
		if args.metadata_table is None or foi not in metadata.keys():
			foi_id  = create_data(my_api_key,project_name, foi, "FILE", folder_id=folder_id)
			foi_md5_id = create_data(my_api_key,project_name, foi_md5, "FILE", folder_id=folder_id)
			download_data_from_url(download_url)
		else:
			foi_id  = create_data(my_api_key,project_name, metadata[foi], "FILE", folder_id=folder_id)
			foi_md5_id = create_data(my_api_key,project_name, metadata[foi]+".md5sum", "FILE", folder_id=folder_id)
			download_data_from_url(download_url, output_name=metadata[foi])
			foi = metadata[foi]
			foi_md5 = f"{foi}.md5sum"
	# compute md5 sum
		get_md5_sum(foi)

	# upload file and md5sum to ICA
		creds = get_temporary_credentials(my_api_key,project_name, foi_md5_id)
		set_temp_credentials(creds)
		upload_file(foi_md5_id, creds)

		creds = get_temporary_credentials(my_api_key,project_name, foi_id)
		set_temp_credentials(creds)
		upload_file(foi,creds)

	# confirm md5sum
		s3_uri_split = creds['rcloneTempCredentials']['filePathPrefix'].split('/')
		bucketname = s3_uri_split[0]
		key_prefix = "/".join(s3_uri_split[1:(len(s3_uri_split)-1)])
		md5_confirmation = etagTomd5sum.confirm_md5sum(foi, bucketname, key_prefix)
		if md5_confirmation is False:
			raise ValueError(f"Issue confirming md5sum for {foi}")
	# remove file and md5sum file once we've confirmed the checksums 
		remove_file = "rm -rf " + foi
		os.system(remove_file)
		remove_md5_file = "rm -rf " + foi_md5
		os.system(remove_md5_file)
	#################
if __name__ == '__main__':
	main()

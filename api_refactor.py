import requests
from requests.structures import CaseInsensitiveDict
import os
from pprint import pprint
#curl -X 'GET' \
#  'https://ica.illumina.com/ica/rest/api/projects/asdfasdfa/data?filename=string&filenameMatchMode=FUZZY&filePathMatchMode=STARTS_WITH_CASE_INSENSITIVE' \
#  -H 'accept: application/vnd.illumina.v3+json' \
#  -H 'X-API-Key: Va39()RG6WISWYo4RD'

ICA_BASE_URL="https://ica.illumina.com/ica"

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

my_api_key = None
api_key_file = '/Users/keng/Downloads/ICAv2.isc_b.illumina-api-key.txt'
if api_key_file is not None:
	if os.path.isfile(api_key_file) is True:
		with open(api_key_file, 'r') as f:
			my_api_key = str(f.read().strip("\n"))

y=get_project_id(my_api_key,"ken_test")
#print(y)
x=list_data(my_api_key,"my_test_run",y)
print(x)
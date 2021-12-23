import icav2
from icav2.api import project_data_api
from icav2.api import project_api
def get_project_id(api_key, project_name):
	configuration = icav2.Configuration(
		host=ICA_BASE_URL + '/rest',
	)
	configuration.api_key['ApiKeyAuth'] = api_key
	project_id = "unknown"
	# Enter a context with an instance of the API client
	with icav2.ApiClient(configuration) as api_client:
		# Create an instance of the API class
		api_instance = project_api.ProjectApi(api_client)
		search = project_name  # str | Search string
		include_hidden_projects = False  # bool, none_type | Include hidden projects. (optional) if omitted the server will use the default value of False
		# example passing only required values which don't have defaults set
		# and optional values
		try:
			# Retrieve a list of projects.
			api_response = api_instance.get_projects(search=search, include_hidden_projects=include_hidden_projects)
			for project_item, project in enumerate(api_response['items']):
				#print(project['name'] + '\t' + project['id'])
				project_id = project['id']
		except icav2.ApiException as e:
			print("Exception when calling ProjectApi->get_projects: %s\n" % e)
	return project_id

def list_data(api_key,sample_query,project_id):# Retrieve project ID from the Bench workspace environment
	projectId = project_id
	datum = []
	configuration = icav2.Configuration(
	host = ICA_BASE_URL + '/rest',
	)
	configuration.api_key['ApiKeyAuth'] = api_key
	apiClient = icav2.ApiClient(configuration, header_name="Content-Type",header_value="application/vnd.illumina.v3+json")
	# Create a Project Data API client instance
	projectDataApiInstance = project_data_api.ProjectDataApi(apiClient)
	# List all data in a project
	pageOffset = 0
	pageSize = 30
	page_number = 0
	number_of_rows_to_skip = 0
	try:
		projectDataPagedList = projectDataApiInstance.get_project_data_list(project_id = projectId,page_size = str(pageSize),page_offset=str(pageOffset),filename_match_mode="FUZZY",filename=sample_query)
		totalRecords = projectDataPagedList.total_item_count
		while page_number*pageSize <  totalRecords:
			projectDataPagedList = projectDataApiInstance.get_project_data_list(project_id = projectId,page_size = str(pageSize),page_offset=str(number_of_rows_to_skip),filename_match_mode="FUZZY",filename=sample_query)
			for projectData in projectDataPagedList.items:
					datum.append({"name":projectData.data.details.name,"id":projectData.data.id})
			page_number += 1
			number_of_rows_to_skip = page_number * pageSize
	except icav2.ApiException as e:
		print("Exception when calling ProjectDataAPIApi->get_project_data_list: %s\n" % e)
	return datum
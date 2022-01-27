import download_and_upload_run_to_ICAv2
folder_id = "fol.e3a9f8b646b14948c6fd08d9df169036"
project_id = "281d52f4-8bda-4758-88a6-de364fce472d"
full_path = "/my_test_folder_nova_tso500_s4_transfer_v2_50/Data/Intensities/BaseCalls/L002/s_2_2447.filter"
my_api_key = "Va39()RG6WISWYo4RD"
project_name = "ken_test"
foi = "s2_2_2447.filter"
foi_id  = download_and_upload_run_to_ICAv2.create_data(my_api_key,project_name, foi, "FILE", folder_id=folder_id,project_id=project_id,filepath=full_path)

print(foi_id)
# bssh2icav2
- Takes an input JSON from the [bssh_to_fastq_json tool](), renames files based on the contents of the metadata_table and transfers to your ICA project
- Performs a checksum check based on the file downloaded from BSSH and what is uploaded to ICAv2

# current image: **keng404/bssh2icav2:1.0.5**

# command line template
``` bash
python3 /data/download_and_upload_to_ICAv2.py  --metadata_table /data/test.metadata_table.csv --input_json /data/272296024.fastq.signedurl.json  --api_key_file <PATH_TO_API_KEY_FILE> --run_id <OUTPUT_FOLDER> --project_name <ICA_PROJECT_NAME>
```

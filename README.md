# bssh2icav2
- Takes an input JSON ( see example [here]() ) from the [bssh_to_fastq_json tool](https://github.com/keng404/bssh2icav2/blob/master/bssh_to_fastq_json.md), renames files based on the contents of the metadata_table ( see example [here](https://github.com/keng404/bssh2icav2/blob/master/test.metadata_table.csv) ) and transfers to your ICA project
- Performs a checksum check based on the file downloaded from BSSH and what is uploaded to ICAv2

# CWL for the [bssh2icav2 tool](https://github.com/keng404/bssh2icav2/blob/master/bssh2icav2.cwl)
# figures for setting up a pipeline that uses this tool can be found [here](https://github.com/keng404/bssh2icav2/blob/master/bssh2icav2.pipeline_diagram.png), [here](https://github.com/keng404/bssh2icav2/blob/master/bssh2icav2.tool_parameters.pt1.png), and [here](https://github.com/keng404/bssh2icav2/blob/master/bssh2icav2.tool_parameters.pt2.png)

**current image** keng404/bssh2icav2:1.0.6

# command line template
``` bash
python3 /data/download_and_upload_to_ICAv2.py  --metadata_table /data/test.metadata_table.csv --input_json /data/272296024.fastq.signedurl.json  --api_key_file <PATH_TO_API_KEY_FILE> --run_id <OUTPUT_FOLDER> --project_name <ICA_PROJECT_NAME>
```

# BSSH FASTQs to JSON
Migrates FASTQs from BSSH to IAP/BB project context (assuming running from that platform)

**Docker image for BSSH to FASTQ JSON tool:** shasson/bssh2bcljson: latest<br>

# CWL for this tool can be found [here](https://github.com/keng404/bssh2icav2/blob/master/bssh_to_fastq_json.cwl)

# Pipeline diagram to create a pipeline using both the bssh_to_fastq_json and bssh2icav2 tools can be found [here](https://github.com/keng404/bssh2icav2/blob/master/bssh2icav2.full_pipeline.multi_tool.pipeline_diagram.png)

# Configurations for the bssh_to_fastq_json tool can be found [here](https://github.com/keng404/bssh2icav2/blob/master/bssh_to_fastq_json.tool_parameters.pt1.png) and [here](https://github.com/keng404/bssh2icav2/blob/master/bssh_to_fastq_json.tool_parameters.pt2.png)

# Refer to [this document](https://github.com/keng404/bssh2icav2/blob/master/README.md) to see how to setup the bssh2icav2 tool.

**How to get a BSSH token?**
After authenticating using BS CLI (https://developer.basespace.illumina.com/docs/content/documentation/cli/cli-overview#Authenticate) you can get the access token from your config file using the following command
 ```less ~/.basespace/default.cfg | grep 'accessToken'```
 
 **How to get the project ID?**
The BSSH project ID could be retrieved using BSCLI or from the url in the browser. For example, in https://ilmn-cci.basespace.illumina.com/projects/225555331 the project ID is 225555331.

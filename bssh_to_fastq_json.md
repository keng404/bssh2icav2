# BSSH FASTQs to JSON
Migrates FASTQs from BSSH to IAP/BB project context (assuming running from that platform)

**Docker image for BSSH to FASTQ JSON tool:** shasson/bssh2bcljson: latest<br>

**How to get a BSSH token?**
After authenticating using BS CLI (https://developer.basespace.illumina.com/docs/content/documentation/cli/cli-overview#Authenticate) you can get the access token from your config file using the following command
 ```less ~/.basespace/default.cfg | grep 'accessToken'```
 
 **How to get the project ID?**
The BSSH project ID could be retrieved using BSCLI or from the url in the browser. For example, in https://ilmn-cci.basespace.illumina.com/projects/225555331 the project ID is 225555331.

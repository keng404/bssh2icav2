#!/usr/bin/env cwl-runner

# (Re)generated by BlueBee Platform

$namespaces:
  ilmn-tes: http://platform.illumina.com/rdf/iap/
cwlVersion: cwl:v1.0
class: CommandLineTool
requirements:
- class: ResourceRequirement
  https://platform.illumina.com/rdf/iap/resources:
    type: hicpu
    size: large
- class: InlineJavascriptRequirement
label: bssh_to_fastqjson
doc: Setting up the tool from https://git.illumina.com/CCI/bssh_to_gds
inputs:
  project_id:
    type: string
    label: The BSSH project ID with FASTQs
    inputBinding:
      prefix: --project_id
  access_token:
    type: string
    label: BSSH access token
    inputBinding:
      prefix: --access-token
  bssh_api_server:
    type: string
    label: BSSH API server to migrate from
    default: https://api.basespace.illumina.com/
    inputBinding:
      prefix: --api-server
outputs:
  bssh2fastqjson_output:
    type: File
    outputBinding:
      glob:
      - $(inputs.project_id).fastq.signedurl.json
baseCommand:
- python
- /git/bssh_to_gds/scripts/get_bsshproject_fastq_urls.py
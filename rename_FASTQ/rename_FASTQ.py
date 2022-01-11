import os
import re
# will parse a FASTQ filename and return sample name and read group in a dict
def parse_FASTQ(filename):
    FQ_PATTERN = re.compile(r'(.+)_S([0-9]+)_L([0-9]{3})_R([1-2])_([0-9]{3})$')
    re_match = FQ_PATTERN.match(os.path.basename(filename).split('.')[0])
    try:
        return (
            {"sample_name": re_match.group(1),
            "read_group": "R"+ re_match.group(4)}
        )
    except Exception:
        raise ValueError(f'Could not parse FASTQ file name {filename}')
# parse original filename and use the results of the parsing to craft new filename
def rename_FASTQ(filename):
    name_components = parse_FASTQ(filename)
    new_name = [name_components['sample_name'],name_components['read_group'],"fastq.gz"]
    return ".".join(new_name)

############ test example
x = parse_FASTQ("UHR-00001-VL00001-L1-DD_S7_L001_R1_001.fastq.gz")
y = rename_FASTQ("UHR-00001-VL00001-L1-DD_S7_L001_R1_001.fastq.gz")
#print(y)
#############

###### validate FASTQ renaming functions based on the below CSV file
linecount = 0
with open("bssh2ica_sample_metadata.csv", "r") as f:
    lines = f.readlines()
    for line in lines:
        linecount += 1
        # skip header
        if linecount > 1:
            line = line.strip("\n")
            linesplit = line.split(",")
            original_name = linesplit[0]
            new_name = linesplit[1]
            renamed_FASTQ = rename_FASTQ(original_name)
            if renamed_FASTQ == new_name:
                print(f"Renaming is all good for [ {original_name} ->  {renamed_FASTQ} ]")
            else:
                raise ValueError(f"ERROR:\tCould not properly rename {original_name}\t Tried {renamed_FASTQ}\tExpected {new_name}\n\n")
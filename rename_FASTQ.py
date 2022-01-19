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

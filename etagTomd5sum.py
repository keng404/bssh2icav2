import hashlib
import subprocess
import os

def md5_checksum(filename):
    m = hashlib.md5()
    with open(filename, 'rb') as f:
        for data in iter(lambda: f.read(1024 * 1024), b''):
            m.update(data)
   
    return m.hexdigest()

def etag_checksum(filename, chunk_size=8 * 1024 * 1024):
    md5s = []
    if os.path.getsize(filename) < chunk_size:
        with open(filename, 'rb') as f:
            md5s.append(hashlib.md5(f.read()))
    else:
        with open(filename, 'rb') as f:
            for data in iter(lambda: f.read(chunk_size), b''):
                md5s.append(hashlib.md5(data).digest())
    m = hashlib.md5(b"".join(md5s))
    print('{}-{}'.format(m.hexdigest(), len(md5s)))
    return '{}-{}'.format(m.hexdigest(), len(md5s))


def etag_compare(filename, etag):
    et = etag
    print('et', et)
    if '-' in et and et == etag_checksum(filename):
        return True
    if '-' not in et and et == md5_checksum(filename):
        return True
    return False


def confirm_md5sum(filename,bucket_name,your_key):
    aws_command = f"aws s3api head-object --bucket {bucket_name} --key {your_key} --query ETag --output text"
    lookup = subprocess.run(aws_command, shell=True, stdout=subprocess.PIPE)
    subprocess_return = lookup.stdout.decode('utf-8').strip('\n').strip("\"")
    etag = subprocess_return
    print('etag', etag)

    validation = etag_compare(filename, etag)
    print(validation)
    etag_checksum(filename, chunk_size=8 * 1024 * 1024)
    return validation


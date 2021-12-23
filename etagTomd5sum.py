import boto
import hashlib
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
    et = etag[1:-1]  # strip quotes
    print('et', et)
    if '-' in et and et == etag_checksum(filename):
        return True
    if '-' not in et and et == md5_checksum(filename):
        return True
    return False


def confirm_md5sum(s3_accesskey,s3_secret,filename,bucket_name,your_key):
    session = boto3.Session(
        aws_access_key_id=s3_accesskey,
        aws_secret_access_key=s3_secret
    )
    s3 = session.client('s3')
    obj_dict = s3.get_object(Bucket=bucket_name, Key=your_key)

    etag = (obj_dict['ETag'])
    print('etag', etag)

    validation = etag_compare(filename, etag)
    print(validation)
    etag_checksum(filename, chunk_size=8 * 1024 * 1024)
    return validation
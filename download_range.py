import pandas as pd, math, boto3, sys, os, subprocess, pdb, time, requests, zipfile, codecs, urllib, unicodedata
from subprocess import Popen
s3 = boto3.resource('s3')

def archive_directory(path, Key, bucket):
    zipf = zipfile.ZipFile('archive.zip', 'w')
    for root, dirs, files in os.walk(path):
        for file in files:
            f_path = os.path.join(root, file)
            zipf.write(f_path)
            os.remove(f_path)
    zipf.close()
    with open('archive.zip', 'rb') as f:
        bucket.put_object(Key=Key, Body=f)
    os.remove('archive.zip')

def strip_utf8(string):
    return unicodedata.normalize('NFKD', string).encode('ascii','ignore')
    
if __name__ == "__main__":
    _, files_key = sys.argv

    try:
        s3.meta.client.head_bucket(Bucket=files_key)
    except Exception as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
#        error_code = int(e.response['Error']['Code'])
 #       if error_code == 404:
        s3.create_bucket(Bucket=files_key)
        #else:
         #   raise Exception('Other error')

    bucket = s3.Bucket(files_key)
    downloaded_files = [int(i.key) for i in bucket.objects.page_size(1000000)]

    if len(downloaded_files) == 0:
        max_so_far = 0
    else:
        max_so_far = max(downloaded_files)

    to_download_path = files_key + ".h5"
    if not os.path.exists(to_download_path):
        s3.Bucket('ghallfiles').download_file(files_key, to_download_path)

    store = pd.HDFStore(to_download_path)
    files = store['df']

    downloaded_dir = 'downloaded'
    for i in xrange(max_so_far, files.shape[0]):
        if i % 100 == 0 and i != 0:
            archive_directory(downloaded_dir, str(i), bucket)
        print i
        curr_url = files.iloc[i]
        end_of_url = curr_url.replace('/blob/', '/')[18:].replace('?',"%3F").replace('#', '%23')
        to_get_url = "https://raw.githubusercontent.com" + end_of_url
        r = requests.get(to_get_url)
        if r.status_code != 200:
            print "Problem!"
            print r.headers
            pdb.set_trace()

        file_name = strip_utf8(end_of_url).replace('/', '_||_')
        with codecs.open(os.path.join(downloaded_dir, file_name), 'w', encoding='utf-8') as f:
            f.write(r.text)
    

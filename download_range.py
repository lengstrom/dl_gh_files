import pandas as pd, math, boto3, sys, os, subprocess, pdb, time, requests, zipfile, codecs, urllib, unicodedata
from subprocess import Popen
s3 = boto3.resource('s3')

def archive_directory(path, Key, bucket):
    archive_path = 'archive.zip'
    zipf = zipfile.ZipFile(archive_path, 'w')
    print "archiving %s to %s" % (Key, archive_path)
    for root, dirs, files in os.walk(path):
        for file in files:
            f_path = os.path.join(root, file)
            zipf.write(f_path)
            os.remove(f_path)
    zipf.close()
    with open(archive_path, 'rb') as f:
        bucket.put_object(Key=Key, Body=f)
    os.remove(archive_path)

def complete_url(url):
    r = requests.get(url)
    if r.status_code != 200:
        code = r.status_code
        if code == 404:
            return None

        if code == 503:
            print "503"
            time.sleep(5)
            return complete_url(url)

        print "Problem!"
        print r.headers
        pdb.set_trace()
    else:
         return r   

def trim_gh_file(path):
    for i in range(4):
        path = path[path.index('/')+1:]
    return path

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
    n = 0
    [os.remove(os.path.join(downloaded_dir, i)) for i in os.listdir(downloaded_dir)]
    for i in xrange(max_so_far, files.shape[0]):
        if n % 100 == 0 and n != 0:
            archive_directory(downloaded_dir, str(i-1), bucket)
        curr_url = files.iloc[i]
        end_of_url = curr_url.replace('/blob/', '/')[18:].replace('?',"%3F").replace('#', '%23')
        to_get_url = "https://raw.githubusercontent.com" + end_of_url
        file_name = trim_gh_file(end_of_url).replace('/', '_||_')[-255:]
        path_to_dl = os.path.join(downloaded_dir, file_name)
        print i, file_name
        if not os.path.exists(path_to_dl):
            r = complete_url(to_get_url)
            if r == None:
                continue
          #  file_name = strip_utf8(end_of_url).replace('/', '_||_')
            with codecs.open(path_to_dl, 'w', encoding='utf-8') as f:
                f.write(r.text)
        n += 1

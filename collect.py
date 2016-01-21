import pandas as pd, math, boto3, sys, os, subprocess, pdb, time, requests
from subprocess import Popen

s3 = boto3.resource('s3')
def get_buckets():
    return filter(lambda x :'authors' in x.name, list(s3.buckets.all()))

if __name__ == "__main__":
    buckets = get_buckets()
    archive_dir = './archive'
    if False:
        if not os.path.exists(archive_dir):
            os.mkdir(archive_dir)
        for bucket in buckets:
            archive_bucket = os.path.join(archive_dir, bucket.name)
            if not os.path.exists(archive_bucket):
                os.mkdir(archive_bucket)
            for obj in bucket.objects.all():
                obj_file = os.path.join(archive_bucket, obj.key)
                if not os.path.exists(obj_file):
                    bucket.download_file(obj.key, obj_file)

    dfs = []
    for i in os.listdir(archive_dir):
        cat = os.path.join(archive_dir, i)
        if os.path.isdir(cat):
            for csv in os.listdir(cat):
                dfs.append(pd.read_csv(os.path.join(cat, csv)))

    end = pd.concat(dfs)
    store = pd.HDFStore(os.path.join(archive_dir, 'all_files.h5'))
    store.put('df', end, format='table')
    store.close()

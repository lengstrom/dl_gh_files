import pandas as pd, math, boto3, sys, os, subprocess, pdb, time, requests
from subprocess import Popen

s3 = boto3.resource('s3')
def get_buckets():
    return filter(lambda x :'authors' in x.nname, list(s3.buckets.all()))

if __name__ == "__main__":
    buckets = get_buckets()
    archive_dir = './archive'
    if not os.path.exists(archive_dir):
        os.mkdir(archive_dir)
    for bucket in buckets:
        archive_bucket = os.path.join(archive_dir, bucket.name)
        if not os.path.exists(archive_bucket):
            os.mkdir(archive_bucket)
        for obj in bucket.objects.all():
            obj_file = os.path.join(archive_bucket, obj.key)
            if not os.path.exists(obj_file):
                s3.download_file(obj.bucket_name, obj.key, obj_file)

import pandas as pd, math, boto3, sys, os, subprocess, pdb, time, requests
from subprocess import Popen

s3 = boto3.resource('s3')
def get_buckets():
    return filter(lambda x :'authors' in x.name, list(s3.buckets.all()))

def get_df(path):
    store = pd.HDFStore(path, 'r')
    df = store['df']
    store.close()
    return df

if __name__ == "__main__":
    buckets = get_buckets()
    archive_dir = './archive'
    _, num_instances = sys.argv
    #tokens = get_df(tokens_path)
    
    in_fp = 'out/histories.h5'
    # if not os.path.exists(out_fp):
    #     if not os.path.exists(archive_dir):
    #         os.mkdir(archive_dir)
    #     for bucket in buckets:
    #         archive_bucket = os.path.join(archive_dir, bucket.name)
    #         if not os.path.exists(archive_bucket):
    #             os.mkdir(archive_bucket)
    #         for obj in bucket.objects.all():
    #             obj_file = os.path.join(archive_bucket, obj.key)
    #             if not os.path.exists(obj_file):
    #                 bucket.download_file(obj.key, obj_file)

    #     dfs = []
    #     for i in os.listdir(archive_dir):
    #         cat = os.path.join(archive_dir, i)
    #         if os.path.isdir(cat):
    #             for csv in os.listdir(cat):
    #                 dfs.append(pd.read_csv(os.path.join(cat, csv)))

    #     end = pd.concat(dfs)
    #     store = pd.HDFStore(out_fp)
    #     store.put('df', end, format='table')
    #     store.close()
    if os.path.exists(in_fp):
        os.remove(in_fp)

    store = pd.HDFStore(in_fp)
    file_list = store['df']['html']
    
    num_instances = int(num_instances)
    num_files = file_list.shape[0]
    num_per_subgroup = int(math.ceil(float(num_files)/num_instances))
b
    subgroups = [file_list.iloc[i * num_per_subgroup:(i+1) * num_per_subgroup] for i in range(num_instances)]

    out_dir = "./out"
    files_out = []
    bucket = s3.Bucket('ghallfiles')
    keys = []
    for n, v in enumerate(subgroups):
        try:
            key = "files_%s" % n
            file_to_make = os.path.join(out_dir, key)
            if os.path.exists(file_to_make):
                os.remove(file_to_make)
            store = pd.HDFStore(file_to_make)
            files_out.append(file_to_make)
            store['df'] = v
            store.close()
            keys.append(key)

            with open(file_to_make, 'rb') as f:
                bucket.put_object(Key=key, Body=f)

        except:
            pdb.set_trace()

    job_params = keys
    with open(os.path.join(os.path.expanduser('~'), '.aws/config'),'r') as f:
        config = f.read()
        config = config.replace('\n', '\\n')

    with open(os.path.join(os.path.expanduser('~'), '.aws/credentials'),'r') as f:
        credentials = f.read()
        credentials = credentials.replace('\n', '\\n')

    with open('./user-data.sh','r') as f:
        base = f.readlines()

    base.append("printf \"%s\" > ~/.aws/config \n" % config)
    base.append("printf \"%s\" > ~/.aws/credentials \n" % credentials)

    for i in job_params:
        key = i
        cp = base[:]
        cp.append('screen -d -m python download_range.py "%s"' % (key,))
        with open(os.path.join('amazon', key), 'w') as f:
            f.writelines(cp)

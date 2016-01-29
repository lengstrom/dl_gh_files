import pandas as pd, math, boto3, sys, os, subprocess, pdb, time, requests
from subprocess import Popen

all_dfs = []
for i in ["authors_" + str(j) for j in range(20)]:
    tmp_dfs = []
    path = '/Users/loganengstrom/s3/' + i
    for f in os.listdir(path):
        df = pd.read_csv(filepath_or_buffer=os.path.join(path, f), index_col=0)
        tmp_dfs.append(df)
    overall = pd.concat(tmp_dfs)
    all_dfs.append(overall)
    print ">>> Finished with %s" % path

all_shell = pd.concat(all_dfs)
all_shell_noforks = all_shell[all_shell['fork'] == False].reset_index()
fp = '/Users/loganengstrom/projects/download_all_ghfiles/data/all_shell_noforks.h5'
if os.path.exists(fp):
    os.remove(fp)

store = pd.HDFStore(fp)
store['df'] = all_shell_noforks
store.close()

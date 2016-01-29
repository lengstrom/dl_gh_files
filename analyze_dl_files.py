import pandas as pd, math, boto3, sys, os, subprocess, pdb, time, requests
from subprocess import Popen

fp = '/Users/loganengstrom/projects/download_all_ghfiles/data/all_shell_noforks.h5'
store = pd.HDFStore(fp)
df = store['df'].drop([u'index', u'api', u'fork'],axis=1)
store.close()

filters = ['bower', 'node_modules', 'bench.sh', 'Pods', 'mongo', 'cordova']

bash_histories = df[df['html'].str.lower().str.contains('bash_history')]
zsh_histories = df[df['html'].str.lower().str.contains('zsh_history')]
#histories = df[df['html'].str.lower().str.contains('_history')]

pdb.set_trace()

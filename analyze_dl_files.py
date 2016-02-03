import pandas as pd, math, boto3, sys, os, subprocess, pdb, time, requests
from subprocess import Popen

fp = '/Users/loganengstrom/projects/download_all_ghfiles/data/all_shell_noforks.h5'
store = pd.HDFStore(fp)
df = store['df'].drop([u'index', u'api', u'fork'],axis=1)
store.close()

bash_histories = df[df['html'].str.lower().str.contains('bash_history$')]
zsh_histories = df[df['html'].str.lower().str.contains('zsh_history$')]
fish_histories = df[df['html'].str.lower().str.contains('fish_history$')]
sh_histories = df[df['html'].str.lower().str.contains('/sh_history$')]
csh_histories = df[df['html'].str.lower().str.contains('/csh_history$')]
tcsh_histories = df[df['html'].str.lower().str.contains('/tcsh_history$')]

histories = pd.concat([bash_histories, zsh_histories, fish_histories, sh_histories, csh_histories, tcsh_histories])

print df.shape
store = pd.HDFStore('/Users/loganengstrom/projects/download_all_ghfiles/data/all_shell_nohistory.h5')
df = store['df']
store.close()
df = df[~df['html'].str.lower().str.contains('_history$')]
df = df[df['fork'] == False]

filters = ['bower', 'node_module', 'npm', 'bench.sh']
df = df[~df['html'].str.lower().str.contains('history')]
for i in filters:
    df = df[~df['html'].str.lower().str.contains(i)]

print df.shape
pdb.set_trace()

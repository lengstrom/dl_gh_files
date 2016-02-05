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
histories_list = [bash_histories, zsh_histories, fish_histories, sh_histories, csh_histories, tcsh_histories]

for i in histories_list:
    print i.shape
histories = pd.concat(histories_list)

# no history
no_history = '/Users/loganengstrom/projects/download_all_ghfiles/data/all_shell_nohistory.h5'
store = pd.HDFStore(no_history)
df = store['df']
store.close()
df = df[~df['html'].str.lower().str.contains('_history$')]
df = df[df['fork'] == False]
print df.shape

filters = ['bower', 'node_module', 'npm', 'bench.sh']
df = df[~df['html'].str.lower().str.contains('history')]
for i in filters:
    df = df[~df['html'].str.lower().str.contains(i)]

histories_p = 'out/histories.h5'
all_else = 'out/all_else.h5'

if os.path.exists(histories_p):
    os.remove(histories_p)

store = pd.HDFStore(histories_p)
store['df'] = histories
store.close()

if os.path.exists(all_else):
    os.remove(all_else)

store = pd.HDFStore(all_else)
store['df'] = df
store.close()

pdb.set_trace()

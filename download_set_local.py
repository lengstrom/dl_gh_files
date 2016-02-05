import pandas as pd, math, boto3, sys, os, subprocess, pdb, time, requests, zipfile, codecs, urllib, unicodedata, random
from subprocess import Popen

def complete_url(url, tries=2, incomp=True):
    r = requests.get(url)
    if r.status_code != 200:
        code = r.status_code
        if code == 404:
            print url
            url = trim_gh_file(url, nnn=6, mid='master/')
            if incomp:
                return complete_url(url, 3, incomp=False)
            return None

        print "Problem!"
        print "code: %s" % (code,)
        print r.headers

        
        time.sleep(5)
        tries -= 1
        if tries > -1:
            return complete_url(url, tries)
        print "Num tries exceeded!"
        pdb.set_trace()
        return None

    else:
        if incomp==False:
            print "GOTEEEM"
        return r   

def trim_gh_file(path,nnn=4,mid=''):
    n = 0
    ret = []
    orig = path
    for i in range(nnn):
        to_kill = path.index('/') + 1
        n += to_kill
        path = path[to_kill:]
        if i >= nnn-2:
            ret.append(n)

    return orig[:ret[0]] + mid + orig[ret[1]:]

def strip_utf8(string):
    return unicodedata.normalize('NFKD', string).encode('ascii','ignore')

# /Users/loganengstrom/projects/dot-files/dot.zsh_history
# remember to get this lmao

if __name__ == "__main__":
    _, files_path = sys.argv

    store = pd.HDFStore(files_path)
    files = store['df']
    if 'html' in files.columns:
        files = files['html']

    downloaded_dir = './downloaded'
#    [os.remove(os.path.join(downloaded_dir, i)) for i in os.listdir(downloaded_dir)]
    for i in xrange(0, files.shape[0]):
        curr_url = files.iloc[i]
        end_of_url = curr_url.replace('/blob/', '/')[18:].replace('?',"%3F").replace('#', '%23')
        to_get_url = "http://raw.githubusercontent.com" + end_of_url
        file_name = trim_gh_file(end_of_url).replace('/', '_||_')[-255:]
        path_to_dl = os.path.join(downloaded_dir, file_name)
        #print to_get_url

        #print i, file_name
        if os.path.exists(path_to_dl):
            continue
            path_to_dl = os.path.join(downloaded_dir, str(random.randint(0,99999)) + file_name[6:])
        else:
            print curr_url

        r = complete_url(to_get_url)
        print r
        if r == None:
            continue
      #  file_name = strip_utf8(end_of_url).replace('/', '_||_')
        print path_to_dl
        with codecs.open(path_to_dl, 'w', encoding='utf-8') as f:
            f.write(r.text)


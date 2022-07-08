import modin.pandas as pd
import urllib.request, urllib.parse
import time
import urllib3
from magniv.core import task
import urllib3
from datetime import datetime
import os
sotr_cred = {"key":os.getenv("AWS_ACCESS_KEY_ID"), "secret":os.getenv("AWS_SECRET_ACCESS_KEY")}
github_t= os.getenv("github_token")
# def get_diff_commit(id_repo):
#   try:
#     request_url = urllib.request.urlopen('https://github.com/'+id_repo,  timeout=1)
#     list_dif = str(request_url.read())
#     if list_dif == None:
#       request_url = urllib.request.urlopen('https://github.com/'+id_repo)
#       list_dif = str(request_url.read())
#     return list_dif
#   except:
#     pass



    
from github3 import login
g = login(github_t,github_t)

# request_url = urllib.request.urlopen('https://github.com/'+"chromium/chromium/commit/1df2093adb550a8468b92929d8ec79160ea4045a.diff")
# list_dif = str(request_url.read())

# list_dif


def get_diff_commit(id_repo):
  
  try:
    request_url = urllib.request.urlopen('https://github.com/'+id_repo,  timeout=100)
    list_dif = str(request_url.read())
    return list_dif
  except:
    try:
      list_dif = get_commit_by_lib(take_split(id_repo))
      return list_dif
    except:
      try:
        list_dif = get_data_url(id_repo)
        return list_dif
      except:
        pass
    





def get_data_url(url_repo):
  try:
    http = urllib3.PoolManager()
    r = http.request(url = 'https://github.com/'+url_repo, method = "get")
    return str(r.data)
  except:
    pass


def take_split(text):
  name_user_repo = text[0].split("/")
  commit_code = text[1].split(".")
  return [name_user_repo[0]] + [name_user_repo[1]] + [commit_code[0]]

def get_commit_by_lib(id_repo_name):

  repo = g.repository(id_repo_name[0], id_repo_name[1])
  rates = g.rate_limit()
  resd = repo.commit(id_repo_name[2])
  #return [resd.diff()] + [resd.as_dict()['commit']['message']]
  return str(resd.diff())
  
# def get_commit_by_lib(id_repo_name):

#   repo = g.repository(id_repo_name[0], id_repo_name[1])
#   rates = g.rate_limit()
#   result_diff = []
#   result_cm = []
#   resd = repo.commit(id_repo_name[2])
#   return [resd.diff()] + [resd.as_dict()['commit']['message']]
  


def return_list_repo(text):

  return [text + "/commit/"]

def return_list_commit(text):
  return [text + ".diff"]


def return_str(text):
  return "".join(text)



def run_main(df,data_name):
    
    df['commit_list'] = df['commit'].apply(return_list_commit)
    df['repo_list'] = df['repo'].apply(return_list_repo)

    df['merge_commit_repo'] = df.iloc[0:1000]['repo_list'] + df.iloc[0:1000]['commit_list']

    df['merge_commit_repo'] = df['merge_commit_repo'].apply(return_str)


    df['diff'] = df['merge_commit_repo'].apply(get_diff_commit)
    df = df.dropna()

    df.to_parquet("s3://gitdevml/dataset_finetuning/"+data_name+".parquet", compression='gzip', storage_options = sotr_cred)

    return df

@task(schedule="*/10 * * * *")
def verify_name():

    
    check_data = pd.read_parquet("s3://gitdevml/dataset_finetuning/clean_100k_dataset_kaggle.parquet",storage_options = sotr_cred)
    
    if check_data:
        data_name = "data_diff"+str(check_data.index[1])
        check_data = check_data[0:1000]
        new_df = run_main(check_data, data_name)

    else:
        pass
    
    check_data = check_data.drop(index=check_data.index[:1000],axis=0, inplace=True)
    check_data.to_parquet("s3://gitdevml/dataset_finetuning/clean_100k_dataset_kaggle.parquet",storage_options = sotr_cred)

    return ("done")
    




if __name__ == '__main__':
    verify_name()
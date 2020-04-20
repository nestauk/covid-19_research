import pandas as pd 
import requests
import ratelim
import time
import logging

import cord19
project_dir = cord19.project_dir

#TODO - implement this function elsewhere in the repo
@ratelim.patient(30,60)
def insistent_request(url,tries,time_off):
    '''
    Request data from a url with retries in case of failure

    Args:
        url (str) is the url we want to collect
        tries (int) is the number of times we want to retry the request
        time_off (int) is the number of seconds we wait after a failure 
    '''
    n = 0
    while n<=tries:
        try:
            req = requests.get(url).content
            return(req)
            break
        except:
            time.sleep(time_off)
            n+=1
            if n > tries:
                logging.info(f"failed with {url}")
                return np.nan
            
#Read repos df
repos = pd.read_csv(f"{project_dir}/data/raw/github/github_repos_first_pass.csv")

#Create a url to look for the README
readme_target = [
    f"https://raw.githubusercontent.com/{'/'.join(x.split('/')[-2:])}/master/README.md" 
    for x in repos['repo_url']]

#Collect all repos
readme_store = []

for n,x in enumerate(readme_target):
    
    if n%500 == 0:
        logging.info(f"Queried {n} repos")
    
    rm = insistent_request(x,5,2)
    readme_store.append(rm)

#Concatenate them into a df
all_rms = pd.DataFrame({'repo_url':readme_target,'readme_content':readme_store})

logging.info(all_rms.head())

#Save the df
all_rms.to_csv(f"{project_dir}/data/raw/github/repo_readmes.csv",index=False)

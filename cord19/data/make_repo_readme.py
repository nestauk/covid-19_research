import pandas as pd 
import requests
import ratelim
import time
import logging
import os

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

def _readme_scrape(repo_urls):

    readme_target = [
        f"https://raw.githubusercontent.com/{'/'.join(x.split('/')[-2:])}/master/README.md" 
        for x in repo_urls]

    #Collect all repos
    readme_store = []

    for n,x in enumerate(readme_target):
        if n%500 == 0:
            logging.info(f"Queried {n} repos")
                
        rm = insistent_request(x,5,2)
        readme_store.append(rm)

    #Concatenate them into a df
    all_rms = pd.DataFrame({'repo_url':repo_urls,'readme_content':readme_store})

    return(all_rms)

def make_readmes(recollect=False):
    '''
    Recollects the readme data.

    Args:
        recollect (boolean) if we want to start collections from scratch. Alternatively
        we check if we already collected the data and focus on new observations.
    '''
    #Read repos df
    repos = pd.read_csv(f"{project_dir}/data/raw/github/github_repos_first_pass.csv")

    if recollect == False:
        if os.path.exists(f"{project_dir}/data/raw/github/repo_readmes.csv")==True:
            rms = pd.read_csv(f"{project_dir}/data/raw/github/repo_readmes.csv")

            collected_urls = set(rms['repo_url'])

            #Get urls from repos we haven't collected already
            new_repo_urls = [x for x in repos['repo_url'] if x not in collected_urls]

            rms_2 = _readme_scrape(new_repo_urls)
            all_rms = pd.concat([rms,rms_2])

            logging.info(all_rms.head())

            #Save the df
            all_rms.to_csv(f"{project_dir}/data/raw/github/repo_readmes.csv",
                           index=False)
        else:
            repo_urls = repos['repo_url']
            rms = _readme_scrape(repo_urls)

            logging.info(rms.head())

            rms.to_csv(f"{project_dir}/data/raw/github/repo_readmes.csv",
                       index=False)
    else:
        repo_urls = repos['repo_url']
        rms = _readme_scrape(repo_urls)

        logging.info(rms.head())

        rms.to_csv(f"{project_dir}/data/raw/github/repo_readmes.csv",
                   index=False)

if __name__=='__main__':
    make_readmes()
import cord19
from cord19.utils.general import *
from cord19.data.github_collect_process import *

## Logging
import logging
import sys
logger = logging.getLogger()

#Utilities
from datetime import datetime
import re
import yaml

#Scientific stack
import numpy as np
import scipy as sp
import pandas as pd

#API work stack
import requests
import ratelim
import time

## Paths
# project directory e.g. `/home/user/GIT/nesta`
project_dir = cord19.project_dir
data_path = f'{project_dir}/data/raw/github'

import os
import dotenv

#Credentials
dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)

username = os.environ.get('username')
token = os.environ.get('token')
creds = (username,token)

#Load model parameters (covid terms we want to search)
with open(f"{project_dir}/model_config.yaml",'r') as infile:
    mod = yaml.safe_load(infile)
cov_terms = mod['github_queries']
logger.info(cov_terms)

def make_repos():
    '''
    Function to make the repos
    '''

    #Return all results
    cov_results = [get_search_results(x,creds) for x in cov_terms]

    #Create list of unique repos
    all_repos = flatten_list([x['results'] for x in cov_results])

    ids = []
    unique_results = []

    for item in all_repos:
        if item['id'] in ids:
            pass
        else:
            ids.append(item['id'])
            
            unique_results.append(item)

    logger.info(f"{len(unique_results)}")

    #Create a lookuup between ids and sources
    cov_source_ids = [set([x['id'] for x in results['results']]) for results in cov_results]
    all_ids = [x['id'] for x in unique_results]
    id_source_lookup = {rid:[s for s,ids in zip(cov_terms,cov_source_ids) if rid in ids] for rid in all_ids}

    ### Create repo df including contributor ids (we will use later)
    #Do the first pass
    first_pass_results = parse_all_repos(unique_results,creds,
                                         id_source_lookup,
                                         'github_repos_first_pass',
                                         data_path)

    first_pass_results[0].to_csv(f'{data_path}/github_repos_first_pass.csv',index=False)

if __name__=='__main__':
    make_repos()
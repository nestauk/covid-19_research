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
from ast import literal_eval

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

def make_contributors(recollect=True):
    '''
    Creates the contributor df.

    Args:
        recollect (boolean): if false, it checks if we already performed a user 
        collection and only collects data for the new users.
    '''

    #Read the GitHub repos that we collected before
    repos = pd.read_csv(f"{data_path}/github_repos_first_pass.csv",dtype={
        'id':str},error_bad_lines=False)


    #Parse the contributor individual urls into lists
    repos['contributor_individual_urls'] = [literal_eval(x) if pd.isnull(x)==False else np.nan for x in 
                                         repos['contributor_individual_urls']]

    #Drop projects with no contributors
    repos.dropna(axis=0,subset=['contributor_individual_urls'],inplace=True)

    #Combine owners and contributors in a single list (sometimes they aren't the same)
    repos['users_involved'] = [x+[y] if y not in x else x for x,y in zip(repos['contributor_individual_urls'],
                                                               repos['owner_url'])]

    #Create a repo - user lookup (which we will save)
    project_user_lookup = pd.concat([
        pd.DataFrame({'id':row['id'],
                      'user':pd.Series(
                          row['users_involved'])}) for rid,row in repos.iterrows()]).reset_index(drop=True)

    #Add a owner flag if needed.
    project_owner_map = repos.set_index('id')['owner_url'].to_dict()

    project_user_lookup['is_owner'] = [True if 
                                       project_owner_map[row['id']]==row['user'] else False for rid,row in 
                                      project_user_lookup.iterrows()]

    project_user_lookup.to_csv(f"{data_path}/github_repo_user_lookup.csv",index=False)

    #Uses the previously defined function
    user_df = create_user_df(project_user_lookup,data_path,creds,recollect)
        
    user_df.to_csv(f"{data_path}/github_users.csv",index=False)


if __name__ == '__main__':
    make_contributors()
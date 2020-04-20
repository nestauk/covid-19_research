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
data_path = f'{project_dir}/data/github'

import os
import dotenv

#Credentials
dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)

username = os.environ.get('username')
token = os.environ.get('token')
creds = (username,token)

#Read the GitHub repos that we collected before
repos = pd.read_csv(f"{data_path}/raw/github_repos_first_pass.csv",dtype={
    'id':str},error_bad_lines=False)

#Parse the contributor individual urls into lists
repos['contributor_individual_urls'] = [literal_eval(x) if pd.isnull(x)==False else np.nan for x in 
                                     repos['contributor_individual_urls']]

#Create a repo - user lookup (which we will save)
project_user_lookup = pd.concat([
    pd.DataFrame({'id':row['id'],
                  'users':pd.Series(
                      row['contributor_individual_urls'])}) for rid,row in repos.iterrows()]).reset_index(drop=True)
project_user_lookup.to_csv(f"{data_path}/github_repo_user_lookup.csv",index=False)

#Get unique users
unique_users = [x for x in list(set(project_user_lookup['users'])) if pd.isnull(x)==False]
logger.info(f"{unique_users}")

#Collect the data
user_results = collect_user_data(unique_users,creds)

#Parse the data and create a dataframe
user_df = pd.DataFrame([parse_user_result(x) for x in user_results])

#Save the df
user_df.to_csv(f"{data_path}/github_users.csv",index=False)


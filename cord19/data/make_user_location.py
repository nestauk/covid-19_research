import pandas as pd
import numpy as np
import re
import pandas_gbq
from google.oauth2 import service_account
import requests
import logging

import cord19
project_dir = cord19.project_dir

def chunks(lst):
    return [(i, j) for i, j in zip(lst, lst[1:])]

def subset_users(user_list,values):
    '''
    This is to create a str with user ids that we can include (via a template)
    in the SQL query
    '''
    subs_users = user_list[values[0]:values[1]]
    
    subs_users_as_string = re.sub('\[|\]','',str(subs_users))
    
    return subs_users_as_string

def _query_gbq(user_login_list,_project_id='github-covid-analysis'):

  sql = f"""
      SELECT
        id,
        login,
        company,
        created_at,
        country_code,
        city,
        long,
        lat
      FROM 
        `ghtorrentmysql1906.MySQL1906.users`
      WHERE 
        login IN
      """

      #We will chunk our queries into 1000s
  user_steps = list(np.arange(0,len(user_login_list),1000))
  user_queries = chunks(user_steps)
  user_steps.append(len(user_login_listn))

      #Run the query
  df = pd.concat([pandas_gbq.read_gbq(sql+f" ({subset_users(user_login_list,q)})", 
                  project_id=_project_id,
                  credentials=creds) for q in user_queries]).reset_index(drop=True)
  return df

#Credentials
creds = service_account.Credentials.from_service_account_file(
    f"{project_dir}/big_query_creds.json")

#project_id = '102376819866753956231'

def make_locations(recollect=False):
  '''
  Collects user locations from Google big query

  '''
  if recollect== False:
    if os.path.exists(f"{project_dir}/data/raw/github/user_locations.csv")==True:

      locs = pd.read_csv(f"{project_dir}/data/raw/github/user_locations.csv")

      #Read users
      users = pd.read_csv(f"{project_dir}/data/raw/github/github_users.csv")

      #Extract user logins
      new_user_login = [x for x in locs['login'] if x not in set(users['login'])]

      df = _query_gbq(new_user_login)

      logging.info(df.head())

      #Concatenate with the previous results
      df_2 = pd.concat([locs,df])
      df_2.to_csv(f"{project_dir}/data/raw/github/user_locations.csv",index=False)
    
    else:
      users = pd.read_csv(f"{project_dir}/data/raw/github/github_users.csv")

      #Extract user logins
      user_login = users['login']

      df = _query_gbq(user_login)
      logging.info(df.head())

      #Save the results
      df.to_csv(f"{project_dir}/data/raw/github/user_locations.csv",index=False)
  else:
    users = pd.read_csv(f"{project_dir}/data/raw/github/github_users.csv")

    #Extract user logins
    user_login = users['login']

    df = _query_gbq(user_login)
    logging.info(df.head())

    #Save the results
    df.to_csv(f"{project_dir}/data/raw/github/user_locations.csv",index=False)


if __name__ == '__main__':
    make_locations()
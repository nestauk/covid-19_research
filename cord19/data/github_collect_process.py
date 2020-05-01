import requests
from datetime import datetime
import ratelim
import re
import time
import os

import pandas as pd
import numpy as np
import ratelim

import cord19
from cord19.utils.general import *

import logging
import sys
logger = logging.getLogger()## Logging

## Paths
# project directory e.g. `/home/user/GIT/nesta`
project_dir = cord19.project_dir
data_path = f'{project_dir}/data'


def _parse_api_content(github_request):
    if type(github_request.json())==list:
        return github_request.json()    
    elif 'items' in github_request.json().keys():
        return github_request.json()['items']
    else:
        return github_request.json()
        
def strip_gh_time(time_string):
    '''
    Strips a github timestamp
    
    '''
    return(datetime.strptime(time_string.split('T')[0],"%Y-%m-%d"))

#Functions
@ratelim.patient(30,60)
def query_github_api(url,my_auth,search=False,verbose=False):
    '''
    Query the GitHub API to get results taking into account pagination
    
    Args:
        url (str) the link we want to use in the query. Can be an API url or query string
        my_auth (tuple) credentials
        search (Boolean) if we are searching for a query string
        verbose (Boolean) if we want to print results

    '''
    if verbose==True:
        logger.info(f'Collecting data for {url}')
    
    if search==True:
        url = f"https://api.github.com/search/repositories?q={re.sub(' ','&',url)}&per_page=100"
    
    #First query
    result = requests.get(url,auth=my_auth)
    
    #If there are multiple pages, loop over them
    if len(result.links)>0:        

        ##TODO make this a function
        #This is the template for all queries (removing the page)
        template= '='.join(result.links['next']['url'].split('=')[:-1])
        
        #Page values
        vals = [int(x['url'].split('=')[-1]) for x in result.links.values()]
        page_range = np.arange(vals[0],vals[1]+1)
        
        result_container = [_parse_api_content(result)]
        
        #Loop over pages to get results
        for n in page_range:
            
            if verbose==True:
                logger.info(template+str(n))
            more_result = requests.get(template+'='+str(n),auth=my_auth)
            
            result_container.append(_parse_api_content(more_result))
        
        return(flatten_list(result_container))
    else:
        return(_parse_api_content(result))

def get_search_results(query,my_auth):
    '''
    Extracts search results and total number of results for a query.
    
    Args:
        query (str) is the term we are querying
        my_auth (tuple) are the credentials

    '''
    #Get total results
    total_results = requests.get(
        f"https://api.github.com/search/repositories?q={re.sub(' ','&',query)}",auth=my_auth).json()['total_count']
    
    #Get all results using query_github_api
    results = query_github_api(query,my_auth,search=True,verbose=True)
    
    return({'total':total_results,'results':results})


def parse_repo(repo_json,my_auth,id_source_lookup,contrib_scrape=False):
    '''
    Parses a repo json

    Args:
        repo_json (dict) is a dict with values for a repo
        my_auth (tuple) are the credentials
        contrib_scrape (Boolean) if we want to scrape the contributor list   
    '''
    result = {}
    
    result['id'] = repo_json['id']
    result['name'] = repo_json['name']
    result['owner_url'] = repo_json['owner']['url']
    result['description'] = repo_json['description']
    result['is_fork'] = repo_json['fork']
    result['repo_url_html'] = repo_json['html_url']
    result['repo_url'] = repo_json['url']
    result['contributors_url'] = repo_json['contributors_url']
    
    #We use the function defined above to parse github time strings
    result['created'] =  strip_gh_time(repo_json['created_at'])
    result['updated'] = strip_gh_time(repo_json['updated_at'])
    result['pushed'] = strip_gh_time(repo_json['pushed_at'])
    
    result['language'] = repo_json['language']
    result['homepage'] = repo_json['homepage']
    result['size'] = repo_json['size']
    result['stargazers_count'] = repo_json['stargazers_count']
    result['forks_count'] = repo_json['forks_count']
    result['sources'] = id_source_lookup[repo_json['id']]
    
    if contrib_scrape==False:    
        return pd.Series(result)
    else:
        try:
            contrs = query_github_api(repo_json['contributors_url'],my_auth)
            result['contributor_individual_urls'] =  [x['url'] for x in contrs if x['type']!='Bot']
            result['countributors_count'] = len(result['contributor_individual_urls'])
            return pd.Series(result)

        except:
            result['contributor_individual_urls'] = np.nan
            result['countributors_count'] = np.nan
            return pd.Series(result)

def parse_all_repos(repo_list,my_auth,id_source_lookup,save_name,save_path=data_path,my_sleep=5):
    '''
    This function parses a list of repos (including queries of contributors) and returns/saves a df with results
    
    Args:
        repo_list (list) is a list of dicts we want to parse
        save_path (str) is the path where we save the results
        save_name (str) is the name of the df where we save results
        sleep (int) is the time to sleep when there is a failure
    '''
    repo_results = []
    repo_failed = []

    repo_results.append(parse_repo(repo_list[0],my_auth,id_source_lookup,contrib_scrape=True))

    #Here we are parsing each repo. 
    #This includes querying the contributor pages.
    #TODO: This should be a function - probably applicable to users as well as repos

    for n,j in enumerate(repo_list[1:]):
        
        if n%100 ==0:
            logger.info(str(n))
            pd.DataFrame(repo_results).to_csv(f'{save_path}/{save_name}.csv',index=False)
            pd.DataFrame([x['id'] for x in repo_failed]).to_csv(
                                                                f'{save_path}/{save_name}_failed.csv',index=False)
        
        if j['id'] in set(pd.DataFrame(repo_results)['id']):
            pass
        
        else:
            try:
                res = parse_repo(j,my_auth,id_source_lookup,contrib_scrape=True)
                repo_results.append(res)
            except:
                #If the request fails we store the repo id. We can query this again later.
                logger.info(f'failed with repo {j["id"]}')
                repo_failed.append(j)
                time.sleep(my_sleep)

    # Save the results
    final_df = pd.DataFrame(repo_results)

    final_df.to_csv(f'{save_path}/{save_name}.csv',index=False)
    pd.DataFrame([x['id'] for x in repo_failed]).to_csv(f'{save_path}/{save_name}_failed.csv',index=False)

    return([final_df,repo_failed])

def collect_user_data(user_urls,my_auth,sleep=5):
    '''
    Collect GitHub user data
    
    Args:
        user_urls (list) is a list of user URLs from GitHub API
        my_auth (tuple) are my credentials
        sleep (int) is the length of sleep if the query fails
    
    '''
    user_results = []
    user_failed = []
    
   #Run the query for each user
    for n,u in enumerate(user_urls):
        if n%500==0:
            logger.info(f"Collected data for {n} users")
        #TODO turn this kind of thing into a function with a number of tries parameter
        try:
            rest = query_github_api(u,my_auth)
            user_results.append(rest)
        except:
            time.sleep(5)
            #Try again
            try:
                rest = query_github_api(u)
            except:
                logger.info(f"failed with user {u}")
                user_failed.append(u)
                
    return([user_results,user_failed])

def parse_user_result(user_dict):
    '''
    Parses a GitHub user repo

    Args:
        user_dict (dict) are the results for a user in the GitHub API
    
    '''
    result = {}
    
    result['login'] = user_dict['login']
    result['name'] = user_dict['name']
    result['company'] = user_dict['company']
    result['location'] = user_dict['location']
    result['bio'] = user_dict['bio']
    result['created'] = strip_gh_time(user_dict['created_at'])
    result['repos_url'] = user_dict['repos_url']
    result['organizations_url'] = user_dict['organizations_url']
    result['followers'] = user_dict['followers']
    result['public_repos'] = user_dict['public_repos']
    
    return pd.Series(result)

def create_user_df(user_lookup_df,data_path,recollect=False):
    '''
    Takes a repo - user lookup and collects user data.

    Args:
        user_lookup_df (pandas dataframe) is a dataframe with repos and users
        recollect (boolean) if we want to check if we already collected the data
        data_path (str) the directory with the data
    '''
    if recollect==False:
        if os.path.exists(f"{data_path}/github_users.csv")==True:
            #Get unique users
            unique_users = [x for x in list(set(user_lookup_df['user'])) if pd.isnull(x)==False]
            
            #Get new users
            #Previously collected data
            user_df = pd.read_csv(f"{data_path}/github_users.csv")
            
            existing_users = set(user_df['login'])
            new_users = [x for x in unique_users if x not in existing_users]
            logger.info(f"{len(new_users)}")

            #Collect the data
            new_user_results = collect_user_data(new_users,creds)

            #Parse the data and create a dataframe
            new_user_df = pd.DataFrame([parse_user_result(x) for x in new_user_results ])

            #Combine with previous users and save
            user_df_2 = pd.concat([user_df,new_user_df])

            return(user_df_2)
        else:
            unique_users = [x for x in list(set(user_lookup_df['user'])) if pd.isnull(x)==False]
            logger.info(f"{len(unique_users)}")

            #Collect the data
            user_results = collect_user_data(unique_users,creds)

            #Parse the data and create a dataframe
            user_df = pd.DataFrame([parse_user_result(x) for x in user_results])

            #Save the df
            return(user_df)
    else:
        unique_users = [x for x in list(set(user_lookup_df['users'])) if pd.isnull(x)==False]
        logger.info(f"{len(unique_users)}")

        #Collect the data
        user_results = collect_user_data(unique_users,creds)

        #Parse the data and create a dataframe
        user_df = pd.DataFrame([parse_user_result(x) for x in user_results])

        #Save the df
        return(user_df)

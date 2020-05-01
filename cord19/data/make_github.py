from cord19.data.make_repo_data import make_repos 
from cord19.data.make_contributor_data import make_contributors
from cord19.data.make_user_location import make_locations
from cord19.data.make_repo_readme import make_readmes

import logging

def make_github_data(recollect):
    logging.info('MAKING REPOS')
    make_repos() 

    logging.info('MAKING CONTRIBUTORS')
    make_contributors() 

    logging.info('MAKING LOCATIONS')
    make_locations(recollect)

    logging.info('MAKING READMES')
    make_readmes(recollect)

if __name__=='__main__':
    make_github_data(recollect=False)


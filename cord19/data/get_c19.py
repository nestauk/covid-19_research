import pandas as pd
import requests
import zipfile
import cord19
import os
import yaml

project_dir = cord19.project_dir
raw_dir = os.path.join(project_dir,'data/raw')

def get_c19_file(url):
    '''
    collects a c19 file

    Args:
        url (str) is the link for a gzipped dataset

    '''
    #We keep the file name to save it later
    name = url.split('/')[-1]

    print(f'Downloading {url}')
    f = requests.get(url)


    print(f'Saving {url}')
    with open(f'{raw_dir}/{name}','wb') as outfile:

        outfile.write(f.content)

#Get the data
with open(f'{project_dir}/model_config.yaml','r') as infile:
    config = yaml.load(infile)

files = config['cord19_files']

for f in files:
    get_c19_file(f)





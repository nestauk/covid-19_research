from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from configparser import ConfigParser
import cord19


def get_engine(config_path, database="production", **engine_kwargs):
    '''Get a SQL alchemy engine from config'''
    cp = ConfigParser()
    cp.read(config_path)
    cp = cp["client"]
    url = URL(drivername="mysql+pymysql", database=database,
              username=cp["user"], host=cp["host"], password=cp["password"])
    return create_engine(url, **engine_kwargs)


def contains_keyword(text):
    keywords = cord19.config['utils']['keywords']  # see model_config.yaml
    if text is None:
        return False
    return any(term.lower() in text.lower() for term in keywords)   

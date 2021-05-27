import numpy as np
from statsmodels.stats.proportion import proportions_ztest
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from configparser import ConfigParser


def get_engine(config_path, database="production", **engine_kwargs):
    """Get a SQL alchemy engine from config"""
    cp = ConfigParser()
    cp.read(config_path)
    cp = cp["client"]
    url = URL(
        drivername="mysql+pymysql",
        database=database,
        username=cp["user"],
        host=cp["host"],
        password=cp["password"],
    )
    return create_engine(url, **engine_kwargs)


def flatten_lists(l):
    """Flattens nested lists."""
    return [item for sublist in l for item in sublist]


def two_proportions_test(df, value=0, alternative="smaller"):
    """Tests for proportions based on normal (z) test.
    
    Args:
        df (:obj:`pandas.DataFrame`): Dataframe used in the analysis.
        value (int): See https://www.statsmodels.org/stable/generated/statsmodels.stats.proportion.proportions_ztest.html
        alternative (str): See https://www.statsmodels.org/stable/generated/statsmodels.stats.proportion.proportions_ztest.html
    Return:
        (:obj:`tuple` of `float`): z-test and p-value.

    """
    successes = np.array(df[1])
    observations = np.array(df.sum(axis=1))
    return proportions_ztest(successes, observations, value, alternative=alternative)

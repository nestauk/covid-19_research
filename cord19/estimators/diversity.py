import numpy as np
from sklearn.metrics.pairwise import cosine_distances


def _calc_centroid(vectors):
    """Averages a list of vectors.
    
    Args:
        vectors (:obj:`list` of `numpy.array`)
    
    Returns:
        (:obj:`numpy.array` of float)
    
    """
    return np.mean(vectors, axis=0)


def distance(df):
    """
    Measure the distance for two sets of vectors; with and without COVID-19 publications.

    Args:
        df (`pandas.DataFrame`)

    Returns:
        d (dict): Dictionary with keys ('with_covid', 'no_covid') and values the distance for 
            each set.

    """
    d = {}
    l = []
    for (flag, val) in zip(["no_covid", "with_covid"], [0,1]):
        l.append(val)
        vectors = [v for v in df[df.is_Covid.isin(l)]["arr"].values]

        centroid = _calc_centroid(vectors)

        distances = sum(
            [
                cosine_distances(vector.reshape(1, -1), centroid.reshape(1, -1))
                for vector in vectors
            ]
        )

        d[flag] = distances / len(vectors)
    return d

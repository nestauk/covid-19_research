from sklearn.decomposition import TruncatedSVD
import umap


def svd(X, n_components):
    """Reduces the dimensionality of a sparse matrix.

    Args:
        X (`scipy.sparse`): Sparse TFIDF matrix.
        n_components (int): Dimensionality of the latent space.
    
    Returns:
        X ()

    """
    trunc_svd = TruncatedSVD(n_components=n_components, random_state=42)
    X = trunc_svd.fit_transform(X)
    return X


def umap_embeddings(X, n_neighbors, min_dist, n_components, metric):
    """Reduces the dimensionality with UMAP.

    Args:
        X ():
        n_neighbors (int): Number of neighbours used to measure UMAP.
        min_dist (float): Minimum acceptable distance between points.
        n_components (int): Dimensions of the reduced latent space.
        metric (str): Metric to calculate distance.

    Returns:
        ()
    
    """
    reducer = umap.UMAP(
        n_neighbors=n_neighbors,
        min_dist=min_dist,
        n_components=n_components,
        metric=metric,
        random_state=42,
    )

    return reducer.fit_transform(X)

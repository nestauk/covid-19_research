import logging
import pandas as pd
import cord19
import numpy as np
import ast
from cord19.transformers.nlp import tfidf_vectors
from cord19.transformers.dim_reduction import svd, umap_embeddings
from cord19.visualisation.plot import scatter_3d
from cord19.estimators.diversity import distance


def main():
    # Read papers
    papers = pd.read_csv(
        f"{cord19.project_dir}/data/interim/papers.csv", dtype={"id": "str"}
    )
    papers.mag_authors = papers.mag_authors.apply(
        lambda x: ast.literal_eval(x) if isinstance(x, str) else np.nan
    )

    # Use only arXiv and AI
    ai_papers_arxiv = papers[(papers.article_source == "arxiv") & (papers.is_AI == 1)]
    logging.info(f"Number of AI papers in arXiv: {ai_papers_arxiv.shape[0]}")

    # Create a paper IDs | author IDs table
    author_ids = []
    author_names = []
    paper_ids = []
    for _, row in ai_papers_arxiv.iterrows():
        if isinstance(row["mag_authors"], list):
            for author in row["mag_authors"]:
                paper_ids.append(row["id"])
                author_ids.append(author["author_id"])
                author_names.append(author["author_name"])

    mag_paper_authors = pd.DataFrame(
        {"id": paper_ids, "author_id": author_ids, "author_name": author_names}
    )

    # Create TFIDF vectors for the AI papers and reduce dimensionality with SVD
    # tfidf vectors
    X = tfidf_vectors(ai_papers_arxiv.abstract, cord19.config["tfidf"]["max_features"])

    # Dim reductions with SVD
    X = svd(X, cord19.config["svd"]["n_components"])

    # Dim reduction with UMAP
    umap_config = cord19.config["umap"]
    embed = umap_embeddings(X, **umap_config)

    # Visualise SVD vectors
    scatter_3d(embed, ai_papers_arxiv)

    # Find the authors with covid-19 publications
    # Reset index to fetch the TFIDF vector by it
    ai_papers_arxiv = ai_papers_arxiv.reset_index()

    # Add a covid-19 flag
    mag_paper_authors = mag_paper_authors.merge(
        ai_papers_arxiv[["id", "is_Covid"]], left_on="id", right_on="id"
    )

    # Author IDs with covid-19 publications
    author_ids_with_covid_pub = mag_paper_authors[mag_paper_authors.is_Covid == 1][
        "author_id"
    ].values

    # Group paper IDs by author IDs
    g = (
        mag_paper_authors[mag_paper_authors.author_id.isin(author_ids_with_covid_pub)]
        .groupby("author_id")["id"]
        .apply(list)
    )

    # Keep only authors with more than 3 publications
    d = {idx: len(item) for idx, item in g.iteritems()}
    ids = [k for k, v in d.items() if v > 2]

    # Subset mag_paper_authors by the ids
    authors_covid_contrib = mag_paper_authors[mag_paper_authors.author_id.isin(ids)]

    # Paper IDs and arrays - only for authors working in AI and have covid-19 contributions
    ids = []
    arr = []
    for idx, row in ai_papers_arxiv[
        ai_papers_arxiv.id.isin(authors_covid_contrib.id.unique())
    ].iterrows():
        ids.append(row["id"])
        arr.append(X[idx])

    arrays = pd.DataFrame({"id": ids, "arr": arr})

    authors_covid_contrib = authors_covid_contrib.merge(
        arrays, left_on="id", right_on="id"
    )

    logging.info(
        f"Unique authors with more than 3 papers and at least one covid-19 contribution: {authors_covid_contrib.author_id.unique().shape[0]}"
    )

    # Measure author-level diversity with and without covid-19 publications
    author_div = {}
    for id_ in authors_covid_contrib.author_id.unique():
        frame = authors_covid_contrib[authors_covid_contrib.author_id == id_]
        try:
            author_div[id_] = distance(frame)
        except ZeroDivisionError as e:
            continue

    # Author-level diversity deltas
    author_div_diff = {}
    for k, v in author_div.items():
        author_div_diff[k] = (v["with_covid"] - v["no_covid"])[0][0]

    # Read as dataframe and rename column
    author_div_diff = pd.DataFrame.from_dict(author_div_diff, orient="index")
    author_div_diff = author_div_diff.rename(index=str, columns={0: "delta"})
    author_div_diff = author_div_diff.reset_index()

    author_div_diff.to_csv(
        f"{cord19.project_dir}/data/processed/author_diversity_delta.csv", index=False
    )


if __name__ == "__main__":
    main()

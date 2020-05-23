"""
Connects to database and collects all of the *rxiv papers. Does some basic processing and 
uses a keyword search approach (the same one is used by arXiv) to identify covid-19 papers.
Stores the table in data/interim.

arXiv's keyword search: https://blogs.cornell.edu/arxiv/2020/03/30/new-covid-19-quick-search/

"""
import logging
import pandas as pd
import cord19
from cord19.utils.utils import get_engine


def main():
    # Connect to db
    con = get_engine(f"{cord19.project_dir}/innovation-mapping-5712.config")
    # Read papers in chunks
    columns = cord19.config["rxiv_columns"]
    chunks = pd.read_sql_table("arxiv_articles", con, columns=columns, chunksize=1000)
    papers = pd.concat(chunks)

    # Drop index
    papers = papers.reset_index(drop=True)

    # Drop papers without a title or abstract
    papers = papers.dropna(subset=["title", "abstract"])

    # Keep the year from the publication date
    papers["year"] = papers.created.apply(lambda x: x.year)

    # Flag covid-19 papers
    covid_keywords = cord19.config["keywords"]["covid_19"]
    papers["is_Covid"] = [
        1
        if any(term in row["abstract"] for term in covid_keywords)
        or any(term in row["title"] for term in covid_keywords)
        else 0
        for idx, row in papers.iterrows()
    ]
    logging.info(f"Total COVID-19 papers in *rxiv: {papers.is_Covid.sum()}")

    # Store interim table
    papers.to_csv(f"{cord19.project_dir}/data/interim/papers.csv", index=False)


if __name__ == "__main__":
    main()

"""
Reads a table with papers from data/interim, preprocesses abstracts, creates n-grams and 
flags AI papers using a keyword-based approach. The keywords are manually handpicked using 
a word2vec model. Stores the word2vec model, preprocessed abstracts and a table with the papers.

"""
import logging
import pandas as pd
import cord19
from cord19.transformers.nlp import clean_and_tokenize
from gensim.models.phrases import Phrases, Phraser
from gensim.models.word2vec import Word2Vec
import pickle


def main():
    # Read papers
    papers = pd.read_csv(f"{cord19.project_dir}/data/interim/papers.csv")

    # Tokenise paper abstracts
    abstracts = [clean_and_tokenize(d, remove_stops=True) for d in papers.abstract]

    # Create trigrams
    phrases = Phrases(abstracts, min_count=5, threshold=10)
    bigram = Phraser(phrases)
    trigram = Phrases(bigram[abstracts], min_count=5, threshold=3)
    abstracts_with_ngrams = list(trigram[abstracts])

    # Train a word2vec model
    w2v = Word2Vec(
        abstracts_with_ngrams, size=300, window=10, min_count=5, seed=42, iter=2
    )

    ml_keywords = cord19.config["keywords"]["ai"]
    papers["is_AI"] = [
        1 if any(k in tokens for k in ml_keywords) else 0
        for tokens in abstracts_with_ngrams
    ]
    logging.info(f"Total AI papers in *rxiv: {papers.is_AI.sum()}")

    # Overwrite interim table, processed abstracts and models
    papers.to_csv(f"{cord19.project_dir}/data/interim/papers.csv", index=False)

    with open(
        f"{cord19.project_dir}/data/interim/processed_abstracts.pickle", "wb"
    ) as h:
        pickle.dump(abstracts_with_ngrams, h)

    w2v.save(f"{cord19.project_dir}/models/word2vec.model")


if __name__ == "__main__":
    main()

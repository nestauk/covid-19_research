import re
import string
import nltk
from nltk.corpus import stopwords
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer

nltk.download("stopwords", quiet=True)
nltk.download("punkt", quiet=True)

stop_words = set(
    stopwords.words("english") + list(string.punctuation) + ["\\n"] + ["quot"]
)

regex_str = [
    r"http[s]?://(?:[a-z]|[0-9]|[$-_@.&+]|" r"[!*\(\),](?:%[0-9a-f][0-9a-f]))+",
    r"(?:\w+-\w+){2}",
    r"(?:\w+-\w+)",
    r"(?:\\\+n+)",
    r"(?:@[\w_]+)",
    r"<[^>]+>",
    r"(?:\w+'\w)",
    r"(?:[\w_]+)",
    r"(?:\S)",
]

# Create the tokenizer which will be case insensitive and will ignore space.
tokens_re = re.compile(r"(" + "|".join(regex_str) + ")", re.VERBOSE | re.IGNORECASE)


def tokenize_document(text, remove_stops=False):
    """Preprocess a whole raw document.

    Args:
        text (str): Raw string of text.
        remove_stops (bool): Flag to remove english stopwords

    Return:
        List of preprocessed and tokenized documents

    """
    return [
        clean_and_tokenize(sentence, remove_stops)
        for sentence in nltk.sent_tokenize(text)
    ]


def clean_and_tokenize(text, remove_stops):
    """Preprocess a raw string/sentence of text.

    Args:
       text (str): Raw string of text.
       remove_stops (bool): Flag to remove english stopwords

    Return:
       tokens (list, str): Preprocessed tokens.

    """
    tokens = tokens_re.findall(text)
    _tokens = [t.lower() for t in tokens]
    filtered_tokens = [
        token.replace("-", "_")
        for token in _tokens
        if not (remove_stops and len(token) <= 2)
        and (not remove_stops or token not in stop_words)
        and not any(x in token for x in string.digits)
        and any(x in token for x in string.ascii_lowercase)
    ]
    return filtered_tokens


def tfidf_vectors(data, max_features):
    """Transforms text to tfidf vectors.

    Args:
        data (pandas.Series)
    
    Returns:
        (`scipy.sparse`): Sparse TFIDF matrix.

    """
    vectorizer = TfidfVectorizer(
        stop_words="english", analyzer="word", max_features=max_features
    )
    return vectorizer.fit_transform(data)
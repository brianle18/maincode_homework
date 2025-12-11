import spacy
import tiktoken
from nltk.tokenize import word_tokenize


def tokenise_spacy(text: str, model: str = "en_core_web_sm") -> int:
    """Tokenise text using spaCy."""
    nlp = spacy.load(model)
    doc = nlp(text)
    return len(doc)


def tokenise_nltk(text: str) -> int:
    """Tokenise text using NLTK."""
    tokens = word_tokenize(text)
    return len(tokens)


def tokenise_tiktoken(text: str, model: str = "gpt-4o") -> int:
    """Tokenise text using tiktoken."""
    encoding = tiktoken.encoding_for_model(model)
    tokens = encoding.encode(text)
    return len(tokens)


if __name__ == "__main__":
    # Load English tokeniser, tagger, parser and NER
    nlp = spacy.load("en_core_web_sm")

    # Process whole documents
    text = (
        "When Sebastian Thrun started working on self-driving cars at "
        "Google in 2007, few people outside of the company took him "
        "seriously. “I can tell you very senior CEOs of major American "
        "car companies would shake my hand and turn away because I wasn’t "
        "worth talking to,” said Thrun, in an interview with Recode earlier "
        "this week."
    )

    print(f"spaCy tokens: {tokenise_spacy(text)}")
    print(f"NLTK tokens: {tokenise_nltk(text)}")
    print(f"tiktoken tokens: {tokenise_tiktoken(text)}")

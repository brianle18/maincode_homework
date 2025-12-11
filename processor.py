import enum
import re
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup
from langdetect import DetectorFactory, detect_langs
from lingua import LanguageDetectorBuilder, Language
from pygments.lexers import guess_lexer
from presidio_analyzer import AnalyzerEngine, BatchAnalyzerEngine
from presidio_anonymizer import BatchAnonymizerEngine
from tokeniser import tokenise_nltk, tokenise_spacy, tokenise_tiktoken

DetectorFactory.seed = 0


class LanguageTool(enum.Enum):
    LINGUA = "lingua"
    LANGDETECT = "langdetect"


def deduplicate_data(
    data: pd.DataFrame, fields: list[str] = ["text", "url"], apply_filter: bool = False
) -> pd.DataFrame:
    """Remove duplicate rows from the DataFrame."""
    print("Deduplicating data...")
    data.loc[:, "duplicate"] = data.duplicated(subset=fields)
    data.loc[:, "text_original"] = data.loc[:, "text"]
    data.loc[:, "text"] = data.loc[:, "text"].str.strip()
    if apply_filter:
        print("\tDuplicates filtered")
        data = data[~data["duplicate"]]
    return data


def extract_domain(url: str) -> str | None:
    """Extract the domain from a given URL."""
    try:
        parsed_url = urlparse(url)
        return parsed_url.__getattribute__("netloc")
    except Exception:
        return None


def extract_domain_from_col(data: pd.DataFrame, filter_github=True) -> pd.DataFrame:
    """
    Extract the domain from the 'url' column of the DataFrame and add it as a new column.
    """
    print("Extracting domain from URL...")
    data.loc[:, "domain"] = data.loc[:, "url"].transform(extract_domain)
    if filter_github:
        data = data[data["domain"] != "github.com"]
    return data


def extract_language(
    data: pd.DataFrame,
    tool: LanguageTool = LanguageTool.LANGDETECT,
    en_only: bool = False,
    en_threshold: float = 0.9,
) -> pd.DataFrame:
    """
    Detect the language of the text in the DataFrame and add it as a new column.
    """
    print("Extracting language...")
    print(f"\tUsing tool: {tool.value}")
    if tool == LanguageTool.LINGUA:
        # NOTE Dataframe level operation (already parallised inside the library)
        detector = LanguageDetectorBuilder.from_all_languages().build()
        data.loc[:, "detected_language_lang"] = (
            detector.detect_languages_in_parallel_of(data.loc[:, "text"])
        )

        # Convert to ISO 639-1 codes
        data.loc[:, "detected_language_lang"] = data.loc[
            :, "detected_language_lang"
        ].transform(lambda x: x.iso_code_639_1.name)

        # Calculate English confidence
        data.loc[:, "detected_language_prob"] = (
            detector.compute_language_confidence_in_parallel(
                data.loc[:, "text"], Language.ENGLISH
            )
        )
        if en_only:
            data = data[
                (data["detected_language_lang"] == "EN")
                & (data["detected_language_prob"] >= en_threshold)
            ]
    elif tool == LanguageTool.LANGDETECT:
        # TODO: Parallelize this operation for better performance on large datasets
        data.loc[:, "detected_language"] = data.loc[:, "text"].transform(
            detect_langs_safe
        )
        data.loc[:, "detected_language_lang"] = data.loc[
            :, "detected_language"
        ].transform(lambda x: x.lang if x is not None else None)
        data.loc[:, "detected_language_prob"] = data.loc[
            :, "detected_language"
        ].transform(lambda x: x.prob if x is not None else None)

        if en_only:
            print(f"\tFiltering non-English texts with probability < {en_threshold}...")
            data = data[
                (data["detected_language_lang"] == "en")
                & (data["detected_language_prob"] >= en_threshold)
            ]
    return data


def detect_langs_safe(text: str) -> str | None:
    """Detect the language of the given text safely."""
    try:
        langs = detect_langs(text)
        if len(langs) > 0:
            return langs[0]
        return None
    except Exception:
        print("Language detection failed for text", text, sep="\n")
        return None


def clean_ascii(text: str) -> str:
    """Clean the text by removing non-ASCII characters."""
    return "".join(c for c in text if (c.isprintable() or c in ["\n", "\r", "\t"]))


def clean_text_ascii(data: pd.DataFrame) -> pd.DataFrame:
    """Apply ASCII cleaning to the 'text' column of the DataFrame."""
    print("Cleaning ASCII characters...")
    data.loc[:, "text"] = data.loc[:, "text"].transform(clean_ascii)
    return data


def check_any_alphabetic(text: str) -> bool:
    """Check if the text contains any alphabetic characters."""
    return any(c.isalpha() for c in text)


def check_text_is_alphabetic(
    data: pd.DataFrame, apply_filter: bool = False
) -> pd.DataFrame:
    """Add a column indicating if the 'text' contains any alphabetic characters."""
    print("Checking if text has alphabetic characters...")
    data.loc[:, "has_alphabetic"] = data.loc[:, "text"].transform(check_any_alphabetic)
    if apply_filter:
        print("\tNon-alphabetic text filtered")
        data = data[data["has_alphabetic"]]
    return data


def check_hyperlink(text: str) -> bool:
    """Check if the text is mostly a hyperlink."""
    # replace urls in string with empty string
    urls = re.findall(r"(?P<url>https?://[^\s]+)", text)
    # calculate the ratio of url length to text length
    if len(urls) == 0:
        return False
    else:
        return float(len("".join(urls))) / float(len(text)) > 0.9


def check_is_code(text: str) -> bool:
    """Check if the text is code by attempting to guess its lexer."""
    try:
        lexer = guess_lexer(text)
        # If the guessed lexer is not a TextLexer, we consider it as code
        return lexer.name
    except Exception as e:
        print(e)
        return False


def check_text_is_code(data: pd.DataFrame, apply_filter: bool = False) -> pd.DataFrame:
    """Add a column indicating if the 'text' is code."""
    print("Checking if text is code...")
    data.loc[:, "is_code"] = data.loc[:, "text"].transform(check_is_code)
    if apply_filter:
        print("\tCode text filtered")
        data = data[~(data["is_code"] == "Text only")]
    return data


def check_text_is_hyperlink(
    data: pd.DataFrame, apply_filter: bool = False
) -> pd.DataFrame:
    """Add a column indicating if the 'text' contains a hyperlink."""
    print("Checking if text is a hyperlink...")
    data.loc[:, "is_hyperlink"] = data.loc[:, "text"].transform(check_hyperlink)
    if apply_filter:
        print("\tHyperlink text filtered")
        data = data[~data["is_hyperlink"]]
    return data


def clean_html(text: str) -> str:
    """Remove HTML tags from the text."""
    return BeautifulSoup(text).get_text()


def clean_text_html(data: pd.DataFrame) -> pd.DataFrame:
    """Apply HTML cleaning to the 'text' column of the DataFrame."""
    print("Cleaning HTML tags...")
    data.loc[:, "text"] = data.loc[:, "text"].transform(clean_html)
    return data


def clean_short_length(
    data: pd.DataFrame, min_text_length: int = 20, min_word_count: int = 10
) -> pd.DataFrame:
    """Remove rows with 'text' shorter than the specified minimum length."""
    data.loc[:, "text_length"] = data.loc[:, "text"].str.len()
    data.loc[:, "word_length"] = data.loc[:, "text"].str.split(" ").str.len()
    data = data[
        (data["text_length"] >= min_text_length)
        & (data["word_length"] >= min_word_count)
    ]
    return data


def detect_pii(data: pd.DataFrame, mask: bool = False) -> pd.DataFrame:
    """Detect if the text contains PII (e.g., email addresses, phone numbers)."""
    analyser = AnalyzerEngine()
    batch_analyser = BatchAnalyzerEngine(analyzer_engine=analyser)
    batch_anonymizer = BatchAnonymizerEngine()
    data.loc[:, "has_pii"] = list(
        batch_analyser.analyze_iterator(
            data.loc[:, "text"].to_list(), language="en", n_process=4, batch_size=100
        )
    )
    if mask:
        data.loc[:, "text"] = list(
            batch_anonymizer.anonymize_list(
                data.loc[:, "text"].to_list(),
                data.loc[:, "has_pii"].to_list(),
            )
        )
    data.loc[:, "has_pii"] = data.loc[:, "has_pii"].transform(lambda x: len(x) > 0)
    return data


def tokenise_texts(data: pd.DataFrame, method: str = "tiktoken") -> pd.DataFrame:
    """Tokenise the 'text' column using different tokenisation methods."""
    print("Tokenising texts...")
    assert method in ["spacy", "nltk", "tiktoken"], (
        "Invalid tokenisation method, choose from 'spacy', 'nltk', 'tiktoken'"
    )
    if method == "spacy":
        data.loc[:, "token_count"] = data.loc[:, "text"].transform(tokenise_spacy)
    elif method == "nltk":
        data.loc[:, "token_count"] = data.loc[:, "text"].transform(tokenise_nltk)
    elif method == "tiktoken":
        data.loc[:, "token_count"] = data.loc[:, "text"].transform(tokenise_tiktoken)
    return data

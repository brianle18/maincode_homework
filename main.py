import os
import pandas as pd
from processor import (
    clean_text_ascii,
    clean_text_html,
    clean_short_length,
    detect_pii,
    check_text_is_code,
    check_text_is_alphabetic,
    check_text_is_hyperlink,
    deduplicate_data,
    extract_domain_from_col,
    extract_language,
    LanguageTool,
)
from config import read_config


def load_data(file_path: str, **kwargs) -> pd.DataFrame:
    """Load data from a CSV file into a pandas DataFrame."""
    return pd.read_json(file_path, lines=True, nrows=1000, **kwargs)


if __name__ == "__main__":
    # Config options
    config = read_config("config.json")
    filters: dict = config.get("filters", {})
    cleaners: dict = config.get("cleaners", {})

    if not os.path.exists("output"):
        os.makedirs("output")
    if not os.path.exists(f"output/{config.get('outname', 'cleaned')}"):
        os.makedirs(f"output/{config.get('outname', 'cleaned')}")

    # Load data
    data = load_data(config["filename"])

    try:
        language_tool = LanguageTool(filters.get("filter_lang_method", "lingua"))
    except ValueError as e:
        print(f"Error initializing language tool: {e}")
        print("Defaulting to LINGUA")
        language_tool = LanguageTool.LINGUA

    # Main data pipeline
    data_processed = (
        data.pipe(
            deduplicate_data,
            apply_filter=filters.get("filter_duplicates", True),
            fields=["text"],
        )
        .pipe(
            lambda data: clean_text_ascii(data)
            if cleaners.get("remove_ascii_characters", True)
            else data
        )
        .pipe(
            check_text_is_alphabetic,
            apply_filter=filters.get("filter_alphabetic_only", True),
        )
        .pipe(
            check_text_is_hyperlink, apply_filter=filters.get("filter_hyperlinks", True)
        )
        .pipe(
            lambda data: clean_text_html(data)
            if cleaners.get("html_normalise", True)
            else data
        )
        .pipe(
            clean_short_length,
            min_text_length=filters.get("filter_text_length_threshold", 50),
            min_word_count=filters.get("filter_word_count_threshold", 20),
        )
        # .pipe(check_text_is_code, apply_filter=filters.get("filter_code", False))
        .pipe(extract_domain_from_col, filter_github=filters.get("filter_github", True))
        .pipe(
            extract_language,
            tool=language_tool,
            en_only=True,
            en_threshold=0.9,
        )
        .pipe(detect_pii, mask=True)
    )
    print(len(data_processed))
    data_processed.info()

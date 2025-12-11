import os
import sys
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
    tokenise_texts,
    LanguageTool,
)
from config import read_config
from splitting import split_data


def load_data(file_path: str, **kwargs) -> pd.DataFrame:
    """Load data from a CSV file into a pandas DataFrame."""
    return pd.read_json(file_path, lines=True, **kwargs)


if __name__ == "__main__":
    # Config options
    config = read_config(sys.argv[1] if len(sys.argv) > 1 else "config.json")
    filters: dict = config.get("filters", {})
    cleaners: dict = config.get("cleaners", {})
    splitter: dict = config.get("splitter", {})

    nrows = config.get("nrows", None)
    seed = config.get("random_seed", 42)

    if not os.path.exists("output"):
        os.makedirs("output")
    if not os.path.exists(
        f"output/{os.path.dirname(config.get('outname', 'cleaned'))}"
    ):
        os.makedirs(f"output/{os.path.dirname(config.get('outname', 'cleaned'))}")

    # Load data
    data = load_data(config["filename"], nrows=nrows)
    print("Initial dataset size", len(data))
    data = data.sample(frac=1, random_state=42)

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
            en_only=filters.get("filter_en_only", True),
            en_threshold=0.9,
        )
        .pipe(detect_pii, mask=filters.get("mask_pii", False))
        .pipe(tokenise_texts, method=config.get("tokenisation_method", "tiktoken"))
    )
    print("Final dataset size", len(data_processed))
    data_processed.info()

    # Save processed data
    if splitter == {}:
        print(
            f"Saving processed data to output/{config.get('outname', 'cleaned.jsonl')}"
        )
        data_processed.to_json(
            f"output/{config.get('outname', 'cleaned.jsonl')}",
            lines=True,
            orient="records",
        )
    else:
        train, valid, test = split_data(
            data_processed,
            test_size=splitter.get("test_size", 0.3),
            val_size=splitter.get("val_size", 0.3),
            random_state=splitter.get("random_state", 42),
        )
        # Save each split
        print(
            f"Saving training data to output/{config.get('outname', 'cleaned').replace('.jsonl', '')}_train.jsonl"
        )
        train.to_json(
            f"output/{config.get('outname', 'cleaned').replace('.jsonl', '')}_train.jsonl",
            lines=True,
            orient="records",
        )
        print(
            f"Saving validation data to output/{config.get('outname', 'cleaned').replace('.jsonl', '')}_valid.jsonl"
        )
        valid.to_json(
            f"output/{config.get('outname', 'cleaned').replace('.jsonl', '')}_valid.jsonl",
            lines=True,
            orient="records",
        )
        print(
            f"Saving test data to output/{config.get('outname', 'cleaned').replace('.jsonl', '')}_test.jsonl"
        )
        test.to_json(
            f"output/{config.get('outname', 'cleaned').replace('.jsonl', '')}_test.jsonl",
            lines=True,
            orient="records",
        )

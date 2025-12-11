# Maincode Homework — Data cleaning pipeline

A small end-to-end data cleaning pipeline that ingests newline-delimited JSON (JSONL) with a `text` field (and optional `url`), applies cleaning and filtering steps, optionally splits data into train/validation/test sets, and writes cleaned JSONL outputs to the `output/` folder.

This README describes how to run the code, required I/O, configuration options, and common notes.

## Requirements

- Python 3.13
- Recommended packages (example): pandas, beautifulsoup4, nltk, matplotlib, seaborn, presidio, tiktoken, spacy, scikit-learn
- Optional: lingua or langdetect for language detection; detoxify and PyTorch for toxicity filtering

Install example (using the project's `uv` manager):

```bash
# Install dependencies defined for the project
uv install

# If you need to install spaCy language models
uv run python3 -m spacy download en_core_web_sm
```

(If not using `uv`, create and activate a virtual environment and install with pip as a fallback.)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# or install curated packages
pip install pandas beautifulsoup4 nltk matplotlib seaborn presidio tiktoken spacy scikit-learn
python -m spacy download en_core_web_sm
```

Note: If you use `lingua`, follow that project's install instructions — it has Java/JNI components and is not a pure Python wheel.

## Input / Output

Input
- A JSONL file (newline-delimited JSON) where each record must contain at least a `text` field. An optional `url` field is supported and used for domain extraction.
- Provide the input filename in the configuration file (see below).

Output
- The pipeline writes cleaned JSONL files into `output/` and a subfolder `output/<outname>/` (the script will create these if missing).
- If splitting is disabled, output is:
  - `output/<outname>_cleaned.jsonl`
- If splitting is enabled, outputs are:
  - `output/<outname>_train.jsonl`
  - `output/<outname>_valid.jsonl`
  - `output/<outname>_test.jsonl`

All outputs are newline-delimited JSON with the same record schema as the processed DataFrame (fields include `text`, `url`, derived metadata, masks, etc.).

## How to run

Basic invocation (from repo root):

```bash
uv run main.py [config.json]
```

- If no config path is provided, the script attempts to load `config.json` from the repository root.
- The script prints progress and writes outputs to the `output/` directory.

## Configuration file (config.json)

The pipeline is controlled by a JSON configuration file. Example keys and behaviour are described below.

Top-level keys
- `filename` (string, required): Path to input JSONL file.
- `nrows` (int or null): Optional; if set, load only the first N rows (useful for debugging).
- `outname` (string): Base name for output files (default: `cleaned`).
- `filters` (object): Filtering flags and thresholds.
- `cleaners` (object): Cleaning toggles for specific cleaners.
- `splitter` (object): If empty or missing, no split is performed; otherwise provide split parameters.

Filters (example)
- `filter_duplicates` (bool): Remove exact duplicate texts (default `true`).
- `filter_alphabetic_only` (bool): Remove texts that are non-alphabetic dominant (default `true`).
- `filter_hyperlinks` (bool): Remove texts dominated by URLs (default `true`).
- `filter_text_length_threshold` (int): Minimum characters required (default `50`).
- `filter_word_count_threshold` (int): Minimum word count required (default `20`).
- `filter_github` (bool): Special handling / extraction for GitHub-sourced text (default `true`).
- `filter_en_only` (bool): Keep only English-detected rows (default `true`).
- `filter_lang_method` (string): Choose language detection backend (e.g. `lingua` or `langdetect`).
- `filter_code` (bool): Enable code detection filtering (default `false`).

Cleaners (example)
- `remove_ascii_characters` (bool): Remove non-printable / non-ASCII characters (default `true`).
- `html_normalise` (bool): Strip and normalise HTML using BeautifulSoup (default `true`).

Splitter (example)
- `test_size` (float): Fraction for the test set (e.g. `0.2`).
- `val_size` (float): Fraction for the validation set (e.g. `0.1`).
- `random_state` (int): Seed for reproducible splits.

### Example config.json

```json
{
  "filename": "data/input.jsonl",
  "nrows": null,
  "outname": "cleaned",
  "filters": {
    "filter_duplicates": true,
    "filter_alphabetic_only": true,
    "filter_hyperlinks": true,
    "filter_text_length_threshold": 50,
    "filter_word_count_threshold": 20,
    "filter_github": true,
    "filter_en_only": true,
    "filter_lang_method": "lingua"
  },
  "cleaners": {
    "remove_ascii_characters": true,
    "html_normalise": true
  },
  "splitter": {
    "test_size": 0.2,
    "val_size": 0.1,
    "random_state": 42
  }
}
```

## Notes, tips and behavior

- Language detection: `lingua` generally provides higher accuracy but requires more resources; `langdetect` is lightweight but less robust on noisy/short texts. The script wraps a language tool abstraction—choose via `filter_lang_method`.
- PII detection (`presidio`) is computationally expensive; enable only for sampled or final runs.
- Tokenisation: Several tokenisers are supported or referenced in the report (NLTK, spaCy, tiktoken). Use `tiktoken` when preparing data for OpenAI-style token limits; spaCy / NLTK for NLP tooling and token counts.
- Scalability: For large datasets switch to partitioned processing: chunked reads, Dask/Ray/Apache Spark, or run expensive operations (language detection, PII masking) on partitions in parallel.
- Outputs: The cleaned JSONL preserves input fields and adds derived metadata (domain, language scores, PII masks, etc.). Inspect `data_processed.info()` output for exact columns after running.

## Troubleshooting

- If outputs are missing, check `config.json` path and `filename` value.
- For memory errors, run with `nrows` or switch to a distributed/partitioned processing method.
- If spaCy models are missing, run `uv run python3 -m spacy download en_core_web_sm`.

## Development

- Code entrypoint: `main.py`.
- Primary processing functions live in `processor.py`, language tools in `processor.py` and `splitting.py` handles dataset splitting.
- Do not modify source code unless familiar with the pipeline semantics; instead modify `config.json` to change behavior.

## License & Author

Author: Dr Brian Le
Last updated: 2025-12-11T12:10:18Z


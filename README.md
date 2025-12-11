# Installation
To install this project, follow these steps:
1. Clone the repository:
   ```bash
   git clone
   ```
2. Install uv dependencies:
   ```bash
   uv install
   ```
3. Download spacy files
    ```bash
    uv run python3 -m spacy download en_core_web_sm
    ```

# Usage
To use this project, run the main script:
```bash
uv run main.py
```

# TODO
- [x] Config file for filter/cleaning options
- [x] Deduplication
- [x] Language detection
    - [x] Check Lingua
- [x] Code detection (filtering using github)
    - [x Try pygments
    - [ ] Try guesslang
- [x] Clean text (remove special characters)
- [x] Remove HTML tags
- [x] Remove URL strings
- [x] Remove short texts
    - [ ] Fine tune threshold
- [ ] Normalize text (lowercase, etc.)
- [x] PII and detoxify
    - [ ] Detoxify - issues with install
- [x] Tokenise
- [ ] Mixing/Sharding
- [ ] README and documentation

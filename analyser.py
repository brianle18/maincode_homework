import sys
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def make_language_plot(data: pd.DataFrame, outpath: str) -> None:
    """Generate and save a language distribution plot."""
    plt.figure(figsize=(10, 6))
    sns.countplot(
        data=data,
        x="detected_language_lang",
        order=data["detected_language_lang"]
        .value_counts()
        .sort_values(ascending=False)
        .index,
    )
    plt.title("Language Distribution")
    plt.xlabel("Language")
    plt.ylabel("Count")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(outpath.replace(".jsonl", "") + "_lang_dist.svg")
    plt.close()
    print(
        f"Saved language distribution plot to {outpath.replace('.jsonl', '') + '_lang_dist.svg'}"
    )


def make_text_length_plot(data: pd.DataFrame, outpath: str) -> None:
    """Generate and save a text length distribution plot."""
    fig, axs = plt.subplots(figsize=(10, 6))
    twin_axs = axs.twinx()
    sns.histplot(
        data=data, x="text_length", bins=50, kde=True, log_scale=(True, True), ax=axs
    )
    sns.ecdfplot(
        data=data, x="text_length", ax=twin_axs, color="orange", stat="proportion"
    )
    plt.title("Text Length Distribution")
    axs.set_xlabel("Text Length (characters)")
    axs.set_ylabel("Frequency")
    twin_axs.set_ylabel("Cumulative Proportion")
    plt.tight_layout()
    plt.savefig(outpath.replace(".jsonl", "") + "_text_length.svg")
    plt.close()
    print(
        f"Saved text length plot to {outpath.replace('.jsonl', '') + '_text_length.svg'}"
    )


def make_word_count_plot(data: pd.DataFrame, outpath: str) -> None:
    """Generate and save a word count distribution plot."""
    fig, axs = plt.subplots(figsize=(10, 6))
    twin_axs = axs.twinx()
    sns.histplot(
        data=data, x="word_length", bins=50, kde=True, log_scale=(True, True), ax=axs
    )
    sns.ecdfplot(
        data=data, x="word_length", ax=twin_axs, color="orange", stat="proportion"
    )
    plt.title("Word Count Distribution")
    axs.set_xlabel("Word Count")
    axs.set_ylabel("Frequency")
    twin_axs.set_ylabel("Cumulative Proportion")
    plt.tight_layout()
    plt.savefig(outpath.replace(".jsonl", "") + "_word_count.svg")
    plt.close()
    print(
        f"Saved word count plot to {outpath.replace('.jsonl', '') + '_word_count.svg'}"
    )


def make_token_count_plot(data: pd.DataFrame, outpath: str) -> None:
    """Generate and save a token count distribution plot."""
    fig, axs = plt.subplots(figsize=(10, 6))
    twin_axs = axs.twinx()
    sns.histplot(
        data=data, x="token_count", bins=50, kde=True, log_scale=(True, True), ax=axs
    )
    sns.ecdfplot(
        data=data, x="token_count", ax=twin_axs, color="orange", stat="proportion"
    )
    plt.title("Token Count Distribution")
    axs.set_xlabel("Token Count")
    axs.set_ylabel("Frequency")
    twin_axs.set_ylabel("Cumulative Proportion")
    plt.tight_layout()
    plt.savefig(outpath.replace(".jsonl", "") + "_token_count.svg")
    plt.close()
    print(
        f"Saved token count plot to {outpath.replace('.jsonl', '') + '_token_count.svg'}"
    )


def generate_analysis_plots(data: pd.DataFrame, outpath: str) -> None:
    """Generate and save analysis plots for the given DataFrame."""
    make_text_length_plot(data, outpath)
    make_word_count_plot(data, outpath)
    make_language_plot(data, outpath)
    make_token_count_plot(data, outpath)


if __name__ == "__main__":
    path = sys.argv[-1]

    data = pd.read_json(path, lines=True)
    generate_analysis_plots(data, path)

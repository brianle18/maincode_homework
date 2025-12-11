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
    plt.savefig(outpath)
    plt.close()

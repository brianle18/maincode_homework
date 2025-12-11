from sklearn.model_selection import train_test_split


def split_data(
    data: pd.DataFrame,
    test_size: float = 0.2,
    val_size: float = 0.1,
    random_state: int = 42,
    **kwargs,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split the DataFrame into training, validation, and test sets."""
    train_data, test_data = train_test_split(
        data, test_size=test_size, random_state=random_state, **kwargs
    )
    val_data, test_data = train_test_split(
        test_data,
        test_size=val_size / (test_size + val_size),
        random_state=random_state,
    )
    return train_data, val_data, test_data

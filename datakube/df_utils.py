import typing as T

import pandas as pd


def sanitize(df: pd.DataFrame) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df


def partition_and_normalize(
    df: pd.DataFrame,
    splits: T.Sequence[T.Tuple[pd.Timestamp, pd.Timestamp]],
) -> T.List[pd.DataFrame]:
    partitions = []
    for start, end in splits:
        partition = df[df["timestamp"].between(start, end)]
        partition["normalized_ts"] = partition["timestamp"] - start
        partitions.append(partition)

    return partitions


def extract_labels_to_columns(df: pd.DataFrame, labels: T.List[str]) -> pd.DataFrame:
    # non-greedy match, terminate the match after the first comma or end of string
    new_df = df
    for label in labels:
        new_df[label] = df["labels"].str.extract(f"{label}=(.*?)(?:,|$)")
    return new_df

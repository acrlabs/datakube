import typing as T

import numpy as np
import pandas as pd

from datakube.constants import NORM_TS_KEY
from datakube.constants import VALUE_KEY


def sanitize(df: pd.DataFrame) -> pd.DataFrame:
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["timestamp"] = df["timestamp"].dt.floor("1s")
    return df


def partition_and_normalize(
    df: pd.DataFrame,
    splits: T.Sequence[T.Tuple[pd.Timestamp, pd.Timestamp]],
) -> T.List[pd.DataFrame]:
    partitions = []
    for start, end in splits:
        partition = df[df["timestamp"].between(start, end)]
        partition[NORM_TS_KEY] = ((partition["timestamp"] - start) / 1000000000).astype(int)
        partition = partition.set_index(NORM_TS_KEY)
        partitions.append(partition)

    return partitions


def extract_labels_to_columns(df: pd.DataFrame, labels: T.Union[str, T.List[str]]) -> pd.DataFrame:
    # non-greedy match, terminate the match after the first comma or end of string
    new_df = df
    if isinstance(labels, str):
        labels = [labels]

    for label in labels:
        new_df[label] = df["labels"].str.extract(f"{label}=(.*?)(?:,|$)")
    return new_df


def aggregate_timeseries(dfs: T.List[pd.DataFrame], key_prefix: str, aggfunc: str = "sum") -> pd.DataFrame:
    data, keys = [], []
    for i, df in enumerate(dfs):
        data.append(df[VALUE_KEY].groupby(level=0).aggregate(aggfunc))
        keys.append(f"{key_prefix}.{i}")

    return pd.concat(data, axis=1, keys=keys).ffill(limit_area="inside").sort_index()  # type: ignore


def delta_histogram(
    df: pd.DataFrame,
    *,
    nbins: int = 10,
    lower: float = 0,
    baseline: float = 0,
) -> T.Tuple[np.ndarray, np.ndarray]:
    deltas = df.diff()
    all_non_zero_deltas = T.cast(
        pd.Series,
        pd.concat(
            [deltas.loc[deltas[col] > baseline, col] for col in deltas],  # type: ignore
            ignore_index=True,
        ),
    )
    sorted_non_zero_deltas = all_non_zero_deltas.sort_values()

    # We sorted them so the biggest value is at the end
    dmax = sorted_non_zero_deltas.iloc[-1]
    bins = [rg[0] for rg in pd.interval_range(0, dmax, nbins).to_tuples()] + [dmax]

    return np.histogram(sorted_non_zero_deltas, bins=bins)

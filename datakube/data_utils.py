import typing as T
from datetime import timedelta
from enum import Enum

import numpy as np
import pandas as pd
from arrow import Arrow
from duckdb.duckdb import DuckDBPyConnection
from duckdb.duckdb import DuckDBPyRelation

from datakube.constants import NORM_TS_KEY


class Comparison(Enum):
    LT = "<"
    LE = "<="
    EQ = "="
    NE = "!="
    GE = ">="
    GT = ">"


class DataKubeRelation:
    def __init__(self, rel: DuckDBPyRelation, conn: DuckDBPyConnection):
        self._rel = rel
        self._conn = conn

    def df(self) -> pd.DataFrame:
        return self._rel.df()

    def fill_missing_data(self, grouper: str, window_size: int = 3) -> T.Self:
        self._rel = self._rel.select(
            "time_bucket(INTERVAL '1s', to_timestamp(timestamp / 1000)) AS rounded_ts, *",
        ).query(
            "rounded_times",
            # We're using native SQL here instead of the relation API because it's (I think) a little
            # clearer, and the windowing query at the end I don't *think* is possible with the relational
            # API anyways, so I decided to just do the whole thing in SQL.  Anyways, this bit is filling
            # in any "missing" data from the raw timeseries data:
            #
            # 1. First we find the minimum and maximum time values for each unique element (say, each pod)
            # 2. Then we gnerate the "complete" list of timestamps between those min and max time values per element
            # 3. We join the "complete" list of timestamps with the existing (normalized) data; we LEFT JOIN
            #    so that any missing data points will appear with NULL values in the result
            # 4. Lastly, we window the data and take the last not null value that we find (essentially, as long as
            #    there's a datapoint in the window we use it).  The PARTITION BY clause ensures that the window doesn't
            #    go outside the bounds of the partition, i.e., it doesn't use data from a different element (pod, for
            #    example).
            f"""
            WITH time_ranges AS (
                SELECT min(rounded_ts) as mints, max(rounded_ts) as maxts, {grouper},
                FROM rounded_times GROUP BY {grouper}
            ), all_timestamps AS (
                SELECT unnest(generate_series(mints::timestamptz, maxts::timestamptz, INTERVAL '1s')) AS timestamp,
                    {grouper},
                FROM time_ranges
            ), joined_timestamps AS (
                SELECT a.timestamp, a.{grouper}, r.value, r.labels,
                FROM all_timestamps a LEFT JOIN rounded_times r
                ON a.timestamp = r.rounded_ts AND a.{grouper} = r.{grouper}
            )

            SELECT timestamp, {grouper},
                last_value(value IGNORE NULLS) OVER group_window AS value,
                last_value(labels IGNORE NULLS) OVER group_window AS labels,
            FROM joined_timestamps
            WINDOW group_window AS (
                PARTITION BY {grouper}
                ORDER BY timestamp
                ROWS BETWEEN {window_size} PRECEDING AND CURRENT ROW
            )
            """,
        )
        return self

    def partition_and_normalize(
        self,
        splits: T.List[T.Tuple[Arrow, Arrow]],
        prefix: str = "sim",
    ) -> T.Self:
        cases = "".join([
            f"WHEN timestamp BETWEEN '{start}' AND '{end}' THEN '{prefix}.{i}'" for i, (start, end) in enumerate(splits)
        ])
        self._rel = self._rel.select(f""" *,
            CASE {cases} END AS {prefix},
            timestamp - min(timestamp) OVER (PARTITION BY {prefix} ORDER BY timestamp) AS {NORM_TS_KEY},
        """)
        return self

    def to_pivot_table(
        self,
        column: str = "sim",
        aggfunc: str = "sum",
        max_time: T.Optional[timedelta] = None,
        fill_value: T.Optional[float] = None,
    ) -> pd.DataFrame:
        if max_time is None:
            max_time = self._rel.max(NORM_TS_KEY).fetchone()[0]
        self._rel.create_view("to_pivot")

        all_normalized_times = pd.DataFrame(
            index=[timedelta(seconds=i) for i in range(0, int(max_time.total_seconds()))],
        )
        all_normalized_times.index.name = NORM_TS_KEY

        df = (
            self._conn.query(
                f"PIVOT to_pivot ON {column} USING {aggfunc}(value) GROUP BY {NORM_TS_KEY} ORDER BY {NORM_TS_KEY}"
            )
            .df()
            .set_index(NORM_TS_KEY)
            .join(all_normalized_times, how="right")
        )
        df.index = df.index.map(lambda td: td.total_seconds())
        if fill_value is not None:
            df = df.fillna(fill_value)

        return df

    def with_label(self, key: str, value: str) -> T.Self:
        self._rel = self._rel.filter(f"labels LIKE '%{key}={value}%'")
        return self

    def with_namespace(self, ns: str) -> T.Self:
        self._rel = self._rel.filter(f"namespace='{ns}'")
        return self

    def with_value(self, val: float, cmp: Comparison = Comparison.EQ) -> T.Self:
        self._rel = self._rel.filter(f"value {cmp.value} {val}")
        return self


def counter_diff(df: pd.DataFrame) -> pd.DataFrame:
    # If the counter resets, we get a negative value, and then the diffs from then on are the same.
    # In this case, we just assume that the adjacent values when the counter reset were equal, and
    # thus the diff is 0 at that index; this could possibly be a bad assumption but I don't know of
    # a better one to make.
    df_diff = df.diff()
    df_diff[df_diff < 0] = 0
    return df_diff


def delta_histogram(
    df: pd.DataFrame,
    *,
    nbins: int = 10,
    lower: float = 0,
    baseline: float = 0,
) -> T.Tuple[np.ndarray, np.ndarray]:
    deltas = counter_diff(df)
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

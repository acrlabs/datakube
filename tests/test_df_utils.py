import pandas as pd
import pytest

from datakube.df_utils import partition_and_normalize
from datakube.df_utils import sanitize


@pytest.fixture
def df() -> pd.DataFrame:
    return sanitize(
        pd.DataFrame({
            "timestamp": [10000, 11000, 12000, 13000, 14000, 15000, 16000, 17000, 18000, 19000],
            "values": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        })
    )


def test_normalized_df_timestamp_range(df):
    split1 = (pd.Timestamp(11000, unit="ms"), pd.Timestamp(13000, unit="ms"))
    split2 = (pd.Timestamp(15000, unit="ms"), pd.Timestamp(18000, unit="ms"))
    res = partition_and_normalize(df, [split1, split2])

    assert len(res) == 2
    assert len(res[0]) == 3
    assert len(res[1]) == 4

    pd.testing.assert_series_equal(
        res[0]["normalized_ts"],
        pd.Series([pd.Timedelta(v, unit="s") for v in range(3)]),
        check_index=False,
        check_names=False,
    )
    pd.testing.assert_series_equal(
        res[1]["normalized_ts"],
        pd.Series([pd.Timedelta(v, unit="s") for v in range(4)]),
        check_index=False,
        check_names=False,
    )

import pandas as pd
import pytest

from datakube.df_utils import compute_extents
from datakube.df_utils import partition_and_normalize
from datakube.df_utils import sanitize


@pytest.fixture
def df() -> pd.DataFrame:
    return sanitize(
        pd.DataFrame({
            "timestamp": [10000, 11000, 12000, 13000, 14000, 15000, 16000, 17000, 18000, 19000],
            "values1": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "values2": [-11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
        })
    )


@pytest.mark.parametrize("stack", [True, False])
def test_compute_extents(df, stack):
    xmin, xmax, ymin, ymax = compute_extents(df[["values1", "values2"]], stack=stack)
    assert xmin == 0
    assert xmax == 9
    assert ymin == -11
    assert ymax == 30 if stack else 20


def test_normalized_df_timestamp_range(df):
    df = sanitize(df)
    split1 = (pd.Timestamp(11000, unit="ms", tz="UTC"), pd.Timestamp(13000, unit="ms", tz="UTC"))
    split2 = (pd.Timestamp(15000, unit="ms", tz="UTC"), pd.Timestamp(18000, unit="ms", tz="UTC"))
    res = partition_and_normalize(df, [split1, split2])

    assert len(res) == 2
    assert len(res[0]) == 3
    assert len(res[1]) == 4

    pd.testing.assert_series_equal(
        res[0].index.to_series(),
        pd.Series(range(3)),
        check_index=False,
        check_names=False,
    )
    pd.testing.assert_series_equal(
        res[1].index.to_series(),
        pd.Series(range(4)),
        check_index=False,
        check_names=False,
    )

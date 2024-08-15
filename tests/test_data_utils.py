import arrow
import duckdb
import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from datakube.data_utils import DataKubeRelation
from datakube.data_utils import counter_diff
from tests.conftest import DATA_COLS

TEST_METRIC_NAME = "test_metric"


@pytest.fixture
def rel(df):
    conn = duckdb.connect(":memory:")
    conn.sql(f"CREATE TABLE {TEST_METRIC_NAME} AS SELECT * FROM df")
    rel = conn.query(f"SELECT * FROM {TEST_METRIC_NAME}")
    return DataKubeRelation(rel, conn, None)


def test_counter_diff(df):
    expected = pd.DataFrame({"values1": [np.nan] + [1] * 9, "values2": [np.nan, 25, 0, 1, 1, 3, 0, 1, 2, 2]})
    assert_frame_equal(counter_diff(df[DATA_COLS]), expected)


def test_normalized_df_timestamp_range(rel):
    split1 = (arrow.get(11), arrow.get(13))
    split2 = (arrow.get(15), arrow.get(18))
    res = rel.partition_and_normalize([split1, split2]).split()

    assert len(res) == 2
    (r0, r1) = res["sim.0"].df(), res["sim.1"].df()
    assert len(r0) == 3
    assert len(r1) == 4

    pd.testing.assert_series_equal(
        r0.index.to_series(),
        pd.Series(range(3)),
        check_index=False,
        check_names=False,
    )
    pd.testing.assert_series_equal(
        r1.index.to_series(),
        pd.Series(range(4)),
        check_index=False,
        check_names=False,
    )

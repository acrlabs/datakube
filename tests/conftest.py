import arrow
import pandas as pd
import pytest

DATA_COLS = ["values1", "values2"]


@pytest.fixture
def df() -> pd.DataFrame:
    return pd.DataFrame({
        "timestamp": [arrow.get(i).datetime for i in range(10, 20)],
        "values1": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        "values2": [-11, 14, 11, 12, 13, 16, 15, 16, 18, 20],
    })

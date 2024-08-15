import pytest

from datakube.plot_utils import _compute_extents
from tests.conftest import DATA_COLS


@pytest.mark.parametrize("stack", [True, False])
def test_compute_extents(df, stack):
    xmin, xmax, ymin, ymax = _compute_extents(df[DATA_COLS], stack=stack)
    assert xmin == 0
    assert xmax == 9
    assert ymin == -11
    assert ymax == 30 if stack else 20

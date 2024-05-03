import typing as T

import pandas as pd
from bokeh.models import ColumnDataSource
from bokeh.models import NumeralTickFormatter
from bokeh.palettes import tol
from bokeh.plotting import figure
from bokeh.plotting import show

from datakube.constants import NORM_TS_KEY

_PALETTE_NAME = "TolRainbow"
_MIN_PALETTE_SIZE = 3
_MAX_PALETTE_SIZE = 23


def new_figure() -> figure:
    p = figure(sizing_mode="stretch_width", height=600, tools="")
    p.xgrid.visible = False
    p.ygrid.visible = False
    p.axis.minor_tick_line_color = None
    p.xaxis.formatter = NumeralTickFormatter(format="00:00:00")
    p.y_range.start = 0  # type: ignore
    p.toolbar_location = None  # type: ignore

    return p


def plot_multiseries(df: pd.DataFrame, stack: bool = False) -> None:
    p = new_figure()
    src = ColumnDataSource(df)
    keys = list(df.columns)
    ncolors = min(max(len(keys), _MIN_PALETTE_SIZE), _MAX_PALETTE_SIZE)
    colors = tol[_PALETTE_NAME][ncolors][: len(keys)]

    if not stack:
        _add_ts_lines(src, keys, p, colors)
    else:
        p.vline_stack(keys, x=NORM_TS_KEY, color=colors, source=src)

    show(p)


# def plot_aggregated_timeseries(df: pd.DataFrame):
#     pass


def _add_ts_lines(src: ColumnDataSource, keys: T.List[str], p: figure, colors: T.Tuple[str, ...]) -> None:
    color_iter = iter(colors)
    for key in src.data.keys():
        if key == NORM_TS_KEY:
            continue

        p.line(x=NORM_TS_KEY, y=key, source=src, color=next(color_iter))

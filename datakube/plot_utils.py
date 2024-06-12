import typing as T

import numpy as np
import pandas as pd
from bokeh.layouts import gridplot
from bokeh.models import BoxZoomTool
from bokeh.models import ColumnDataSource
from bokeh.models import CrosshairTool
from bokeh.models import CustomJS
from bokeh.models import HoverTool
from bokeh.models import NumeralTickFormatter
from bokeh.models import Range1d
from bokeh.models import ResetTool
from bokeh.palettes import inferno
from bokeh.palettes import tol
from bokeh.plotting import figure
from bokeh.plotting import show

from datakube.df_utils import compute_extents

_PALETTE_NAME = "TolRainbow"
_MIN_PALETTE_SIZE = 3
_MAX_PALETTE_SIZE = 23
_MARGIN_FACTOR = 1.1

_HOVER_JS = """
export default (args, obj, data, context) => {
    const tooltips = [];
    const x = Math.floor(data.geometry.x);

    const seriesNames = Object.keys(args.source.data).filter(key => key !== 'PLACEHOLDER');
    for (let i = 0; i < seriesNames.length; i++) {
        const color = args.colors[i];
        const series = seriesNames[i]
        tooltips.push(`<strong style="color: ${color}">${series}</strong>: ${args.source.data[series][x]}`);
    }
    obj.tooltips = tooltips.join("<br>");
}
"""


def new_figure(h: int = 600) -> figure:
    p = figure(height=h, tools=[])
    p.xgrid.visible = False
    p.ygrid.visible = False
    p.axis.minor_tick_line_color = None

    return p


def plot_histogram(counts: np.ndarray, bins: np.ndarray) -> None:
    p = new_figure()
    p.y_range.start = 0  # type: ignore
    p.x_range.start = 0  # type: ignore
    p.xaxis.ticker = bins
    p.quad(top=counts, left=bins[:-1], right=bins[1:], bottom=0)
    show(p)


def plot_multiseries(dfs: T.Mapping[str, pd.DataFrame], *, stack: bool = False, ncols: int = 3) -> None:
    plots = []
    for title, df in dfs.items():
        p = new_figure()
        p.title.text = title  # type: ignore
        p.xaxis.formatter = NumeralTickFormatter(format="00:00:00")

        (xmin, xmax, ymin, ymax) = compute_extents(df, stack=stack)
        p.x_range = Range1d(xmin, xmax * 1.05)  # type: ignore
        p.y_range = Range1d(ymin, ymax * 1.05)  # type: ignore
        src = ColumnDataSource(df)

        keys = list(df.columns)
        ncolors = max(len(keys), _MIN_PALETTE_SIZE)
        if ncolors > _MAX_PALETTE_SIZE:
            colors = inferno(ncolors)
        else:
            colors = tol[_PALETTE_NAME][ncolors][: len(keys)]

        if not stack:
            _add_ts_lines(src, df.index.name, keys, p, colors)
        else:
            p.vline_stack(keys, x=df.index.name, color=colors, source=src)

        _setup_ts_tools(src, df.index.name, p, colors, xmin, xmax, ymin, ymax)
        plots.append(p)

    ncols = min(ncols, len(dfs))
    grid = gridplot(plots, ncols=ncols, sizing_mode="stretch_width")  # type: ignore

    show(grid)


def _add_ts_lines(src: ColumnDataSource, index: str, keys: T.List[str], p: figure, colors: T.Tuple[str, ...]) -> None:
    color_iter = iter(colors)
    for key in keys:
        p.line(x=index, y=key, source=src, color=next(color_iter))


def _setup_ts_tools(
    src: ColumnDataSource,
    index: str,
    p: figure,
    colors: T.Tuple[str, ...],
    xmin: float,
    xmax: float,
    ymin: float,
    ymax: float,
) -> None:
    # The crosshair tool displays the cursor and a nice vertical line from top to bottom
    cursor = CrosshairTool(dimensions="height", line_alpha=0.5)

    # The hover tool displays the tooltip, with a custom callback to set the text to the current y-value for all series
    # at the current x-location
    #
    # visible = False hides the hover tool icons (https://github.com/bokeh/bokeh/pull/6380, toggleable is deprecated)
    hover = HoverTool(tooltips=None, point_policy="follow_mouse", visible=False)
    callback = CustomJS(args=dict(source=src, colors=colors), code=_HOVER_JS.replace("PLACEHOLDER", index))
    hover.callback = callback  # type: ignore

    # This nice little hack draws an invisible box over the entire chart so that the hover tool is always active,
    # instead of just when it's hovering over a line
    xmargin = xmax * _MARGIN_FACTOR
    ymargin = ymax * _MARGIN_FACTOR
    q = p.quad(left=xmin - ymargin, right=xmax + xmargin, bottom=ymin - ymargin, top=ymax + ymargin, alpha=0)
    hover.renderers = [q]  # type: ignore

    # The box zoom tool lets you zoom in to a particular time window, a la grafana
    zoom = BoxZoomTool(dimensions="width", visible=False)

    p.tools.extend([hover, cursor, zoom, ResetTool()])

    # This horrendous hack syncs the state of the cursor tool and the hover/zoom tools whenever the mouse enters the
    # canvas, because there doesn't seem to be any other way to do it.
    p.js_on_event(
        "mouseenter",
        CustomJS(
            args=dict(h=hover, c=cursor, z=zoom),
            code="h.active = c.active; z.active = c.active;",
        ),
    )

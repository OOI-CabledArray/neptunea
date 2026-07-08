"""RCA QC Explorer — interactive companion to the rca-data-tools QAQC pipeline.

Four tabs:
  1. Curated Products — verify CRESST curated zarr products (profiler heatmaps,
     fixed-mooring time series) with lock-on hover and full-res zoom.
  2. Full Time Series — any stream from the ooi-data bucket (menu built from
     rca-data-tools sitesDictionary.csv), datashaded, with a toggleable HITL
     annotation overlay fetched live from the OOI M2M API. Zooming below
     ~100k points in view materializes real, hoverable data points.
  3. Time Machine — one year vs selected prior years on the same axis
     (rca-data-tools time-machine mechanic). 1D sources only: curated
     fixed-mooring products and decimated ooi-data streams.
  4. Annotations — searchable HITL annotation table per reference designator.

Plot standards (colormaps, ranges, labels) come from rca-data-tools
params/variableParameters.csv; heatmaps use profileMin/profileMax, 1D uses
min/max. Requires OOI_USERNAME / OOI_TOKEN in the environment (or .env) for
annotation features.

    qc-explorer                               # console entry point
    panel serve rca_panel_app/qc_explorer.py  # equivalent
"""
import panel as pn
pn.extension('tabulator', loading_spinner='dots')

import os
import asyncio
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import s3fs
import requests
import numpy as np
import pandas as pd
import xarray as xr
import holoviews as hv
import hvplot.xarray
import cmocean
from bokeh.models import DatetimeTickFormatter, DatetimeTicker, HoverTool

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

CURATED_PREFIX = "rca-advanced-qaqc/cresst"
OOI_DATA_BUCKET = "ooi-data"
ANNO_URL = "https://ooinet.oceanobservatories.org/api/m2m/12580/anno/find"
OOI_AUTH = (os.getenv("OOI_USERNAME", ""), os.getenv("OOI_TOKEN", ""))

PARAMS_LOCAL = Path.home() / "repos/rca-data-tools/rca_data_tools/qaqc/params"
PARAMS_URL = "https://raw.githubusercontent.com/OOI-CabledArray/rca-data-tools/main/rca_data_tools/qaqc/params"

DATETIME_OPTS = dict(
    xformatter=DatetimeTickFormatter(months="%b %Y", years="%Y", days="%d %b"),
    xticks=DatetimeTicker(desired_num_ticks=12),
)
CMAP_OPTIONS = sorted(m for m in cmocean.cm.cmap_d if not m.endswith("_r"))
# rca-data-tools time-machine palette, indexed by years-back
TM_COLORS = ["#1f78b4", "#a6cee3", "#b2df8a", "#33a02c", "#fb9a99", "#e31a1c",
             "#fdbf6f", "#ff7f00", "#cab2d6", "#6a3ba2", "#8c564b", "#7f7f7f"]

fs = s3fs.S3FileSystem(anon=False)
EXECUTOR = ThreadPoolExecutor(max_workers=4)
DETAIL_THRESHOLD = 100_000  # max points to materialize for hoverable zoom detail
BIG_STREAM = 50_000_000     # warn above this many points


# --- Plot standards from rca-data-tools params ---
@pn.cache
def load_param_csv(name):
    local = PARAMS_LOCAL / name
    return pd.read_csv(local if local.exists() else f"{PARAMS_URL}/{name}")


def split_list(cell):
    """params CSV list cells parse with literal quotes attached, e.g. '\"a,b,c\"'."""
    return cell.strip('"').split(",")


VARIABLE_PARAMS = load_param_csv("variableParameters.csv").set_index("variable")
VAR_TO_PARAM = {
    v: row.parameter
    for _, row in load_param_csv("variableMap.csv").iterrows()
    for v in split_list(row.variableNames)
}
SITES_DICT = {
    row.refDes: dict(zarr=row.zarrFile, params=[p for p in split_list(row.dataParameters) if p != "time"])
    for _, row in load_param_csv("sitesDictionary.csv").dropna(subset=["zarrFile", "dataParameters"]).iterrows()
}


def var_style(var):
    """Standard cmap/clim/ylim/label for an xarray variable name."""
    param = VAR_TO_PARAM.get(var)
    if param not in VARIABLE_PARAMS.index:
        return dict(cmap="thermal", clim=None, ylim=None, label=var)
    r = VARIABLE_PARAMS.loc[param]
    return dict(
        cmap=r.colorMap,
        clim=(float(r.profileMin), float(r.profileMax)),
        ylim=(float(r["min"]), float(r["max"])),
        label=f"{param} ({r.label})",
    )


# --- Data access ---
@pn.cache
def discover_curated():
    paths = sorted(fs.glob(f"{CURATED_PREFIX}/*.zarr") + fs.glob(f"{CURATED_PREFIX}/*/*.zarr"))
    return {p.removeprefix(f"{CURATED_PREFIX}/"): p for p in paths}


@pn.cache
def get_dataset(s3_path):
    ds = xr.open_zarr(fs.get_mapper(s3_path), consolidated=True)
    if "profile_number" in ds.dims:
        valid = ~np.isnat(ds.peak_time.values)
        ds = ds.isel(profile_number=valid).sortby("peak_time")
        ds = ds.swap_dims({"profile_number": "peak_time"})
    return ds


@pn.cache(max_items=12, policy="LRU")
def load_var(s3_path, var):
    """Curated variables are loaded into memory once so zoom re-rendering
    aggregates from RAM instead of re-reading chunks from S3."""
    return get_dataset(s3_path)[var].load()


def time_dim(obj):
    return "peak_time" if "peak_time" in obj.dims else "time"


@pn.cache
def fetch_annotations(refdes):
    if not OOI_AUTH[0]:
        raise RuntimeError("Set OOI_USERNAME and OOI_TOKEN (env or .env) for annotations")
    resp = requests.get(ANNO_URL, params={"refdes": refdes}, auth=OOI_AUTH, timeout=30)
    resp.raise_for_status()
    df = pd.DataFrame(resp.json())
    if df.empty:
        return df
    df["beginDT"] = pd.to_datetime(df["beginDT"], unit="ms")
    df["endDT"] = pd.to_datetime(df["endDT"], unit="ms")
    return df.sort_values("beginDT", ignore_index=True)


def annotation_overlay(refdes):
    df = fetch_annotations(refdes)
    if df.empty:
        return hv.VSpans(([], [], []), vdims=["annotation"])
    end = df["endDT"].fillna(pd.Timestamp.now())
    text = df["annotation"].str.slice(0, 150)
    return hv.VSpans((df["beginDT"], end, text), vdims=["annotation"]).opts(
        color="red", alpha=0.12, tools=["hover"]
    )


# --- Plot builders ---
def heatmap(da, tdim, style, cmap_name):
    label = style["label"]
    hover = HoverTool(
        tooltips=[("time", "$x{%F %H:%M}"), ("pressure", "$y{0} dbar"), (label, "@image")],
        formatters={"$x": "datetime"},
    )
    return da.hvplot.quadmesh(
        x=tdim, y="sea_water_pressure",
        cmap=cmocean.cm.cmap_d[cmap_name],
        rasterize=True, colorbar=True, clabel=label,
        xlabel="Time", ylabel="Pressure (dbar)",
        min_height=350, responsive=True,
    ).opts(
        invert_yaxis=True, clim=style["clim"] or (None, None),
        tools=[hover], shared_axes=False, **DATETIME_OPTS,
    )


def scatter_1d(da, tdim, style):
    label = style["label"]
    hover = HoverTool(
        tooltips=[("time", f"@{tdim}{{%F %H:%M}}"), (label, f"@{{{da.name}}}")],
        formatters={f"@{tdim}": "datetime"},
    )
    return da.hvplot.scatter(
        x=tdim, xlabel="Time", ylabel=label, size=4, grid=True,
        min_height=300, responsive=True,
    ).opts(tools=[hover], shared_axes=False, **DATETIME_OPTS)


def _to_timestamp(v):
    return pd.Timestamp(v, unit="ms") if isinstance(v, (int, float)) else pd.Timestamp(v)


def detail_layer(da, style):
    """Real, hoverable points that appear once the zoom window holds fewer
    than DETAIL_THRESHOLD samples; empty above that so cost stays bounded."""
    idx = da.indexes["time"]
    label = style["label"]
    hover = HoverTool(
        tooltips=[("time", "@time{%F %H:%M:%S}"), (label, "@value")],
        formatters={"@time": "datetime"},
    )

    def points(x_range, y_range):
        sl = da.isel(time=slice(0, 0))
        if x_range is not None:
            lo = idx.searchsorted(_to_timestamp(x_range[0]))
            hi = idx.searchsorted(_to_timestamp(x_range[1]))
            if 0 < hi - lo <= DETAIL_THRESHOLD:
                sl = da.isel(time=slice(lo, hi)).load()
        return hv.Scatter((sl["time"].values, sl.values), "time", "value").opts(
            color="crimson", size=5, tools=[hover])

    return hv.DynamicMap(points, streams=[hv.streams.RangeXY()])


def stream_plot(da, style, ylim):
    shaded = da.hvplot.scatter(
        x="time", datashade=True, xlabel="Time", ylabel=style["label"],
        ylim=ylim, grid=True, min_height=350, responsive=True,
    )
    return (shaded * detail_layer(da, style)).opts(shared_axes=False, **DATETIME_OPTS)


# --- Panel scaffolding ---
class PanelBase:
    """Selector row + plot pane + error alert, removable from its column."""

    def __init__(self, on_remove):
        self.loading = pn.indicators.LoadingSpinner(value=False, width=30, height=30)
        self.remove_btn = pn.widgets.Button(name="✕", button_type="danger", width=40)
        self.remove_btn.on_click(lambda e: on_remove(self))
        self.plot_pane = pn.pane.HoloViews(None, sizing_mode="stretch_width")
        self.error_pane = pn.pane.Alert("", alert_type="danger", visible=False)

    def build_layout(self, *widgets):
        return pn.Column(
            pn.Row(*widgets, self.loading, self.remove_btn, align="center"),
            self.error_pane,
            self.plot_pane,
            styles={"border": "1px solid #ddd", "border-radius": "8px",
                    "padding": "10px", "margin": "5px"},
            sizing_mode="stretch_width",
        )

    def run(self, work, apply=None):
        """Run blocking `work` in a thread so the UI keeps repainting (loading
        overlay, spinner). Its result is applied on the event loop: via
        `apply(result)` when given, else assigned to the plot pane."""
        self.loading.value = True
        self.plot_pane.loading = True
        self.error_pane.visible = False

        async def task():
            try:
                result = await asyncio.get_running_loop().run_in_executor(EXECUTOR, work)
                if apply is not None:
                    apply(result)
                elif result is not None:
                    self.plot_pane.object = result
            except Exception as e:
                print(f"[qc-explorer] {traceback.format_exc()}")
                self.error_pane.object = f"**Error:** `{type(e).__name__}: {e}`"
                self.error_pane.visible = True
            finally:
                self.loading.value = False
                self.plot_pane.loading = False

        pn.state.execute(task)


class CuratedPanel(PanelBase):
    """Tab 1: one curated product + variable + colormap."""

    def __init__(self, on_remove):
        super().__init__(on_remove)
        datasets = discover_curated()
        self.dataset_select = pn.widgets.Select(
            name="Dataset", options=["-- Select --"] + list(datasets), width=480)
        self.var_select = pn.widgets.Select(name="Variable", options=[], width=260)
        self.cmap_select = pn.widgets.Select(name="Colormap", options=CMAP_OPTIONS, width=130)
        self.dataset_select.param.watch(self._dataset_changed, "value")
        self.var_select.param.watch(self._var_changed, "value")
        self.cmap_select.param.watch(lambda e: self.run(self._build, apply=self._show), "value")
        self.layout = self.build_layout(self.dataset_select, self.var_select, self.cmap_select)

    def _dataset_changed(self, event):
        datasets = discover_curated()
        if event.new not in datasets:
            self.var_select.options = []
            self.plot_pane.object = None
            return
        def set_vars(names):
            self.var_select.options = names
            self.var_select.value = names[0]
        self.run(lambda: list(get_dataset(datasets[event.new]).data_vars), apply=set_vars)

    def _var_changed(self, event):
        if not event.new:
            return
        # reset colormap to this variable's standard; the cmap watcher replots
        standard = var_style(event.new)["cmap"]
        if self.cmap_select.value != standard:
            self.cmap_select.value = standard
        else:
            self.run(self._build, apply=self._show)

    def _build(self):
        datasets = discover_curated()
        if self.dataset_select.value not in datasets or not self.var_select.value:
            return None
        var = self.var_select.value
        da = load_var(datasets[self.dataset_select.value], var)
        style = var_style(var)
        tdim = time_dim(da)
        if da.ndim == 2:
            return heatmap(da, tdim, style, self.cmap_select.value), True
        return scatter_1d(da, tdim, style), False

    def _show(self, result):
        if result is None:
            return
        plot, is_2d = result
        self.cmap_select.disabled = not is_2d
        self.plot_pane.object = plot


class StreamPanel(PanelBase):
    """Tab 2: full-resolution ooi-data stream + annotation overlay."""

    def __init__(self, on_remove):
        super().__init__(on_remove)
        groups = {}
        for refdes in SITES_DICT:
            groups.setdefault(refdes.split("-")[0], []).append(refdes)
        self.refdes_select = pn.widgets.Select(
            name="Instrument", groups={"": ["-- Select --"], **groups}, width=340)
        self.var_select = pn.widgets.Select(name="Variable", options=[], width=280)
        self.anno_toggle = pn.widgets.Checkbox(name="Annotations", value=False)
        self.yrange_radio = pn.widgets.RadioButtonGroup(
            options=["Auto", "Standard"], value="Auto", width=150)
        self.info_pane = pn.pane.Alert("", alert_type="primary", visible=False)
        self.refdes_select.param.watch(self._refdes_changed, "value")
        for w in (self.var_select, self.anno_toggle, self.yrange_radio):
            w.param.watch(lambda e: self._plot(), "value")
        self.layout = self.build_layout(
            self.refdes_select, self.var_select, self.yrange_radio, self.anno_toggle)
        self.layout.insert(1, self.info_pane)

    def _refdes_changed(self, event):
        if event.new not in SITES_DICT:
            self.var_select.options = []
            self.plot_pane.object = None
            self.info_pane.visible = False
            return
        def set_vars(options):
            self.var_select.options = options
            self.var_select.value = options[0] if options else None
        def go():
            info = SITES_DICT[event.new]
            ds = get_dataset(f"{OOI_DATA_BUCKET}/{info['zarr']}")
            return [v for v in info["params"] if v in ds]
        self.run(go, apply=set_vars)

    def _plot(self):
        refdes = self.refdes_select.value
        if refdes not in SITES_DICT or not self.var_select.value:
            return
        var = self.var_select.value

        def build():
            ds = get_dataset(f"{OOI_DATA_BUCKET}/{SITES_DICT[refdes]['zarr']}")
            style = var_style(var)
            ylim = style["ylim"] if self.yrange_radio.value == "Standard" else None
            plot = stream_plot(ds[var], style, ylim)
            if self.anno_toggle.value:
                plot = plot * annotation_overlay(refdes)
            return plot, ds[var].sizes["time"]

        def show(result):
            plot, n = result
            note = f"**{n/1e6:.0f}M points** — zoom below {DETAIL_THRESHOLD:,} points in view for value hover."
            if n > BIG_STREAM:
                note += " Large stream: full-range rendering scans every chunk from S3 and can take minutes."
            self.info_pane.object = note
            self.info_pane.visible = True
            self.plot_pane.object = plot

        self.info_pane.object = "Loading stream from S3 — large datasets can take a while on first render…"
        self.info_pane.visible = True
        self.run(build, apply=show)


@pn.cache(max_items=24, policy="LRU")
def load_year(kind, key, var, year):
    """One year of a 1D series, in memory. Streams are stride-decimated to
    ~200k points so multi-year overlays stay light."""
    if kind == "curated":
        return load_var(key, var).sel(time=str(year))
    da = get_dataset(f"{OOI_DATA_BUCKET}/{SITES_DICT[key]['zarr']}")[var].sel(time=str(year))
    stride = max(1, da.sizes["time"] // 200_000)
    return da.isel(time=slice(None, None, stride)).load()


class TimeMachinePanel(PanelBase):
    """Tab 3: one year vs selected prior years. Sources are 1D series only:
    curated fixed-mooring products and (decimated) full ooi-data streams."""

    def __init__(self):
        super().__init__(on_remove=lambda p: None)
        self.remove_btn.visible = False
        fixed = [k for k in discover_curated() if "_profiles_" not in k]
        self.sources = {k: ("curated", discover_curated()[k]) for k in fixed}
        self.sources.update({r: ("stream", r) for r in SITES_DICT})
        self.dataset_select = pn.widgets.Select(
            name="Dataset", width=480,
            groups={"": ["-- Select --"], "Curated (fixed)": fixed,
                    "OOI streams (decimated)": list(SITES_DICT)})
        self.var_select = pn.widgets.Select(name="Variable", options=[], width=280)
        self.year_select = pn.widgets.Select(name="Year", options=[], width=100)
        self.compare_select = pn.widgets.MultiChoice(
            name="Compare with", options=[], width=320)
        self.dataset_select.param.watch(self._dataset_changed, "value")
        self.var_select.param.watch(lambda e: self._years_then_plot(), "value")
        for w in (self.year_select, self.compare_select):
            w.param.watch(lambda e: self.run(self._build), "value")
        self.layout = self.build_layout(
            self.dataset_select, self.var_select, self.year_select, self.compare_select)

    def _open(self):
        kind, key = self.sources[self.dataset_select.value]
        if kind == "curated":
            return kind, key, get_dataset(key)
        return kind, key, get_dataset(f"{OOI_DATA_BUCKET}/{SITES_DICT[key]['zarr']}")

    def _dataset_changed(self, event):
        if event.new not in self.sources:
            self.plot_pane.object = None
            return
        def set_vars(names):
            self.var_select.options = names
            self.var_select.value = names[0] if names else None
        def go():
            kind, key, ds = self._open()
            if kind == "stream":
                return [v for v in SITES_DICT[key]["params"] if v in ds]
            return list(ds.data_vars)
        self.run(go, apply=set_vars)

    def _years_then_plot(self):
        if not self.var_select.value:
            return
        def set_years(years):
            self.year_select.options = years
            self.compare_select.options = years
            if self.year_select.value == years[0]:
                self.run(self._build)  # watcher won't fire on same value
            else:
                self.year_select.value = years[0]
        def go():
            idx = self._open()[2].indexes["time"]
            return list(range(idx[-1].year, idx[0].year - 1, -1))
        self.run(go, apply=set_years)

    def _build(self):
        ref = self.year_select.value
        if self.dataset_select.value not in self.sources or ref is None:
            return None
        kind, key = self.sources[self.dataset_select.value]
        var = self.var_select.value
        style = var_style(var)
        curves = []
        compare = [y for y in self.compare_select.value if y != ref]
        for yr in [ref] + sorted(compare, reverse=True):
            sl = load_year(kind, key, var, yr).dropna("time")
            if not sl.time.size:
                continue
            # rca-data-tools mechanic: slide prior years forward onto the ref-year axis
            shifted = sl.assign_coords(time=sl.time + np.timedelta64((ref - yr) * 365, "D"))
            color = TM_COLORS[(ref - yr) % len(TM_COLORS)]
            curves.append(shifted.hvplot.scatter(
                x="time", size=3, color=color, label=str(yr),
                xlabel=f"Time ({ref})", ylabel=style["label"],
                min_height=400, responsive=True,
            ))
        if not curves:
            return None
        return hv.Overlay(curves).opts(
            shared_axes=False, legend_position="top_right", **DATETIME_OPTS)


class AnnotationBrowser:
    """Tab 4: searchable annotation table per reference designator."""

    def __init__(self):
        groups = {}
        for refdes in SITES_DICT:
            groups.setdefault(refdes.split("-")[0], []).append(refdes)
        self.refdes_select = pn.widgets.Select(
            name="Instrument", groups={"": ["-- Select --"], **groups}, width=340)
        self.count = pn.pane.Markdown("")
        self.error_pane = pn.pane.Alert("", alert_type="danger", visible=False)
        self.table = pn.widgets.Tabulator(
            pd.DataFrame(), header_filters=True, pagination="local", page_size=25,
            layout="fit_data_stretch", show_index=False, disabled=True,
            sizing_mode="stretch_width",
        )
        self.refdes_select.param.watch(self._load, "value")
        self.layout = pn.Column(
            pn.Row(self.refdes_select, self.count, align="center"),
            self.error_pane, self.table, sizing_mode="stretch_width")

    def _load(self, event):
        self.error_pane.visible = False
        if event.new not in SITES_DICT:
            return
        try:
            df = fetch_annotations(event.new)
            cols = [c for c in ["beginDT", "endDT", "annotation", "qcFlag", "source"] if c in df]
            self.table.value = df[cols] if not df.empty else pd.DataFrame()
            self.count.object = f"**{len(df)} annotations**"
        except Exception as e:
            print(f"[qc-explorer] {traceback.format_exc()}")
            self.error_pane.object = f"**Error:** `{type(e).__name__}: {e}`"
            self.error_pane.visible = True


def multi_panel_tab(panel_cls):
    """Column of removable panels with an add button."""
    column = pn.Column(sizing_mode="stretch_width")
    def add(event=None):
        column.append(panel_cls(on_remove=lambda p: column.remove(p.layout)).layout)
    btn = pn.widgets.Button(name="+ Add panel", button_type="primary", width=150)
    btn.on_click(add)
    add()
    return pn.Column(btn, column, sizing_mode="stretch_width")


tabs = pn.Tabs(
    ("Curated Products", multi_panel_tab(CuratedPanel)),
    ("Full Time Series", multi_panel_tab(StreamPanel)),
    ("Time Machine", TimeMachinePanel().layout),
    ("Annotations", AnnotationBrowser().layout),
    dynamic=True,
    sizing_mode="stretch_width",
)

template = pn.template.FastListTemplate(
    title="RCA QC Explorer",
    main=[tabs],
    accent_base_color="#1a6b8a",
    header_background="#1a6b8a",
)
template.servable()

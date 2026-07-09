"""RCA QC Explorer — interactive companion to the rca-data-tools QAQC pipeline.

Four tabs:
  1. Curated Products — verify CRESST curated zarr products (profiler heatmaps,
     fixed-mooring time series) with lock-on hover and full-res zoom.
  2. Full Time Series — any stream from the ooi-data bucket (menu from
     rca-data-tools sitesDictionary.csv). Three-tier rendering: cached
     full-range overview, windowed re-aggregation on zoom, individual
     points once sparse. Hover works at every zoom level. Toggleable
     overlays: HITL annotations, deployment markers, QARTOD flags,
     climatology envelope. Drag an x-box-select to draft an M2M annotation.
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
import ast
import json
import asyncio
import traceback
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from pathlib import Path

import s3fs
import requests
import numpy as np
import pandas as pd
import xarray as xr
import holoviews as hv
import hvplot.xarray
import cmocean
from holoviews.operation.datashader import rasterize
from bokeh.models import DatetimeTickFormatter, DatetimeTicker, HoverTool

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

CURATED_PREFIX = "rca-advanced-qaqc/cresst"
OOI_DATA_BUCKET = "ooi-data"
ANNO_URL = "https://ooinet.oceanobservatories.org/api/m2m/12580/anno/find"
DEPLOY_URL = "https://ooinet.oceanobservatories.org/api/m2m/12587/events/deployment/query"
OOI_AUTH = (os.getenv("OOI_USERNAME", ""), os.getenv("OOI_TOKEN", ""))

QC_LOOKUP = Path.home() / "repos/qc-lookup/qartod"

# CTD refdes per curated product prefix (for deployment markers); longest match wins
CURATED_REFDES = {
    "axial_base_deep":      "RS03AXPD-DP03A-01-CTDPFL304",
    "axial_base":           "RS03AXPS-SF03A-2A-CTDPFA302",
    "slope_base_deep":      "RS01SBPD-DP01A-01-CTDPFL104",
    "slope_base":           "RS01SBPS-SF01A-2A-CTDPFA102",
    "oregon_offshore_deep": "CE04OSPD-DP01B-01-CTDPFL105",
    "oregon_offshore":      "CE04OSPS-SF01B-2A-CTDPFA107",
}

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
# The rasterized image carries hover (time / value / count) at every zoom
# level; individual points are only drawn once sparse enough to distinguish.
DETAIL_THRESHOLD = 2_000
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
    # chunks={} keeps arrays dask-backed (zarr chunking) so stream aggregation
    # runs partition-wise instead of loading whole variables into memory
    ds = xr.open_zarr(fs.get_mapper(s3_path), consolidated=True, chunks={})
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


@pn.cache
def fetch_deployments(refdes):
    if not OOI_AUTH[0]:
        raise RuntimeError("Set OOI_USERNAME and OOI_TOKEN (env or .env) for deployments")
    resp = requests.get(DEPLOY_URL, params={"refdes": refdes}, auth=OOI_AUTH, timeout=30)
    resp.raise_for_status()
    return sorted(
        (pd.Timestamp(d["eventStartTime"], unit="ms"), d["deploymentNumber"])
        for d in resp.json() if d.get("eventStartTime")
    )


def deployment_overlay(refdes):
    deps = fetch_deployments(refdes)
    return hv.VLines(
        ([t for t, _ in deps], [f"Deploy {n}: {t:%Y-%m-%d}" for t, n in deps]),
        vdims=["deployment"],
    ).opts(line_color="gray", line_dash="dashed", line_width=1, tools=["hover"])


def curated_refdes(dataset_name):
    for prefix, refdes in CURATED_REFDES.items():
        if dataset_name.startswith(prefix):
            return refdes
    return None


def mask_gaps(da, tdim, factor=3.0):
    """Insert NaN columns inside sampling gaps so quadmesh cells don't
    stretch across them and read as (false) continuous data."""
    t = da[tdim].values
    if t.size < 3:
        return da
    dt = np.diff(t)
    med = np.median(dt)
    gaps = np.where(dt > factor * med)[0]
    if not gaps.size:
        return da
    fill = np.concatenate([t[gaps] + med, t[gaps + 1] - med])
    return da.reindex({tdim: np.union1d(t, fill)})


@pn.cache
def climatology_envelope(refdes, var):
    """Monthly (min, max) from the qc-lookup QARTOD climatology table for this
    refdes/variable, enveloped across pressure bins. None if no table."""
    candidates = {var, var.removeprefix("sea_water_"),
                  var.replace("sea_water_", "seawater_"),
                  var.replace("sea_water_electrical_", "seawater_")}
    table = next((f for f in QC_LOOKUP.glob(f"*/climatology_tables/{refdes}-*.csv")
                  if f.stem.removeprefix(f"{refdes}-") in candidates), None)
    if table is None:
        return None
    df = pd.read_csv(table, index_col=0)
    parsed = df.map(ast.literal_eval)  # cells are "[min, max]"
    mins = parsed.map(lambda v: v[0]).min(axis=0).values
    maxs = parsed.map(lambda v: v[1]).max(axis=0).values
    return mins, maxs


def climatology_overlay(refdes, var, t0, t1):
    env = climatology_envelope(refdes, var)
    if env is None:
        return None
    mins, maxs = env
    starts = pd.date_range(pd.Timestamp(t0).to_period("M").to_timestamp(), t1, freq="MS")
    xs, lo, hi = [], [], []
    for s in starts:
        e = s + pd.offsets.MonthBegin(1)
        xs += [s, e]
        lo += [mins[s.month - 1]] * 2
        hi += [maxs[s.month - 1]] * 2
    return hv.Area((xs, lo, hi), vdims=["low", "high"]).opts(
        fill_color="seagreen", fill_alpha=0.15, line_alpha=0)


# --- Plot builders ---
def _clamp(rng, lo, hi, to_dt=False):
    """Clamp a requested axis range to the data extent. Bokeh can request
    ranges far outside the data (deep zoom-out, inverted axes report
    reversed), which makes the quadmesh rasterizer produce an Image whose
    bounds fail validation — the long-standing blank-heatmap bug."""
    if rng is None:
        return None
    a, b = sorted(np.datetime64(_to_timestamp(v)) if to_dt else v for v in rng)
    a, b = max(a, lo), min(b, hi)
    return (a, b) if a < b else (lo, hi)


def heatmap(da, tdim, style, cmap_name):
    label = style["label"]
    hover = HoverTool(
        tooltips=[("time", "$x{%F %H:%M}"), ("pressure", "$y{0} dbar"), (label, "@image")],
        formatters={"$x": "datetime"},
    )
    opts = dict(
        cmap=cmocean.cm.cmap_d[cmap_name], colorbar=True, clabel=label,
        clim=style["clim"] or (None, None), invert_yaxis=True,
        xlabel="Time", ylabel="Pressure (dbar)", tools=[hover],
        shared_axes=False, responsive=True, min_height=350, **DATETIME_OPTS,
    )
    qm = hv.QuadMesh(da, kdims=[tdim, "sea_water_pressure"])
    t = da[tdim].values
    p = da["sea_water_pressure"].values
    x0, x1 = t[0], t[-1]
    y0, y1 = float(p.min()), float(p.max())

    def view(x_range=None, y_range=None, width=None, height=None, scale=1.0):
        return rasterize(
            qm,
            x_range=_clamp(x_range, x0, x1, to_dt=True),
            y_range=_clamp(y_range, y0, y1),
            width=int(min(max(width or 1200, 200), 4000)),
            height=int(min(max(height or 400, 150), 2000)),
            dynamic=False,
        ).opts(**opts)

    return hv.DynamicMap(view, streams=[hv.streams.RangeXY(), hv.streams.PlotSize()])


def scatter_1d(da, tdim, style):
    label = style["label"]
    hover = HoverTool(
        tooltips=[("time", f"@{tdim}{{%F %H:%M}}"), (label, f"@{{{da.name}}}")],
        formatters={f"@{tdim}": "datetime"},
    )
    return da.hvplot.scatter(
        x=tdim, xlabel="Time", ylabel=label, size=1, grid=True,
        min_height=300, responsive=True,
    ).opts(tools=[hover], shared_axes=False, **DATETIME_OPTS)


def _to_timestamp(v):
    return pd.Timestamp(v, unit="ms") if isinstance(v, (int, float)) else pd.Timestamp(v)


# --- Stream rendering: cached full-range overview + windowed re-aggregation.
# The expensive full scan happens once per (refdes, var, flag) in a worker
# thread; pan/zoom at coarse level redraws the cached image instantly, mid
# zooms re-aggregate only the visible window (index-pruned S3 reads), and
# below DETAIL_THRESHOLD real hoverable points appear.
MID_CAP = 10_000_000  # max points to window-aggregate on the fly

STREAM_LAYERS = {  # QARTOD rollup flag value -> image style
    None: dict(cmap="kbc_r", cnorm="eq_hist"),
    "3":  dict(cmap=["#ff8c00"]),
    "4":  dict(cmap=["#d62728"]),
}


def _stream_path(refdes):
    return f"{OOI_DATA_BUCKET}/{SITES_DICT[refdes]['zarr']}"


def flag_col(ds, var):
    col = f"{var}_qartod_results"
    return col if col in ds else None


def _rasterize(data, var, x_range=None, y_range=None):
    el = hv.Scatter(data, "time", var)
    return rasterize(el, x_range=x_range, y_range=y_range, dynamic=False,
                     width=1200, height=400)


@pn.cache(max_items=12, policy="LRU")
def stream_overview(refdes, var, flag):
    ds = get_dataset(_stream_path(refdes))
    fcol = flag_col(ds, var)
    ddf = ds[[var] + ([fcol] if fcol else [])].unify_chunks().to_dask_dataframe()
    if flag:
        ddf = ddf[ddf[fcol].astype(str).str.strip() == flag]
    return _rasterize(ddf, var)


@lru_cache(maxsize=4)
def load_slice(refdes, var, lo, hi):
    ds = get_dataset(_stream_path(refdes))
    fcol = flag_col(ds, var)
    return ds[[var] + ([fcol] if fcol else [])].isel(time=slice(lo, hi)).load() \
        .to_dataframe().reset_index()


def stream_layer(refdes, var, flag, label):
    ds = get_dataset(_stream_path(refdes))
    idx, fcol = ds.indexes["time"], flag_col(ds, var)
    hover = HoverTool(
        tooltips=[("time", "$x{%F %H:%M}"), (label, "$y{0.000}"), ("count", "@image")],
        formatters={"$x": "datetime"},
    )
    opts = dict(nodata=0, xlabel="Time", ylabel=label,
                tools=[hover] if flag is None else [], **STREAM_LAYERS[flag])

    def view(x_range, y_range):
        if x_range is not None:
            lo = idx.searchsorted(_to_timestamp(x_range[0]))
            hi = idx.searchsorted(_to_timestamp(x_range[1]))
            if hi - lo <= MID_CAP:
                df = load_slice(refdes, var, int(lo), int(hi))
                if flag:
                    df = df[df[fcol].astype(str).str.strip() == flag]
                xr_ = tuple(np.datetime64(_to_timestamp(v)) for v in x_range)
                yr_ = tuple(y_range) if y_range is not None else None
                if len(df):
                    return _rasterize(df, var, x_range=xr_, y_range=yr_).opts(**opts)
                y0, y1 = yr_ or (0.0, 1.0)
                return hv.Image(([xr_[0], xr_[1]], [y0, y1],
                                 np.full((2, 2), np.nan)), ["time", var]).opts(**opts)
        return stream_overview(refdes, var, flag).opts(**opts)

    return hv.DynamicMap(view, streams=[hv.streams.RangeXY()])


def detail_layer(refdes, var, label):
    """Real, hoverable points once the window holds < DETAIL_THRESHOLD samples."""
    idx = get_dataset(_stream_path(refdes)).indexes["time"]
    hover = HoverTool(
        tooltips=[("time", "@time{%F %H:%M:%S}"), (label, "@value")],
        formatters={"@time": "datetime"},
    )

    def points(x_range, y_range):
        data = ([], [])
        if x_range is not None:
            lo = idx.searchsorted(_to_timestamp(x_range[0]))
            hi = idx.searchsorted(_to_timestamp(x_range[1]))
            if 0 < hi - lo <= DETAIL_THRESHOLD:
                df = load_slice(refdes, var, int(lo), int(hi))
                data = (df["time"].values, df[var].values)
        return hv.Scatter(data, "time", "value").opts(
            color="crimson", size=5, tools=[hover])

    return hv.DynamicMap(points, streams=[hv.streams.RangeXY()])


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
        self.mask_toggle = pn.widgets.Checkbox(name="Mask gaps", value=True)
        self.deploy_toggle = pn.widgets.Checkbox(name="Deployments", value=False)
        self.dataset_select.param.watch(self._dataset_changed, "value")
        self.var_select.param.watch(self._var_changed, "value")
        for w in (self.cmap_select, self.mask_toggle, self.deploy_toggle):
            w.param.watch(lambda e: self.run(self._build, apply=self._show), "value")
        self.layout = self.build_layout(
            self.dataset_select, self.var_select, self.cmap_select,
            self.mask_toggle, self.deploy_toggle)

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
        name = self.dataset_select.value
        if name not in datasets or not self.var_select.value:
            return None
        var = self.var_select.value
        da = load_var(datasets[name], var)
        style = var_style(var)
        tdim = time_dim(da)
        if da.ndim == 2:
            if self.mask_toggle.value:
                da = mask_gaps(da, tdim)
            plot, is_2d = heatmap(da, tdim, style, self.cmap_select.value), True
        else:
            plot, is_2d = scatter_1d(da, tdim, style), False
        refdes = curated_refdes(name)
        if self.deploy_toggle.value and refdes:
            plot = plot * deployment_overlay(refdes)
        return plot, is_2d

    def _show(self, result):
        if result is None:
            return
        plot, is_2d = result
        self.cmap_select.disabled = not is_2d
        self.mask_toggle.disabled = not is_2d
        self.plot_pane.object = plot


QC_FLAG_OPTIONS = ["suspect", "fail", "not_operational", "not_available", "pass"]


class StreamPanel(PanelBase):
    """Tab 2: full-resolution ooi-data stream with QARTOD flag, climatology,
    annotation, and deployment overlays, plus an annotation drafting tool."""

    def __init__(self, on_remove):
        super().__init__(on_remove)
        groups = {}
        for refdes in SITES_DICT:
            groups.setdefault(refdes.split("-")[0], []).append(refdes)
        self.refdes_select = pn.widgets.Select(
            name="Instrument", groups={"": ["-- Select --"], **groups}, width=340)
        self.var_select = pn.widgets.Select(name="Variable", options=[], width=280)
        self.yrange_radio = pn.widgets.RadioButtonGroup(
            options=["Standard", "Auto"], value="Standard", width=150)
        self.anno_toggle = pn.widgets.Checkbox(name="Annotations", value=False)
        self.deploy_toggle = pn.widgets.Checkbox(name="Deployments", value=True)
        self.flags_toggle = pn.widgets.Checkbox(name="QARTOD flags", value=False)
        self.clim_toggle = pn.widgets.Checkbox(name="Climatology", value=False)
        self.info_pane = pn.pane.Alert("", alert_type="primary", visible=False)

        self.refdes_select.param.watch(self._refdes_changed, "value")
        for w in (self.var_select, self.yrange_radio, self.anno_toggle,
                  self.deploy_toggle, self.flags_toggle, self.clim_toggle):
            w.param.watch(lambda e: self._plot(), "value")

        self._init_draft()
        self.layout = self.build_layout(
            self.refdes_select, self.var_select, self.yrange_radio)
        self.layout.insert(1, pn.Row(self.anno_toggle, self.deploy_toggle,
                                     self.flags_toggle, self.clim_toggle))
        self.layout.insert(2, self.info_pane)
        self.layout.append(self.draft_card)

    # -- annotation drafting --
    def _init_draft(self):
        self.draft_begin = pn.widgets.DatetimePicker(name="Begin", width=200)
        self.draft_end = pn.widgets.DatetimePicker(name="End", width=200)
        self.draft_flag = pn.widgets.Select(
            name="qcFlag", options=QC_FLAG_OPTIONS, value="suspect", width=150)
        self.draft_text = pn.widgets.TextAreaInput(
            name="Annotation", rows=3, width=620, placeholder="Describe the problem…")
        self.draft_json = pn.widgets.TextAreaInput(
            name="M2M payload (copy/paste)", rows=8, width=620, disabled=True)
        for w in (self.draft_begin, self.draft_end, self.draft_flag, self.draft_text):
            w.param.watch(lambda e: self._update_draft(), "value")
        self.draft_card = pn.Card(
            pn.Row(self.draft_begin, self.draft_end, self.draft_flag),
            self.draft_text, self.draft_json,
            title="Draft annotation — drag an x-box-select on the plot to set the time range",
            collapsed=True, sizing_mode="stretch_width")
        self.bounds_stream = None

    def _bounds_changed(self, event):
        if event.new is None:
            return
        x0, x1 = event.new
        self.draft_begin.value = _to_timestamp(x0).to_pydatetime()
        self.draft_end.value = _to_timestamp(x1).to_pydatetime()
        self.draft_card.collapsed = False

    def _update_draft(self):
        refdes = self.refdes_select.value
        if refdes not in SITES_DICT or not self.draft_begin.value:
            return
        subsite, node, sensor = refdes.split("-", 2)
        method, stream = SITES_DICT[refdes]["zarr"].removeprefix(f"{refdes}-").split("-", 1)
        payload = {
            "subsite": subsite, "node": node, "sensor": sensor,
            "method": method, "stream": stream,
            "beginDT": int(pd.Timestamp(self.draft_begin.value).timestamp() * 1000),
            "endDT": int(pd.Timestamp(self.draft_end.value).timestamp() * 1000) if self.draft_end.value else None,
            "annotation": self.draft_text.value,
            "qcFlag": self.draft_flag.value,
            "source": OOI_AUTH[0],
        }
        self.draft_json.value = json.dumps(payload, indent=2)

    # -- data/plot --
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
            ds = get_dataset(_stream_path(refdes))
            style = var_style(var)
            label = style["label"]
            ylim = style["ylim"] if self.yrange_radio.value == "Standard" else None
            notes = []
            if self.yrange_radio.value == "Standard" and not style["ylim"]:
                notes.append("no standard range for this parameter (using auto)")
            has_flags = flag_col(ds, var) is not None
            flags = [None] + (["3", "4"] if self.flags_toggle.value and has_flags else [])
            if self.flags_toggle.value and not has_flags:
                notes.append("no QARTOD results variable for this parameter")
            for f in flags:  # warm the overview cache here, off the event loop
                stream_overview(refdes, var, f)
            main = stream_layer(refdes, var, None, label)
            plot = main
            for f in flags[1:]:
                plot = plot * stream_layer(refdes, var, f, label)
            plot = plot * detail_layer(refdes, var, label)
            if self.clim_toggle.value:
                idx = ds.indexes["time"]
                band = climatology_overlay(refdes, var, idx[0], idx[-1])
                if band is not None:
                    plot = band * plot
                else:
                    notes.append("no climatology table found in qc-lookup")
            for enabled, fn, name in [(self.anno_toggle.value, annotation_overlay, "annotations"),
                                      (self.deploy_toggle.value, deployment_overlay, "deployments")]:
                if enabled:
                    try:
                        plot = plot * fn(refdes)
                    except Exception as e:
                        notes.append(f"{name} unavailable: {e}")
            plot = plot.opts(shared_axes=False, responsive=True, min_height=450, **DATETIME_OPTS)
            if ylim:
                plot = plot.opts(ylim=ylim)
            return plot, main, ds[var].sizes["time"], notes

        def show(result):
            plot, main, n, notes = result
            self.bounds_stream = hv.streams.BoundsX(source=main)
            self.bounds_stream.param.watch(self._bounds_changed, "boundsx")
            note = (f"**{n/1e6:.0f}M points** — hover reads binned value/count at any zoom; "
                    f"individual points draw below {DETAIL_THRESHOLD:,} in view.")
            if notes:
                note += " ⚠ " + "; ".join(notes)
            self.info_pane.object = note
            self.info_pane.alert_type = "warning" if notes else "primary"
            self.info_pane.visible = True
            self.plot_pane.object = plot

        self.info_pane.object = ("Aggregating stream — the first full-range view of a large "
                                 "dataset scans every chunk from S3 and can take minutes; "
                                 "it is cached afterwards…")
        self.info_pane.alert_type = "primary"
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

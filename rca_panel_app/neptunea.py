"""Neptunea — interactive companion to the rca-data-tools QAQC pipeline.

Five tabs:
  1. Curated Products — verify CRESST curated zarr products (profiler heatmaps,
     fixed-mooring time series) with lock-on hover and full-res zoom.
     Heatmap color range is adjustable live (drag/type, standard, fit to view).
     Products come from CRESST release manifests (catalog.json) where a
     folder has them. Compare with a lower QC level, another release/build,
     or another product, and overlay the curation decisions (HITL ledger,
     adjustments) behind it.
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
  5. Metadata — xarray's notebook-style HTML repr (dims, coords, variables,
     attrs, chunking) for any curated store or ooi-data stream.

Plot standards (colormaps, ranges, labels) come from rca-data-tools
params/variableParameters.csv; heatmaps use profileMin/profileMax, 1D uses
min/max. Requires OOI_USERNAME / OOI_TOKEN in the environment (or .env) for
annotation features.

    neptunea                               # console entry point
    panel serve rca_panel_app/neptunea.py  # equivalent
"""
import panel as pn
pn.extension('tabulator', loading_spinner='dots')

import io
import os
import re
import ast
import json
import hashlib
import datetime
import asyncio
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from functools import lru_cache, partial
from pathlib import Path

import s3fs
import requests
import numpy as np
import pandas as pd
import xarray as xr
import holoviews as hv
import hvplot.xarray  # noqa: F401  (registers .hvplot)
import cmocean
from holoviews.operation.datashader import rasterize
from bokeh.models import DatetimeTickFormatter, DatetimeTicker, HoverTool

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

CURATED_BUCKET = "rca-advanced-qaqc"
OOI_DATA_BUCKET = "ooi-data"
ANNO_URL = "https://ooinet.oceanobservatories.org/api/m2m/12580/anno/find"
DEPLOY_URL = "https://ooinet.oceanobservatories.org/api/m2m/12587/events/deployment/query"
METADATA_URL = "https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv/{}/{}/{}/metadata"
OOI_AUTH = (os.getenv("OOI_USERNAME", ""), os.getenv("OOI_TOKEN", ""))

QC_LOOKUP = Path.home() / "repos/qc-lookup/qartod"

# Curated-product config (instruments per product, HITL ledger, adjustments)
CRESST_LOCAL = Path.home() / "repos/rca-cicoes-cresst"
CRESST_URL = "https://raw.githubusercontent.com/ooi-data/rca-cicoes-cresst/main"

PARAMS_LOCAL = Path.home() / "repos/rca-data-tools/rca_data_tools/qaqc/params"
PARAMS_URL = "https://raw.githubusercontent.com/OOI-CabledArray/rca-data-tools/main/rca_data_tools/qaqc/params"

DATETIME_OPTS = dict(
    xformatter=DatetimeTickFormatter(months="%b %Y", years="%Y", days="%d %b"),
    xticks=DatetimeTicker(desired_num_ticks=12),
)
# Every plot has time on x: the scroll wheel zooms time only (box zoom still
# does both axes). Set per element type, since each overlay layer contributes
# its own default tools.
hv.opts.defaults(*(
    getattr(hv.opts, el)(default_tools=["save", "pan", "xwheel_zoom", "box_zoom", "reset"],
                         active_tools=["pan", "xwheel_zoom"])
    for el in ("Image", "QuadMesh", "Scatter", "Curve", "VSpans", "VLines", "Area")))
CMAP_OPTIONS = sorted(m for m in cmocean.cm.cmap_d if not m.endswith("_r"))
# rca-data-tools time-machine palette, indexed by years-back
# HITL annotation spans, cycled in time order so neighbors/overlaps differ;
# avoids QARTOD red/orange and deployment gray (colorblind-safe)
ANNO_COLORS = ["#0072B2", "#009E73", "#CC79A7", "#999933"]
TM_COLORS = ["#1f78b4", "#a6cee3", "#b2df8a", "#33a02c", "#fb9a99", "#e31a1c",
             "#fdbf6f", "#ff7f00", "#cab2d6", "#6a3ba2", "#8c564b", "#7f7f7f"]

fs = s3fs.S3FileSystem(anon=False)
EXECUTOR = ThreadPoolExecutor(max_workers=4)
# The rasterized image carries hover (time / value / count) at every zoom
# level; individual points are only drawn once sparse enough to distinguish.
DETAIL_THRESHOLD = 2_000


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
        label=f"{param} ({r.label})" if pd.notna(r.label) else param,
    )


def refdes_groups():
    """Instrument select groups keyed by site, with a blank placeholder."""
    groups = {}
    for refdes in SITES_DICT:
        groups.setdefault(refdes.split("-")[0], []).append(refdes)
    return {"": ["-- Select --"], **groups}


def select_first(select, options, refresh):
    """Load `options` and pick the first. Calls `refresh` when the value is
    unchanged (same variable name in a new source), since no watcher fires."""
    old = select.value
    select.options = options
    select.value = options[0] if options else None
    if select.value == old:
        refresh()


# --- Data access ---
@pn.cache  # releases are immutable
def manifest(folder, release):
    return json.loads(fs.cat(f"{CURATED_BUCKET}/{folder}/releases/{release}/manifest.json"))


@pn.cache  # releases are immutable
def release_csv(folder, release, name):
    """A CSV published with a release (annotations.csv, adjustments.csv, ...)."""
    return pd.read_csv(io.BytesIO(fs.cat(f"{CURATED_BUCKET}/{folder}/releases/{release}/{name}")))


@pn.cache(ttl=600)  # pick up newly written products and releases
def discover_curated():
    """Curated products in every top-level folder of the bucket -> zarr path.
    Folders with a catalog.json are read from their release manifests, newest
    release first, keyed "folder/release/product/level"; other folders are
    listed for *.zarr, keyed "folder/name.zarr". Folders that are themselves
    zarr stores (advanced-QC instrument streams) are skipped."""
    fs.invalidate_cache()  # a path arg only clears that path and its parents, not subfolders
    found = {}
    for d in fs.ls(CURATED_BUCKET, detail=True):
        folder = d["name"]
        if d["type"] != "directory" or fs.exists(f"{folder}/.zgroup") or fs.exists(f"{folder}/zarr.json"):
            continue
        name = folder.removeprefix(f"{CURATED_BUCKET}/")
        if fs.exists(f"{folder}/catalog.json"):
            for rel in json.loads(fs.cat(f"{folder}/catalog.json"))["releases"]:
                m = manifest(name, rel["id"])
                base = m["base_url"].removeprefix("s3://")
                found |= {f"{name}/{rel['id']}/{p['product']}/{p['level']}": base + p["files"]["zarr"]
                          for p in m["products"]}
        else:
            found |= {p.removeprefix(f"{CURATED_BUCKET}/"): p
                      for p in sorted(fs.glob(f"{folder}/*.zarr") + fs.glob(f"{folder}/*/*.zarr"))}
    return dict(sorted(found.items(), key=lambda kv: not release_of(kv[0])))  # releases first


# QC levels, cumulative, lowest first (rca-cicoes-cresst spec §6). Older flat
# builds carry the level as a name token (phadv before its prefix).
QC_LEVELS = ["ooi", "qartod", "hitl", "advanced", "deploymentcorrected"]
LEGACY_TOKENS = {"qf49_HITL_phadv": "advanced", "qf49_HITL": "hitl"}
LEVEL_STEPS = {  # what each level adds, and the color of points it removes
    "qartod": ("QARTOD", "#ff8c00"),
    "hitl": ("HITL + adjustments", "#d62728"),
    "advanced": ("advanced pH", "#9467bd"),
    "deploymentcorrected": ("deployment corrections", "#8c564b"),
}


def release_of(key):
    """(folder, release id) of a release product key, else None."""
    m = re.fullmatch(r"([^/]+)/(v[^/]+)/[^/]+/[^/]+", key)
    return m.groups() if m else None


def product_key(key):
    """(build, product, QC level) of a curated key, so builds and levels of one
    product match. Release keys carry all three:
    'cresst-one-easy-rca/v2026.10/axial_base_fixed_1h/hitl'
    -> ('cresst-one-easy-rca/v2026.10', 'axial_base_fixed_1h', 'hitl').
    Older flat builds drop dates and the level token from the name:
    'cresst/axial_base_fixed_20150101_20260708_1h_qf49_HITL.zarr'
    -> ('cresst', 'axial_base_fixed_1h', 'hitl')."""
    if release_of(key):
        return tuple(key.rsplit("/", 2))
    folder, _, name = key.rpartition("/")
    stem = re.sub(r"_\d{8}_\d{8}", "", name.removesuffix(".zarr"))
    for token, level in LEGACY_TOKENS.items():
        if f"_{token}" in stem:
            return folder, stem.replace(f"_{token}", "", 1), level
    return folder, stem, "ooi"


def menu_label(key):
    """Name within its build: 'product · level' for releases, else the file name."""
    _, product, level = product_key(key)
    return f"{product} · {level}" if release_of(key) else key.rpartition("/")[2]


def curated_groups(keys):
    """Select groups per build: {build: {name: key}}."""
    groups = {}
    for k in keys:
        groups.setdefault(product_key(k)[0], {})[menu_label(k)] = k
    return groups


def build_label(key):
    """Release id, or an older build's folder plus the dates in the name:
    'cresst (20150101–20260708)'."""
    if rel := release_of(key):
        return rel[1]
    folder, _, name = key.rpartition("/")
    dates = re.search(r"_(\d{8})_(\d{8})", name)
    return f"{folder} ({dates[1]}–{dates[2]})" if dates else folder


def compare_groups(key, keys):
    """Compare-select groups for `key`: the same product at lower QC levels of
    the same build, the same product and level in other builds, then other
    products of the same build."""
    build, product, level = product_key(key)
    groups = {"": {"None": ""}}
    for k in keys:
        b, p, lvl = product_key(k)
        if k == key:
            continue
        if (b, p) == (build, product) and QC_LEVELS.index(lvl) < QC_LEVELS.index(level):
            groups.setdefault("Lower QC level", {})[lvl] = k
        elif (p, lvl) == (product, level):
            groups.setdefault("Other builds", {})[build_label(k)] = k
        elif b == build:
            groups.setdefault("Other products", {})[menu_label(k)] = k
    return groups


def compare_arrays(key_a, key_b, var, datasets):
    """What to draw when comparing curated product A with B for `var`:
    B itself, points B has that A lacks (`removed`) and the reverse (`added`),
    and A − B (1D) where the grids overlap. A lower level of the same build is
    split by QC step, walking every level published in between; another build
    of the same product is compared only over the shared time span, so a
    longer record doesn't count as a difference."""
    (fa, pa, la), (fb, pb, lb) = product_key(key_a), product_key(key_b)
    a, b = load_var(datasets[key_a], var), load_var(datasets[key_b], var)
    tdim = time_dim(a)
    removed, added = [], []
    if (fa, pa) == (fb, pb):
        levels = {product_key(k)[2]: k for k in datasets if product_key(k)[:2] == (fa, pa)}
        chain = [lvl for lvl in QC_LEVELS[QC_LEVELS.index(lb):QC_LEVELS.index(la) + 1] if lvl in levels]
        for lo, hi in zip(chain, chain[1:]):
            steps = QC_LEVELS[QC_LEVELS.index(lo) + 1:QC_LEVELS.index(hi) + 1]
            before, after = xr.align(load_var(datasets[levels[lo]], var),
                                     load_var(datasets[levels[hi]], var), join="outer")
            removed.append(("removed by " + " + ".join(LEVEL_STEPS[s][0] for s in steps),
                            LEVEL_STEPS[hi][1], before.where(after.isnull())))
        label_a, label_b = la, lb
    else:
        label_a, label_b = ((build_label(key_a), build_label(key_b)) if pa == pb
                            else (menu_label(key_a), menu_label(key_b)))
        if pa == pb:
            span = slice(max(a[tdim].values[0], b[tdim].values[0]), min(a[tdim].values[-1], b[tdim].values[-1]))
            a_s, b_s = xr.align(a.sel({tdim: span}), b.sel({tdim: span}), join="outer")
            removed = [(f"only in {label_b}", "#CC79A7", b_s.where(a_s.isnull()))]
            added = [(f"only in {label_a}", "#009E73", a_s.where(b_s.isnull()))]
    diff = None
    if a.ndim == 1:
        a2, b2 = xr.align(a, b, join="inner")
        if a2.sizes[tdim] >= 0.5 * min(a.sizes[tdim], b.sizes[tdim]):
            diff = (a2 - b2).rename("difference")
    return dict(b=b, label_a=label_a, label_b=label_b, removed=removed, added=added,
                diff=diff, same_product=pa == pb)


# --- rca-cicoes-cresst config: instruments per product, HITL ledger, adjustments
@pn.cache(ttl=600)  # ledger decisions and harvests change between builds
def cresst_file(path):
    """Bytes of a file in rca-cicoes-cresst: local checkout, else GitHub main."""
    local = CRESST_LOCAL / path
    if local.exists():
        return local.read_bytes()
    resp = requests.get(f"{CRESST_URL}/{path}", timeout=30)
    resp.raise_for_status()
    return resp.content


def cresst_csv(path, **kw):
    return pd.read_csv(io.BytesIO(cresst_file(path)), **kw)


def ledger_sha():
    """Matches a product's `hitl_ledger_sha1` when built from this ledger."""
    return hashlib.sha1(cresst_file("annotations/ledger.csv")).hexdigest()[:10]


def product_instruments(key):
    """sites.csv rows for the instruments feeding a curated product."""
    sites = cresst_csv("config/sites.csv", comment="#")
    m = re.match(r"(.+?)_(fixed|profiles)", product_key(key)[1])
    if not m:
        return sites.iloc[:0]
    kind = "fixed" if m[2] == "fixed" else "profiler"
    return sites[(sites.site == m[1]) & (sites["product"] == kind)]


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


@pn.cache(ttl=600)  # refresh so newly entered annotations show up
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
    end = df["endDT"].fillna(pd.Timestamp.now("UTC").tz_localize(None))
    spans = pd.DataFrame({
        "begin": df["beginDT"], "end": end,
        "annotation": _truncate(df["annotation"]),
        "flag": df["qcFlag"].fillna("no flag"),
        "dates": df["beginDT"].dt.strftime("%Y-%m-%d") + " → " + end.dt.strftime("%Y-%m-%d"),
        "color": [ANNO_COLORS[i % len(ANNO_COLORS)] for i in range(len(df))],
    })
    hover = HoverTool(tooltips=(  # wrapped: long annotation text otherwise runs off the plot
        '<div style="max-width:380px; white-space:normal">'
        "<b>@flag</b> &middot; @dates<br>@annotation</div>"))
    return hv.VSpans(spans, kdims=["begin", "end"], vdims=["annotation", "flag", "dates", "color"]).opts(
        color="color", line_color="color", fill_alpha=0.15, line_alpha=0.7, line_width=1,
        tools=[hover],
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


@pn.cache
def parameter_ids(refdes):
    """zarr variable -> OOI parameter number (PD908 -> 908) for annotation
    `parameters`. Matched on the variable name, else its alternate_parameter_name."""
    sub, node, sensor = refdes.split("-", 2)
    resp = requests.get(METADATA_URL.format(sub, node, sensor), auth=OOI_AUTH, timeout=30)
    resp.raise_for_status()
    ids = {p["particleKey"]: int(p["pdId"].removeprefix("PD")) for p in resp.json()["parameters"]}
    ds = get_dataset(_stream_path(refdes))
    return {v: ids.get(v, ids.get(ds[v].attrs.get("alternate_parameter_name"))) for v in ds.data_vars}


def deployment_overlay(refdes):
    deps = fetch_deployments(refdes)
    return hv.VLines(
        ([t for t, _ in deps], [f"Deploy {n}: {t:%Y-%m-%d}" for t, n in deps]),
        vdims=["deployment"],
    ).opts(line_color="gray", line_dash="dashed", line_width=1, tools=["hover"])


def curated_refdes(dataset_key):
    """CTD feeding a curated product (deployment markers)."""
    rows = product_instruments(dataset_key)
    ctd = rows[rows.instrument == "ctd"].refdes
    return ctd.iloc[0] if len(ctd) else None


def _truncate(text, n=400):
    text = text.fillna("")
    return text.where(text.str.len() <= n, text.str.slice(0, n) + "…")


DECISION_STYLE = {  # curation decision -> (span color, fill alpha)
    "mask": ("#d62728", 0.15),
    "mask (other variables)": ("#d62728", 0.04),
    "ignore": ("#7f7f7f", 0.06),
    "superseded": ("#CC79A7", 0.1),
    "adjustment": ("#009E73", 0.15),
}


def curation_overlay(key, da):
    """HITL annotations and adjustments behind a curated product's variable,
    one legend entry per decision. Release products use the annotations.csv
    and adjustments.csv published with them; older builds use rca-cicoes-cresst
    main (ledger.csv, adjustments.csv). Annotations come from the product's
    nodes; those on other instruments there are kept only when masked. Masks
    applied to this variable come from its `hitl_masks` attribute when the
    build records one (spec R25); without it, a mask counts when it names this
    variable or one with the same params standard name, else shows as "mask
    (other variables)"; node wildcards and salinity inheritance (R19–R20)
    aren't resolved."""
    rows = product_instruments(key)
    refdes = set(rows.refdes) | set(rows.stitch_from.dropna())
    subsites, nodes, sensors = map(set, zip(*(r.split("-", 2) for r in refdes)))
    var, tdim = da.name, time_dim(da)
    if rel := release_of(key):
        anns, adj = release_csv(*rel, "annotations.csv"), release_csv(*rel, "adjustments.csv")
    else:
        anns = pd.concat([cresst_csv(f"annotations/raw/{s}.csv") for s in sorted(subsites)])
        anns = anns.join(cresst_csv("annotations/ledger.csv").set_index("id")[
            ["decision", "parameters_affected", "rationale"]], on="id", how="inner")
        adj = cresst_csv("config/adjustments.csv")
    applied = ({m["annotation_id"]: m for m in json.loads(da.attrs["hitl_masks"])}
               if "hitl_masks" in da.attrs else None)
    keep = (anns.subsite.isin(subsites) & (anns.node.isna() | anns.node.isin(nodes))
            & (anns.sensor.isna() | anns.sensor.isin(sensors) | (anns.decision == "mask")))
    anns = anns[keep | anns.id.isin(list(applied or ()))].reset_index(drop=True)

    def decision(r):
        if r.decision != "mask":
            return r.decision
        if applied is not None:
            return "mask" if r.id in applied else "mask (other variables)"
        params = ast.literal_eval(r.parameters_affected) if isinstance(r.parameters_affected, str) else []
        # by params standard name too: stitched records keep the source sensor's
        # variable name in the ledger (PHSENH ph_total feeds product ph_seawater)
        same = var in params or VAR_TO_PARAM.get(var) in {VAR_TO_PARAM.get(p) for p in params} - {None}
        return "mask" if not params or same else "mask (other variables)"

    data_end = pd.Timestamp(da[tdim].values[-1])  # open-ended windows run to here
    to_time = lambda s: pd.to_datetime(s, format="ISO8601", utc=True).dt.tz_localize(None)
    spans = pd.DataFrame({
        "begin": to_time(anns.beginDT),
        "end": to_time(anns.endDT).fillna(data_end),
        "decision": [decision(r) for r in anns.itertuples()],
        "id": "#" + anns.id.astype(str),
        "params": anns.parameters_affected.fillna("whole stream"),
        "rationale": anns.rationale.fillna(""),
        "annotation": _truncate(anns.annotation),
    })
    for i, m in (applied or {}).items():  # windows as applied by the build
        hit = anns.id.values == i
        spans.loc[hit, ["begin", "end"]] = to_time(pd.Series([m["begin"], m["end"]])).fillna(data_end).values
        if m["via"] != "direct":
            spans.loc[hit, "id"] += f" via {m['via']}"
    adj = adj[adj.refdes.isin(refdes) & (adj.variable == var)]
    spans = pd.concat([spans, pd.DataFrame({
        "begin": to_time(adj.begin), "end": to_time(adj.end).fillna(data_end),
        "decision": "adjustment", "id": "adjustment " + adj.id.astype(str),
        "params": adj.action + " " + adj.variable, "rationale": adj.rationale.fillna(""),
        "annotation": "",
    })])
    spans["dates"] = spans.begin.dt.strftime("%Y-%m-%d") + " → " + spans.end.dt.strftime("%Y-%m-%d")
    hover = HoverTool(tooltips=(
        '<div style="max-width:380px; white-space:normal">'
        "<b>@decision</b> &middot; @id &middot; @dates<br><i>@params</i><br>@rationale<br>@annotation</div>"))
    vdims = ["decision", "id", "params", "rationale", "annotation", "dates"]
    return hv.Overlay([
        hv.VSpans(g, kdims=["begin", "end"], vdims=vdims, label=d).opts(
            color=DECISION_STYLE[d][0], fill_alpha=DECISION_STYLE[d][1], line_alpha=0.6,
            line_width=1, tools=[hover])
        for d, g in spans.groupby("decision", sort=False)])


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


def heatmap(da, tdim, style, cmap, clim, range_xy, **extra):
    """Rasterized pressure-time heatmap. `cmap`/`clim` may be widget params or
    bound functions; they restyle the live plot without resetting zoom.
    `range_xy` is the RangeXY stream tracking the visible window; `extra`
    overrides plot options (comparison panels link axes, add titles)."""
    label = style["label"]
    hover = HoverTool(
        tooltips=[("time", "$x{%F %H:%M}"), ("pressure", "$y{0} dbar"), (label, "@image")],
        formatters={"$x": "datetime"},
    )
    opts = dict(
        colorbar=True, clabel=label, invert_yaxis=True,
        xlabel="Time", ylabel="Pressure (dbar)", tools=[hover],
        shared_axes=False, responsive=True, min_height=350, **DATETIME_OPTS,
    )
    opts.update(extra)
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

    dmap = hv.DynamicMap(view, streams=[range_xy, hv.streams.PlotSize()])
    return dmap.apply.opts(cmap=cmap, clim=clim)


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


def compare_1d(a, cmp, tdim, style, overlays=()):
    """Curated series A over comparison B (gray), points only one of them has
    colored by category, and an A − B panel below when the grids overlap.
    `overlays` (spans, markers) go on every panel."""
    label = style["label"]

    def points(da, name, color, size):
        da = da.dropna(tdim)
        hover = HoverTool(
            tooltips=[("series", name), ("time", f"@{tdim}{{%F %H:%M}}"), (label, f"@{{{da.name}}}")],
            formatters={f"@{tdim}": "datetime"},
        )
        return hv.Scatter((da[tdim].values, da.values), tdim, da.name, label=name).opts(
            color=color, size=size, tools=[hover])

    main = points(cmp["b"], cmp["label_b"], "#bdbdbd", 3) * points(a, cmp["label_a"], POINT_COLOR, 2)
    for name, color, d in cmp["removed"] + cmp["added"]:
        if d.notnull().any():  # empty categories would still add legend entries
            main = main * points(d, name, color, 5)
    panels = [(main, dict(ylabel=label, legend_position="top_left", min_height=300))]
    if cmp["diff"] is not None:
        diff = points(cmp["diff"], f"{cmp['label_a']} − {cmp['label_b']}", "#009E73", 2) \
            * hv.HLine(0).opts(color="gray", line_width=1)
        panels.append((diff, dict(ylabel=f"Δ {label}", min_height=150)))
    out = []
    for p, opts in panels:  # overlays first: an Overlay's options don't survive another *
        for o in overlays:
            p = p * o
        out.append(p.opts(xlabel="Time", show_grid=True, responsive=True, **opts, **DATETIME_OPTS))
    return out


def _to_timestamp(v):
    return pd.Timestamp(v, unit="ms") if isinstance(v, (int, float)) else pd.Timestamp(v)


# --- Stream rendering: cached full-range overview + windowed re-aggregation.
# The expensive full scan happens once per (refdes, var, flag) in a worker
# thread; pan/zoom at coarse level redraws the cached image instantly, mid
# zooms re-aggregate only the visible window (index-pruned S3 reads), and
# below DETAIL_THRESHOLD real hoverable points appear.
# Windows above MID_CAP fall back to the cached overview image; below it the
# read is strided down to TARGET_POINTS — all a detail raster can resolve.
# MID_CAP ~= one 1 Hz deployment-year, so a single deployment re-aggregates to
# a true windowed read (a few chunks, parallel fetch); wider views ride the
# overview, rasterized extra-wide so cropping to a sub-span stays sharp.
MID_CAP = 40_000_000
TARGET_POINTS = 400_000
OVERVIEW_WIDTH = 6000

STREAM_LAYERS = {  # QARTOD rollup flag value -> image style
    None: dict(cmap="kbc_r", cnorm="eq_hist"),
    "3":  dict(cmap=["#ff8c00"]),
    "4":  dict(cmap=["#d62728"]),
}
FLAG_NAMES = {"3": "QARTOD suspect", "4": "QARTOD fail"}
QARTOD_LABELS = {"1": "pass", "2": "not evaluated", "3": "suspect", "4": "fail", "9": "missing"}
POINT_COLOR = "#08306b"  # unflagged detail points; orange/red mean suspect/fail only


def flag_legend(flags, t0):
    """Legend entries for the flag layers: rasters make no legend items, so
    overlay square markers in the layer colors. Each holds one NaN point at
    t0 (Bokeh draws no legend swatch for an empty series; NaN never plots)."""
    return hv.Overlay([
        hv.Scatter([(t0, float("nan"))], "time", "value", label=FLAG_NAMES[f]).opts(
            color=STREAM_LAYERS[f]["cmap"][0], marker="square", size=10)
        for f in flags])


def _stream_path(refdes):
    return f"{OOI_DATA_BUCKET}/{SITES_DICT[refdes]['zarr']}"


def flag_col(ds, var):
    col = f"{var}_qartod_results"
    return col if col in ds else None


def _rasterize(data, var, x_range=None, y_range=None, width=1200):
    el = hv.Scatter(data, "time", var)
    return rasterize(el, x_range=x_range, y_range=y_range, dynamic=False,
                     width=width, height=400)


@pn.cache(max_items=64)
def bulk_range(refdes, var, q=(0.01, 0.99)):
    """Range of the middle 98% of a stream's values (+5% pad), sampled from
    ~3 chunks spread across the record (S3 reads whole chunks, so sampling
    more blocks approaches a full scan). None for constant/empty data
    (rasterize then uses the data extent)."""
    da = get_dataset(_stream_path(refdes))[var]
    starts, pos = [], 0
    for size in da.chunks[0]:
        starts.append(pos)
        pos += size
    picks = starts[::max(len(starts) // 3, 1)]
    sample = xr.concat([da.isel(time=slice(s, s + 20_000)) for s in picks], "time")
    lo, hi = (float(v) for v in sample.quantile(list(q)).values)
    if not lo < hi:
        return None
    pad = (hi - lo) * 0.05
    return lo - pad, hi + pad


@pn.cache(max_items=12, policy="LRU")
def stream_overview(refdes, var, flag, y_range):
    """Full-record raster over the stream's time span and a fixed y_range:
    spikes/fill values can't squash the real data into a few pixel rows, and
    a flag with no points yields an empty layer instead of one at 1970."""
    ds = get_dataset(_stream_path(refdes))
    fcol, idx = flag_col(ds, var), ds.indexes["time"]
    ddf = ds[[var] + ([fcol] if fcol else [])].unify_chunks().to_dask_dataframe()
    if flag:
        ddf = ddf[ddf[fcol].astype(str).str.strip() == flag]
    return _rasterize(ddf, var, x_range=(idx[0].to_datetime64(), idx[-1].to_datetime64()),
                      y_range=y_range, width=OVERVIEW_WIDTH)


@lru_cache(maxsize=8)
def load_slice(refdes, var, lo, hi, stride):
    ds = get_dataset(_stream_path(refdes))
    fcol = flag_col(ds, var)
    return ds[[var] + ([fcol] if fcol else [])].isel(time=slice(lo, hi, stride)).load() \
        .to_dataframe().reset_index()


def stream_layer(refdes, var, flag, label, overview_y):
    ds = get_dataset(_stream_path(refdes))
    idx, fcol = ds.indexes["time"], flag_col(ds, var)
    hover = HoverTool(
        tooltips=[("time", "$x{%F %H:%M}"), (label, "$y{0.000}"), ("count", "@image")],
        formatters={"$x": "datetime"},
    )
    opts = dict(nodata=0, xlabel="Time", ylabel=label,
                tools=[hover, "xbox_select"] if flag is None else [], **STREAM_LAYERS[flag])

    def view(x_range, y_range):
        if x_range is not None:
            lo = idx.searchsorted(_to_timestamp(x_range[0]))
            hi = idx.searchsorted(_to_timestamp(x_range[1]))
            n = int(hi - lo)
            if n <= MID_CAP:
                stride = max(1, n // TARGET_POINTS)  # cap the S3 read per zoom
                df = load_slice(refdes, var, int(lo), int(hi), stride)
                if flag:
                    df = df[df[fcol].astype(str).str.strip() == flag]
                xr_ = tuple(np.datetime64(_to_timestamp(v)) for v in x_range)
                yr_ = tuple(y_range) if y_range is not None else None
                if len(df):
                    return _rasterize(df, var, x_range=xr_, y_range=yr_).opts(**opts)
                y0, y1 = yr_ or (0.0, 1.0)
                return hv.Image(([xr_[0], xr_[1]], [y0, y1],
                                 np.full((2, 2), np.nan)), ["time", var]).opts(**opts)
        return stream_overview(refdes, var, flag, overview_y).opts(**opts)

    return hv.DynamicMap(view, streams=[hv.streams.RangeXY()])


def detail_layer(refdes, var, label, show_flags):
    """Real, hoverable points once the window holds < DETAIL_THRESHOLD samples.
    Navy unless `show_flags`, then suspect/fail points take the flag colors."""
    ds = get_dataset(_stream_path(refdes))
    idx, fcol = ds.indexes["time"], flag_col(ds, var)
    hover = HoverTool(
        tooltips=[("time", "@time{%F %H:%M:%S}"), (label, "@value"), ("QARTOD", "@flag")],
        formatters={"@time": "datetime"},
    )
    flag_colors = {f: STREAM_LAYERS[f]["cmap"][0] for f in FLAG_NAMES} if show_flags else {}

    def points(x_range, y_range):
        data = pd.DataFrame({"time": pd.to_datetime([]), "value": [], "flag": [], "color": []})
        if x_range is not None:
            lo = idx.searchsorted(_to_timestamp(x_range[0]))
            hi = idx.searchsorted(_to_timestamp(x_range[1]))
            if 0 < hi - lo <= DETAIL_THRESHOLD:
                df = load_slice(refdes, var, int(lo), int(hi), 1)  # true points
                flags = (df[fcol].astype(str).str.strip() if fcol
                         else pd.Series("", index=df.index))
                data = pd.DataFrame({
                    "time": df["time"], "value": df[var],
                    "flag": flags.map(QARTOD_LABELS).fillna("n/a"),
                    "color": flags.map(flag_colors).fillna(POINT_COLOR),
                })
        return hv.Scatter(data, "time", ["value", "flag", "color"]).opts(
            color="color", line_color="white", line_width=0.5, size=6, tools=[hover])

    return hv.DynamicMap(points, streams=[hv.streams.RangeXY()])


# --- Panel scaffolding ---
class PanelBase:
    """Selector row + plot pane + error alert; removable from its column when
    given `on_remove`."""

    def __init__(self, on_remove=None):
        self._gen = 0  # latest run() request; older results are dropped
        self.loading = pn.indicators.LoadingSpinner(value=False, width=30, height=30)
        self.remove_btn = pn.widgets.Button(
            name="✕", button_type="danger", width=40, visible=on_remove is not None)
        if on_remove:
            self.remove_btn.on_click(lambda e: on_remove(self))
        self.plot_pane = pn.pane.HoloViews(None, sizing_mode="stretch_width")
        self.error_pane = pn.pane.Alert("", alert_type="danger", visible=False)

    def clear(self):
        self.plot_pane.object = None

    def close(self):
        """Release resources when the panel is removed or the session ends."""

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
        `apply(result)` when given, else assigned to the plot pane. A newer
        request supersedes this one, so a slow load can't overwrite a fast one."""
        self._gen += 1
        gen = self._gen
        self.loading.value = True
        self.plot_pane.loading = True
        self.error_pane.visible = False

        async def task():
            try:
                result = await asyncio.get_running_loop().run_in_executor(EXECUTOR, work)
                if gen != self._gen:
                    return
                if apply is not None:
                    apply(result)
                elif result is not None:
                    self.plot_pane.object = result
            except Exception as e:
                print(f"[neptunea] {traceback.format_exc()}")
                if gen == self._gen:
                    self.clear()
                    self.error_pane.object = f"**Error:** `{type(e).__name__}: {e}`"
                    self.error_pane.visible = True
            finally:
                if gen == self._gen:
                    self.loading.value = False
                    self.plot_pane.loading = False

        pn.state.execute(task)


class CuratedPanel(PanelBase):
    """Tab 1: one curated product + variable + colormap. Compare against the
    same product at a lower QC level, another build, or another product, and
    overlay the curation decisions (HITL ledger, adjustments) behind it."""

    def __init__(self, on_remove):
        super().__init__(on_remove)
        datasets = discover_curated()
        self.dataset_select = pn.widgets.Select(
            name="Dataset", groups={"": {"-- Select --": "-- Select --"}, **curated_groups(datasets)}, width=480)
        self.var_select = pn.widgets.Select(name="Variable", options=[], width=260)
        self.cmap_select = pn.widgets.Select(name="Colormap", options=CMAP_OPTIONS, width=130)
        self.compare_select = pn.widgets.Select(name="Compare", groups={"": {"None": ""}}, width=480)
        self.mask_toggle = pn.widgets.Checkbox(name="Mask gaps", value=True, align="end")
        self.deploy_toggle = pn.widgets.Checkbox(name="Deployments", value=False, align="end")
        self.anno_toggle = pn.widgets.Checkbox(name="Annotations", value=False, align="end")
        self.note_pane = pn.pane.Alert("", alert_type="warning", visible=False)
        # heatmap color range: drag the slider or type either end (values past
        # the slider bounds are allowed). Colormap and range restyle in place.
        self.clim_slider = pn.widgets.EditableRangeSlider(
            name="Color range", start=0.0, end=1.0, value=(0.0, 1.0), step=0.01,
            format="0[.]000", width=340)
        self.clim_std_btn = pn.widgets.Button(name="Standard", width=90, align="end")
        self.clim_auto_btn = pn.widgets.Button(name="Fit view (2–98%)", width=130, align="end")
        self.clim_std_btn.on_click(lambda e: self.clim_slider.param.update(value=self._clims["standard"]))
        self.clim_auto_btn.on_click(lambda e: self._fit_view())
        self.clim_row = pn.Row(self.clim_slider, self.clim_std_btn, self.clim_auto_btn,
                               visible=False)
        self._clims, self._clim_key, self._view = {}, None, None
        self.dataset_select.param.watch(self._dataset_changed, "value")
        self.var_select.param.watch(self._var_changed, "value")
        for w in (self.compare_select, self.mask_toggle, self.deploy_toggle, self.anno_toggle):
            w.param.watch(lambda e: self._replot(), "value")
        self.layout = self.build_layout(
            self.dataset_select, self.var_select, self.cmap_select)
        self.layout.insert(1, pn.Row(self.compare_select, self.mask_toggle,
                                     self.deploy_toggle, self.anno_toggle))
        self.layout.insert(2, self.clim_row)
        self.layout.insert(3, self.note_pane)

    def _replot(self):
        self.run(self._build, apply=self._show)

    def _dataset_changed(self, event):
        datasets = discover_curated()
        self.compare_select.param.update(
            groups=compare_groups(event.new, datasets) if event.new in datasets else {"": {"None": ""}},
            value="")
        if event.new not in datasets:
            self.var_select.options = []
            self.clim_row.visible = False
            self.note_pane.visible = False
            self.clear()
            return
        self.run(lambda: list(get_dataset(datasets[event.new]).data_vars),
                 apply=lambda names: select_first(self.var_select, names, self._replot))

    def _var_changed(self, event):
        if not event.new:
            return
        self.cmap_select.value = var_style(event.new)["cmap"]
        self._replot()

    def _fit_view(self):
        """Set the color range to the 2–98th percentile of the visible window."""
        da, tdim, rng = self._view
        sel = {}
        if rng.x_range:
            sel[tdim] = slice(*sorted(_to_timestamp(v) for v in rng.x_range))
        if rng.y_range:
            sel["sea_water_pressure"] = slice(*sorted(rng.y_range))
        lo, hi = (float(v) for v in da.sel(sel).quantile([0.02, 0.98]).values)
        if lo < hi:
            self.clim_slider.value = (lo, hi)

    def _build(self):
        datasets = discover_curated()
        name, other = self.dataset_select.value, self.compare_select.value
        var = self.var_select.value
        if name not in datasets or var not in get_dataset(datasets[name]):
            return None  # also skips the transient replot while a new dataset loads
        da = load_var(datasets[name], var)
        style = var_style(var)
        tdim = time_dim(da)
        clims, cmp, notes = None, None, []
        if other in datasets:
            if var not in get_dataset(datasets[other]):
                notes.append(f"{var} isn't in {other}")
            elif get_dataset(datasets[other])[var].ndim != da.ndim:
                notes.append("can't compare a profile product with a time series")
            else:
                cmp = compare_arrays(name, other, var, datasets)
        overlays = self._overlays(name, da, notes)
        if da.ndim == 2:
            q = [float(v) for v in da.quantile([0.001, 0.02, 0.98, 0.999]).values]
            clims = dict(standard=style["clim"], auto=(q[1], q[2]), bounds=(q[0], q[3]))
            panels = self._heatmaps(da, tdim, style, cmp, clims, notes)
        elif cmp:
            panels = compare_1d(da, cmp, tdim, style, overlays)
            overlays = []
        else:
            panels = [scatter_1d(da, tdim, style)]
        for o in overlays:
            panels = [p * o for p in panels]
        plot = panels[0] if len(panels) == 1 else hv.Layout(panels).cols(1)
        return plot, clims, notes

    def _overlays(self, name, da, notes):
        """Deployment markers and curation spans, as toggled; failures become notes."""
        overlays = []
        if self.deploy_toggle.value:
            try:
                refdes = curated_refdes(name)
                if refdes:
                    overlays.append(deployment_overlay(refdes))
            except Exception as e:
                notes.append(f"deployments unavailable: {e}")
        if self.anno_toggle.value:
            try:
                overlays.append(curation_overlay(name, da))
                # releases carry their own decisions; older builds are drawn from main
                built = get_dataset(discover_curated()[name]).attrs.get("hitl_ledger_sha1")
                if not release_of(name) and built and built != ledger_sha():
                    notes.append(f"the ledger has changed since this build ({built}); spans show current decisions")
            except Exception as e:
                notes.append(f"annotations unavailable: {e}")
        return overlays

    def _heatmaps(self, da, tdim, style, cmp, clims, notes):
        """The heatmap, plus the comparison product and, for another build or
        level of the same product, a changes panel: red where only B has data,
        green where only A has. Panels of one product share zoom; another
        product keeps its own axes."""
        cmap = pn.bind(lambda n: cmocean.cm.cmap_d[n], self.cmap_select)
        clim = self.clim_slider.param.value
        gaps = (lambda x, t: mask_gaps(x, t)) if self.mask_toggle.value else (lambda x, t: x)
        rng = hv.streams.RangeXY()
        clims["view"] = (gaps(da, tdim), tdim, rng)
        if cmp is None:
            return [heatmap(gaps(da, tdim), tdim, style, cmap, clim, rng)]
        b, tb = cmp["b"], time_dim(cmp["b"])
        extra = dict(shared_axes=cmp["same_product"], min_height=260)
        panels = [heatmap(gaps(da, tdim), tdim, style, cmap, clim, rng, title=cmp["label_a"], **extra),
                  heatmap(gaps(b, tb), tb, style, cmap, clim, hv.streams.RangeXY(),
                          title=cmp["label_b"], **extra)]
        if cmp["same_product"]:
            # 1 = only B has data, 2 = only A has; the sets are disjoint
            masks = xr.align(*[d.notnull() for _, _, d in cmp["removed"] + cmp["added"]],
                             join="outer", fill_value=False)
            only_b = sum(masks[:len(cmp["removed"])]) > 0
            only_a = sum(masks[len(cmp["removed"]):]) > 0 if cmp["added"] else xr.zeros_like(only_b)
            if not (only_b.any() or only_a.any()):
                notes.append(f"no cells differ in coverage between {cmp['label_a']} and {cmp['label_b']}")
                return panels
            change = xr.where(only_b, 1.0, xr.where(only_a, 2.0, np.nan)).rename("change")
            panels.append(heatmap(
                gaps(change, tdim), tdim, dict(label="change (1 = only B, 2 = only A)"),
                ["#d62728", "#009E73"], (1, 2), hv.streams.RangeXY(), colorbar=False,
                title=f"red: only in {cmp['label_b']} · green: only in {cmp['label_a']}", **extra))
        return panels

    def _show(self, result):
        if result is None:
            self.note_pane.visible = False
            return
        plot, clims, notes = result
        self.note_pane.object = "⚠ " + "; ".join(notes) if notes else ""
        self.note_pane.visible = bool(notes)
        is_2d = clims is not None
        self.cmap_select.disabled = not is_2d
        self.mask_toggle.disabled = not is_2d
        self.clim_row.visible = is_2d
        if is_2d:
            # slider spans the data (0.1–99.9%) and the standard range; the
            # value resets to standard only for a new dataset/variable
            self._view = clims.pop("view")
            self._clims = clims
            clims["standard"] = clims["standard"] or clims["auto"]
            lo = min(clims["bounds"][0], clims["standard"][0])
            hi = max(clims["bounds"][1], clims["standard"][1])
            key = (self.dataset_select.value, self.var_select.value)
            reset = dict(value=clims["standard"]) if key != self._clim_key else {}
            self._clim_key = key
            self.clim_slider.param.update(start=lo, end=hi, step=(hi - lo) / 500, **reset)
        self.plot_pane.object = plot


QC_FLAG_OPTIONS = ["suspect", "fail", "not_operational", "not_available", "pass"]


class StreamPanel(PanelBase):
    """Tab 2: full-resolution ooi-data stream with QARTOD flag, climatology,
    annotation, and deployment overlays, plus an annotation drafting tool."""

    def __init__(self, on_remove):
        super().__init__(on_remove)
        self.refdes_select = pn.widgets.Select(
            name="Instrument", groups=refdes_groups(), width=340)
        self.var_select = pn.widgets.Select(name="Variable", options=[], width=280)
        self.yrange_radio = pn.widgets.RadioButtonGroup(
            options=["Standard", "Auto"], value="Standard", width=150)
        self.anno_toggle = pn.widgets.Checkbox(name="Annotations", value=False)
        self.deploy_toggle = pn.widgets.Checkbox(name="Deployments", value=True)
        self.flags_toggle = pn.widgets.Checkbox(name="QARTOD flags", value=False)
        self.clim_toggle = pn.widgets.Checkbox(name="Climatology", value=False)
        self.info_pane = pn.pane.Alert("", alert_type="primary", visible=False)
        # selectable stream name + copy-to-clipboard button
        self.stream_text = pn.widgets.TextInput(
            width=440, visible=False, styles={"font-family": "monospace"})
        self.copy_btn = pn.widgets.ButtonIcon(
            icon="clipboard", active_icon="check", toggle_duration=1000,
            description="Copy stream name", visible=False, align="center")
        self.copy_btn.js_on_click(args={"src": self.stream_text},
                                  code="navigator.clipboard.writeText(src.value)")
        # zoom re-aggregation runs in the RangeXY callback (not run()); mirror
        # the server-busy flag so a spinner marks "thinking" vs. "stuck".
        self.zoom_busy = pn.indicators.LoadingSpinner(
            value=False, size=22, color="warning", name="re-aggregating…",
            visible=False)
        # pn.state is process-wide: unwatch on panel removal / session end.
        # Bind unwatch now; module globals are gone by session teardown.
        watcher = pn.state.param.watch(self._busy_changed, "busy")
        self._unwatch_busy = partial(pn.state.param.unwatch, watcher)
        pn.state.on_session_destroyed(lambda ctx: self.close())

        self.refdes_select.param.watch(self._refdes_changed, "value")
        for w in (self.var_select, self.yrange_radio, self.anno_toggle,
                  self.deploy_toggle, self.flags_toggle, self.clim_toggle):
            w.param.watch(lambda e: self._plot(), "value")

        self._init_draft()
        self.layout = self.build_layout(
            self.refdes_select, self.var_select, self.yrange_radio, self.zoom_busy)
        self.layout.insert(1, pn.Row(self.anno_toggle, self.deploy_toggle,
                                     self.flags_toggle, self.clim_toggle,
                                     self.stream_text, self.copy_btn, align="center"))
        self.layout.insert(2, self.info_pane)
        self.layout.append(self.draft_card)

    def _busy_changed(self, event):
        self.zoom_busy.value = self.zoom_busy.visible = event.new

    def close(self):
        if self._unwatch_busy:
            self._unwatch_busy()
            self._unwatch_busy = None

    # -- annotation drafting --
    def _init_draft(self):
        self.draft_begin = pn.widgets.DatetimePicker(name="Begin (UTC)", width=200)
        self.draft_end = pn.widgets.DatetimePicker(name="End (UTC)", width=200)
        self.draft_flag = pn.widgets.Select(
            name="qcFlag", options=QC_FLAG_OPTIONS, value="suspect", width=150)
        self.draft_text = pn.widgets.TextAreaInput(
            name="Annotation", rows=3, width=620, placeholder="Describe the problem…")
        self.draft_source = pn.widgets.TextInput(
            name="Your name/email (source)", width=250)
        self.draft_json = pn.widgets.TextAreaInput(
            name="M2M payload (copy/paste)", rows=8, width=620, disabled=True)
        self.draft_note = pn.pane.Markdown("")
        for w in (self.draft_begin, self.draft_end, self.draft_flag, self.draft_text,
                  self.draft_source, self.var_select):
            w.param.watch(lambda e: self._update_draft(), "value")
        self.draft_card = pn.Card(
            pn.pane.Markdown("Builds an M2M annotation payload to copy; nothing is sent to OOI. "
                             "Set the time range by choosing the **box select** tool in the "
                             "plot toolbar and dragging across the plot, or type it below."),
            pn.Row(self.draft_begin, self.draft_end, self.draft_flag, self.draft_source),
            self.draft_text, self.draft_note, self.draft_json,
            title="Draft annotation", collapsed=True, sizing_mode="stretch_width")
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
        var = self.var_select.value
        try:
            pid = parameter_ids(refdes).get(var)
        except Exception:  # metadata lookup needs OOI credentials
            pid = None
        self.draft_note.object = (
            f"Applies to `{var}` (PD{pid})." if pid else
            f"`{var}` has no OOI parameter ID, so the annotation would cover the whole stream.")
        payload = {
            "subsite": subsite, "node": node, "sensor": sensor,
            "method": method, "stream": stream,
            "beginDT": int(pd.Timestamp(self.draft_begin.value).timestamp() * 1000),
            "endDT": int(pd.Timestamp(self.draft_end.value).timestamp() * 1000) if self.draft_end.value else None,
            "annotation": self.draft_text.value,
            "qcFlag": self.draft_flag.value,
            "parameters": [pid] if pid else [],
            "source": self.draft_source.value,
        }
        self.draft_json.value = json.dumps(payload, indent=2)

    # -- data/plot --
    def _refdes_changed(self, event):
        known = event.new in SITES_DICT
        self.stream_text.value = SITES_DICT[event.new]["zarr"] if known else ""
        self.stream_text.visible = self.copy_btn.visible = known
        if not known:
            self.var_select.options = []
            self.clear()
            self.info_pane.visible = False
            return
        def go():
            ds = get_dataset(_stream_path(event.new))
            return [v for v in SITES_DICT[event.new]["params"] if v in ds]
        self.run(go, apply=lambda opts: select_first(self.var_select, opts, self._plot))

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
            overview_y = ylim or bulk_range(refdes, var)
            for f in flags:  # warm the overview cache here, off the event loop
                stream_overview(refdes, var, f, overview_y)
            with suppress(Exception):  # warm for annotation drafts; needs OOI credentials
                parameter_ids(refdes)
            main = stream_layer(refdes, var, None, label, overview_y)
            plot = main
            for f in flags[1:]:
                plot = plot * stream_layer(refdes, var, f, label, overview_y)
            plot = plot * detail_layer(refdes, var, label, len(flags) > 1)
            if flags[1:]:
                plot = plot * flag_legend(flags[1:], ds.indexes["time"][0])
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
            plot = plot.opts(shared_axes=False, responsive=True, min_height=450,
                             legend_position="top_left", **DATETIME_OPTS)
            if ylim:
                plot = plot.opts(ylim=ylim)
            return plot, main, ds[var].sizes["time"], notes, ylim is None

        def show(result):
            plot, main, n, notes, auto = result
            self.bounds_stream = hv.streams.BoundsX(source=main)
            self.bounds_stream.param.watch(self._bounds_changed, "boundsx")
            note = (f"**{n:,} points** — hover reads binned value/count at any zoom; "
                    f"individual points draw below {DETAIL_THRESHOLD:,} in view.")
            if auto:
                note += " Auto y-range fits the middle 98% of values; zoom or pan to find spikes."
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
    da = get_dataset(_stream_path(key))[var].sel(time=str(year))
    stride = max(1, da.sizes["time"] // 200_000)
    return da.isel(time=slice(None, None, stride)).load()


class TimeMachinePanel(PanelBase):
    """Tab 3: one year vs selected prior years. Sources are 1D series only:
    curated fixed-mooring products and (decimated) full ooi-data streams."""

    def __init__(self):
        super().__init__()
        fixed = [k for k in discover_curated() if "_profiles" not in product_key(k)[1]]
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
        # Month-day window applied to every overlaid year; the year component
        # tracks the reference year. Default full-year = original behavior.
        self.win_start = pn.widgets.DatePicker(name="Window start", width=170)
        self.win_end = pn.widgets.DatePicker(name="Window end", width=170)
        self.full_year_btn = pn.widgets.Button(name="Full year", width=90)
        self.invert_toggle = pn.widgets.Checkbox(name="Invert y-axis", value=False)
        self.size_slider = pn.widgets.DiscreteSlider(
            name="Point size", options=[0.25, 0.5, 1, 2, 3, 4, 6, 8, 10], value=3, width=170)
        self._suppress = False
        self.dataset_select.param.watch(self._dataset_changed, "value")
        self.var_select.param.watch(lambda e: self._years_then_plot(), "value")
        self.year_select.param.watch(self._year_changed, "value")
        for w in (self.compare_select, self.win_start, self.win_end,
                  self.invert_toggle, self.size_slider):
            w.param.watch(lambda e: self._rebuild(), "value")
        self.full_year_btn.on_click(lambda e: self._reset_window())
        self.layout = self.build_layout(
            self.dataset_select, self.var_select, self.year_select, self.compare_select)
        self.layout.insert(1, pn.Row(self.win_start, self.win_end, self.full_year_btn,
                                     self.invert_toggle, self.size_slider, align="center"))

    def _open(self):
        kind, key = self.sources[self.dataset_select.value]
        return kind, key, get_dataset(key if kind == "curated" else _stream_path(key))

    def _dataset_changed(self, event):
        if event.new not in self.sources:
            self.clear()
            return
        def go():
            kind, key, ds = self._open()
            if kind == "stream":
                return [v for v in SITES_DICT[key]["params"] if v in ds]
            return list(ds.data_vars)
        self.run(go, apply=lambda names: select_first(
            self.var_select, names, self._years_then_plot))

    def _years_then_plot(self):
        if not self.var_select.value:
            return
        def set_years(payload):
            years, ref = payload
            self._suppress = True
            self.year_select.options = years
            self.compare_select.options = years
            self.year_select.value = ref
            # default: reference = last full year only; keep comparisons
            # the user already picked that exist in this source
            self.compare_select.value = [y for y in self.compare_select.value if y in years]
            self._remap_window(ref)
            self._suppress = False
            self.run(self._build)
        def go():
            idx = self._open()[2].indexes["time"]
            years = list(range(idx[-1].year, idx[0].year - 1, -1))
            last = idx[-1]
            # a year is "full" if data reaches ~mid-December; else step back one
            ref = last.year if last.dayofyear >= 350 else last.year - 1
            return years, (ref if ref in years else years[0])
        self.run(go, apply=set_years)

    def _remap_window(self, year):
        """Point the window pickers at `year`, keeping month/day; default to
        the full year. Callers hold self._suppress."""
        for pk, default in ((self.win_start, (1, 1)), (self.win_end, (12, 31))):
            m, d = (pk.value.month, pk.value.day) if pk.value else default
            pk.start = datetime.date(year, 1, 1)
            pk.end = datetime.date(year, 12, 31)
            pk.value = datetime.date(year, m, d)

    def _year_changed(self, event):
        if self._suppress or event.new is None:
            return
        self._suppress = True
        self._remap_window(event.new)
        self._suppress = False
        self.run(self._build)

    def _rebuild(self):
        if not self._suppress:
            self.run(self._build)

    def _reset_window(self):
        if self.year_select.value is None:
            return
        self._suppress = True
        self.win_start.value = datetime.date(self.year_select.value, 1, 1)
        self.win_end.value = datetime.date(self.year_select.value, 12, 31)
        self._suppress = False
        self.run(self._build)

    def _build(self):
        ref = self.year_select.value
        if self.dataset_select.value not in self.sources or ref is None:
            return None
        kind, key = self.sources[self.dataset_select.value]
        var = self.var_select.value
        style = var_style(var)
        ws, we = self.win_start.value, self.win_end.value
        curves = []
        compare = [y for y in self.compare_select.value if y != ref]
        for yr in [ref] + sorted(compare, reverse=True):
            sl = load_year(kind, key, var, yr).dropna("time")
            if ws and we:  # restrict each year to the month-day window
                try:
                    lo = pd.Timestamp(year=yr, month=ws.month, day=ws.day)
                    hi = pd.Timestamp(year=yr, month=we.month, day=we.day) + pd.Timedelta(days=1)
                except ValueError:
                    continue
                sl = sl.sel(time=slice(lo, hi))
            if not sl.time.size:
                continue
            # rca-data-tools mechanic: slide prior years forward onto the ref-year axis
            shifted = sl.assign_coords(time=sl.time + np.timedelta64((ref - yr) * 365, "D"))
            color = TM_COLORS[(ref - yr) % len(TM_COLORS)]
            curves.append(shifted.hvplot.scatter(
                x="time", size=self.size_slider.value, color=color, label=str(yr),
                xlabel=f"Time ({ref})", ylabel=style["label"],
                min_height=400, responsive=True,
            ))
        if not curves:
            raise ValueError(f"No {var} data in the selected years/window")
        return hv.Overlay(curves).opts(
            shared_axes=False, legend_position="top_right", padding=(0, 0.1),
            invert_yaxis=self.invert_toggle.value, **DATETIME_OPTS)


class AnnotationBrowser(PanelBase):
    """Tab 4: searchable annotation table per reference designator."""

    def __init__(self):
        super().__init__()
        self.refdes_select = pn.widgets.Select(
            name="Instrument", groups=refdes_groups(), width=340)
        self.count = pn.pane.Markdown("")
        # the table stands in for the plot pane so run() drives its loading state
        self.plot_pane = pn.widgets.Tabulator(
            pd.DataFrame(), header_filters=True, pagination="local", page_size=25,
            layout="fit_data_stretch", show_index=False, disabled=True,
            sizing_mode="stretch_width",
        )
        self.refdes_select.param.watch(self._load, "value")
        self.layout = self.build_layout(self.refdes_select, self.count)

    def clear(self):
        self.plot_pane.value = pd.DataFrame()
        self.count.object = ""

    def _load(self, event):
        if event.new not in SITES_DICT:
            self.clear()
            return
        def show(df):
            # `source` (annotator emails) is left out: the dashboard is public
            cols = [c for c in ["beginDT", "endDT", "annotation", "qcFlag"] if c in df]
            self.plot_pane.value = df[cols] if not df.empty else pd.DataFrame()
            self.count.object = f"**{len(df)} annotations**"
        self.run(lambda: fetch_annotations(event.new), apply=show)


class MetadataPanel(PanelBase):
    """Tab 5: xarray's rich HTML repr for any curated store or ooi-data stream
    (the same collapsible view a Dataset shows in a Jupyter notebook)."""

    def __init__(self):
        super().__init__()
        # PanelBase.run() targets plot_pane; an HTML pane has the same
        # object/loading interface, so it slots straight in.
        self.plot_pane = pn.pane.HTML(None, sizing_mode="stretch_width")
        self.source_select = pn.widgets.Select(
            name="Dataset", width=520,
            groups={"": ["-- Select --"],
                    "Curated": list(discover_curated()),
                    "OOI streams": list(SITES_DICT)})
        self.source_select.param.watch(self._changed, "value")
        self.layout = self.build_layout(self.source_select)

    def _changed(self, event):
        curated = discover_curated()
        if event.new in curated:
            path = curated[event.new]
        elif event.new in SITES_DICT:
            path = _stream_path(event.new)
        else:
            self.clear()
            return
        self.run(lambda: get_dataset(path)._repr_html_())


def multi_panel_tab(panel_cls):
    """Column of removable panels with an add button."""
    column = pn.Column(sizing_mode="stretch_width")
    def remove(p):
        p.close()
        column.remove(p.layout)
    def add(event=None):
        column.append(panel_cls(on_remove=remove).layout)
    btn = pn.widgets.Button(name="+ Add panel", button_type="primary", width=150)
    btn.on_click(add)
    add()
    return pn.Column(btn, column, sizing_mode="stretch_width")


tabs = pn.Tabs(
    ("Curated Products", multi_panel_tab(CuratedPanel)),
    ("Full Time Series", multi_panel_tab(StreamPanel)),
    ("Time Machine", TimeMachinePanel().layout),
    ("Annotations", AnnotationBrowser().layout),
    ("Metadata", MetadataPanel().layout),
    dynamic=True,
    sizing_mode="stretch_width",
)

template = pn.template.FastListTemplate(
    title="Neptunea",
    main=[tabs],
    accent_base_color="#1a6b8a",
    header_background="#1a6b8a",
)
template.servable()

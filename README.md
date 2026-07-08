# RCA QC Explorer

Interactive [Panel](https://panel.holoviz.org) app for verifying OOI Regional
Cabled Array data products — a companion to the static plot pipeline in
[rca-data-tools](https://github.com/OOI-CabledArray/rca-data-tools). Built for
the two things static plots can't do: **lock-on hover** to read exact values,
and **full-resolution zoom** to pinpoint when a data problem starts.

## Tabs

| Tab | What it does |
|---|---|
| **Curated Products** | View CRESST curated zarr products from `s3://rca-advanced-qaqc/cresst/` — profiler datasets as depth–time heatmaps, fixed-mooring datasets as time series. Per-panel cmocean colormap selector (defaults to the QAQC standard). Add panels to compare datasets side by side. |
| **Full Time Series** | Any instrument stream from the `ooi-data` bucket, datashaded at native resolution (hundreds of millions of points). Menu built from rca-data-tools `sitesDictionary.csv`. Zoom below ~100k points in view and real, hoverable data points appear on top of the shaded image. Toggleable HITL annotation overlay (translucent red bands with hover text, live from the OOI M2M API) and an Auto/Standard y-range switch. |
| **Time Machine** | One year vs selected prior years on the same axis — the rca-data-tools time-machine mechanic (prior years slid forward by 365-day multiples, colored by years-back). 1D sources only: curated fixed-mooring products and ooi-data streams (stride-decimated to ~200k points/year). |
| **Annotations** | Searchable, filterable table of HITL annotations per reference designator. |

## Install

```bash
pip install -e .
```

Annotation features need OOI API credentials in the environment or a `.env`
file in the working directory:

```
OOI_USERNAME=...
OOI_TOKEN=...
```

AWS credentials must be configured for S3 access (`rca-advanced-qaqc` and
`ooi-data` buckets).

## Run

```bash
qc-explorer                # or: panel serve rca_panel_app/qc_explorer.py
qc-explorer --port 8080 --no-show
```

## Design notes

- **Plot standards come from rca-data-tools** (`params/variableParameters.csv`,
  `variableMap.csv`, `sitesDictionary.csv`) — read from a local checkout at
  `~/repos/rca-data-tools` when present, otherwise from GitHub raw. Heatmap
  color limits use `profileMin`/`profileMax`; 1D standard y-ranges use
  `min`/`max`, matching the QAQC pipeline exactly.
- **Curated variables are loaded into memory** (LRU cache, 12 variables ≈
  400 MB max) so zoom re-rendering aggregates from RAM instead of re-reading
  zarr chunks from S3. This is the difference between millisecond and
  multi-second zooms, and it keeps dask out of the interactive render path,
  which is where the flaky holoviews rasterize edge cases live. Full `ooi-data`
  streams are too large to load, so they stay dask-backed and datashaded.
- **Hover without performance cost**: heatmap hover reads the aggregated value
  off the rasterized image (constant cost at any data size); 1D curated plots
  are small enough to hover real points. Datashaded stream plots get a
  zoom-detail layer: once the visible window holds fewer than 100k samples,
  the actual points are fetched and drawn (crimson) with full value hover —
  cost stays bounded no matter how large the stream.
- **Loading feedback**: blocking data loads run in a thread pool, so the pane
  shows Panel's loading overlay and the UI stays responsive while S3 reads
  happen. Stream panels display a point count and a warning for very large
  streams. One caveat: the *first* full-range datashade of a huge stream
  happens at render time on the event loop and can still stall the page for
  its duration — the loading overlay and notice remain visible while it runs.
- **Known transient error**: a holoviews `ValueError: Supplied Image bounds do
  not match...` has been observed once during heatmap zooming but never
  reproduced across all bucket stores and every zoom edge case tested. If it
  recurs it aborts a single re-render (the next zoom recovers); the in-memory
  change likely eliminates it. If you see it repeatedly, capture the dataset +
  zoom range.
- **Time-machine caveat**: prior years shift by exact 365-day multiples
  (faithful to rca-data-tools), so alignment drifts ~1 day per 4 years across
  leap years.
- The annotation `exclude` flag is deprecated and ignored throughout.

## Roadmap ideas

In rough order of QC value:

1. Deployment-boundary markers on all time plots (problem onsets often
   coincide with deployments).
2. Click-a-profile inspector: click a heatmap → that profile as a depth curve.
3. Annotation ↔ plot linking: click a table row → jump the plot to that span.
4. `localRanges.yaml` support as a third y-range option.

# RCA QC Explorer

Interactive [Panel](https://panel.holoviz.org) app for QC of OOI Regional
Cabled Array data — hover and zoom to pinpoint exactly when a data problem
starts. Companion to the static plots from
[rca-data-tools](https://github.com/OOI-CabledArray/rca-data-tools).

## Features

**Curated Products** — CRESST curated zarr products (`s3://rca-advanced-qaqc/cresst/`)
- Profiler datasets as depth–time heatmaps, fixed-mooring as time series
- Lock-on hover, cmocean colormap selector (defaults to QAQC standards)
- Gap masking: sampling gaps render transparent instead of smearing
- Deployment markers; add/remove panels to compare datasets

**Full Time Series** — any stream in `ooi-data` (menu from `sitesDictionary.csv`)
- Full native resolution (100M+ points); hover at every zoom level
- Three-tier rendering: cached overview (instant after first scan) →
  windowed re-aggregation on zoom → individual points once sparse
- Overlay toggles: HITL annotations (red bands), deployments (dashed lines),
  QARTOD flags (suspect orange / fail dark red), climatology envelope (green)
- Annotation drafting: drag an x-box-select, fill in text and qcFlag, copy a
  ready-to-paste M2M payload (draft-only — the app never POSTs)

**Time Machine** — year-over-year comparison on a shared axis
(rca-data-tools mechanic); curated fixed products and decimated streams

**Annotations** — searchable, filterable HITL annotation table per refdes

## Install & run

```bash
pip install -e .
qc-explorer                # or: panel serve rca_panel_app/qc_explorer.py
qc-explorer --port 8080 --no-show
```

Requires AWS credentials for S3, and `OOI_USERNAME`/`OOI_TOKEN` (env or
`.env`) for annotation/deployment features.

## Notes

- Plot standards (colormaps, ranges, labels) come from rca-data-tools
  `params/` — local checkout if present, else GitHub raw. Heatmaps use
  `profileMin/profileMax`; 1D uses `min/max`.
- Climatology bands read the qc-lookup QARTOD tables from `~/repos/qc-lookup`.
- Curated variables load into memory (LRU-capped) for millisecond zooms;
  stream overviews are cached per instrument/variable/flag after one scan
  (~2 min for 350M points, done off the event loop).
- Time machine shifts prior years by 365-day multiples (drifts ~1 day per
  4 years across leap years, faithful to rca-data-tools).
- The annotation `exclude` flag is deprecated and ignored throughout.

## Roadmap

1. POST drafted annotations to M2M behind a confirmation step
2. Click-a-profile inspector: click a heatmap → that profile as a depth curve
3. Annotation ↔ plot linking: table row → plot span
4. `localRanges.yaml` as a third y-range option
5. Persist stream overview caches to disk across restarts

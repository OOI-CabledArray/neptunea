# RCA QC Explorer

Interactive [Panel](https://panel.holoviz.org) app for QC of OOI Regional
Cabled Array data — hover and zoom to pinpoint when a data problem starts.
Companion to [rca-data-tools](https://github.com/OOI-CabledArray/rca-data-tools).

## Tabs

- **Curated Products** — CRESST zarr products as heatmaps (profilers) or time
  series (fixed moorings). Hover, cmocean colormap selector, gap masking,
  deployment markers, side-by-side comparison panels.
- **Full Time Series** — any `ooi-data` stream at native resolution. Hover at
  every zoom level; cached overview → windowed re-aggregation → points once
  sparse. Overlays: annotations, deployments, QARTOD flags, climatology.
  Drag-select a range to draft an M2M annotation payload (never POSTs).
- **Time Machine** — year-over-year overlay of fixed products and streams.
- **Annotations** — searchable HITL annotation table per refdes.
- **Metadata** — xarray notebook-style repr for any store.

## Install & run

```bash
pip install -e .
qc-explorer                # or: panel serve rca_panel_app/qc_explorer.py
```

Needs AWS credentials for S3; `OOI_USERNAME`/`OOI_TOKEN` (env or `.env`) for
annotation/deployment features.

## Notes

- Plot standards come from rca-data-tools `params/` (local checkout, else
  GitHub raw); climatology from `~/repos/qc-lookup` QARTOD tables.
- Caching: curated variables load into memory; stream overviews cached per
  instrument/variable/flag after one full scan (~2 min, off the event loop).
- Time machine shifts prior years by 365-day multiples (~1 day leap drift
  per 4 years, faithful to rca-data-tools).

## Roadmap

1. POST drafted annotations to M2M behind a confirmation step
2. Click a heatmap profile → depth-curve inspector
3. Annotation table row → plot span linking
4. `localRanges.yaml` as a third y-range option
5. Persist stream overview caches to disk

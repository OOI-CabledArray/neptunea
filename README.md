# Neptunea

Interactive [Panel](https://panel.holoviz.org) app for QC of OOI Regional
Cabled Array data — hover and zoom to pinpoint when a data problem starts.
Companion to [rca-data-tools](https://github.com/OOI-CabledArray/rca-data-tools).
Named for *Neptunea*, a slow-moving marine snail.

Hosted at **https://neptunea.ooi-rca.net** (weekdays 7am–7pm Pacific);
see [deploy/README.md](deploy/README.md).

## Tabs

- **Curated Products** — zarr products from every folder in `rca-advanced-qaqc`
  (`cresst/`, `cresst-one-easy-rca/`, …) as heatmaps (profilers) or time
  series (fixed moorings). Colormap, gap masking, deployment markers,
  side-by-side panels. Heatmap color range: drag/type, params standard, or
  fit to the zoomed view.
- **Full Time Series** — any `ooi-data` stream at native resolution, hoverable
  at every zoom. Y-range: params standard or Auto (middle 98%, so spikes don't
  flatten the view). Overlays: annotations, deployments, QARTOD flags,
  climatology. Box-select a time range to draft an M2M annotation payload to
  copy (never POSTs).
- **Time Machine** — year-over-year overlay of fixed products and streams.
- **Annotations** — searchable HITL annotation table per refdes.
- **Metadata** — xarray notebook-style repr for any store.

## Run locally

```bash
uv run neptunea                                                    # app
uv run panel serve rca_panel_app/neptunea.py --autoreload --show   # dev, reloads on save
```

Needs AWS credentials for S3; `OOI_USERNAME`/`OOI_TOKEN` (env or `.env`) for
annotations, deployments and annotation parameter IDs.

Dependencies: edit `pyproject.toml`, run `uv lock` (`uv lock --upgrade` to
update packages), test, and commit both files — the server installs exactly
what `uv.lock` pins.

## Notes

- Plot standards come from rca-data-tools `params/` (local checkout, else
  GitHub raw); climatology from `~/repos/qc-lookup` QARTOD tables.
- Caching: curated product list and annotations refresh every 10 min; curated
  variables load into memory; stream overviews are cached per
  instrument/variable/flag/y-range after one full scan (~2 min).
- Time Machine shifts prior years by 365-day multiples (~1 day leap drift per
  4 years, as in rca-data-tools).

## Roadmap

1. POST drafted annotations to M2M behind a confirmation step
2. Click a heatmap profile → depth-curve inspector
3. Annotation table row → plot span linking
4. `localRanges.yaml` as a third y-range option
5. Persist stream overview caches to disk

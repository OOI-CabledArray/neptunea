# Neptunea

Interactive [Panel](https://panel.holoviz.org) app for QC of OOI Regional
Cabled Array data — hover and zoom to pinpoint when a data problem starts.
Companion to [rca-data-tools](https://github.com/OOI-CabledArray/rca-data-tools).
Named for *Neptunea*, a slow-moving marine snail.

Hosted at **https://neptunea.ooi-rca.net** (weekdays 7am–7pm Pacific);
see [deploy/README.md](deploy/README.md).

## Tabs

- **Curated Products** — CRESST releases listed by `catalog.json` /
  `manifest.json` in `rca-advanced-qaqc` (`cresst-one-easy-rca/`, newest
  first), plus zarr products in folders without a catalog (`cresst/`), as
  heatmaps (profilers) or time series (fixed moorings). Colormap, gap masking,
  deployment markers, side-by-side panels. Heatmap color range: drag/type,
  params standard, or fit to the zoomed view.
  - **Profile** — hover a heatmap column for its pressure-vs-value curve,
    following the heatmap's pressure zoom: the nearest profile (or 24 h bin)
    with data for the variable, plus T and S from the same column (the upcast
    in profile products; pH/pCO2 come from the following downcast). Click to
    pin a profile and overlay that cast's raw samples from `ooi-data`: the
    variable's instrument (pH/pCO2 on the downcast, with sample count and
    largest pressure gap) and raw CTD T/S over the same window. "Follow hover"
    unpins.
  - **Compare** against the same product at a lower QC level (`ooi` → `qartod`
    → `hitl` → `advanced`), another release/build, or another product in the
    same release. Time series: the comparison in gray, points only one side has
    colored (removed by QARTOD / HITL + adjustments / advanced pH, or only in
    one build), and an A − B panel when the grids overlap. Heatmaps: both
    products with linked zoom, plus a changes panel for builds/levels of one
    product. Hovering a difference shows the mask or adjustment annotations
    that cover it (A's for points A dropped, B's for points B dropped), or
    that none does.
  - **Annotations** — HITL annotations and adjustments colored by curation
    decision (mask / ignore / superseded / adjustment; legend toggles each),
    rationale on hover. Releases use their own `annotations.csv` /
    `adjustments.csv` and the per-variable `hitl_masks` attribute (node
    wildcards and salinity inheritance resolved); older builds use
    [rca-cicoes-cresst](https://github.com/ooi-data/rca-cicoes-cresst) main and
    warn when the ledger has changed since the build.
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

- Plot standards (ranges, colormaps, profile line colors from `lineColor`)
  come from rca-data-tools `params/` (local checkout, else GitHub raw);
  climatology from `~/repos/qc-lookup` QARTOD tables.
- Caching: curated product list and annotations refresh every 10 min; release
  manifests and files are kept until restart (releases are immutable); curated
  variables load into memory; up to 6 open streams are kept. Stream overviews
  (one full scan, ~2 min) and y-ranges persist in `~/.cache/neptunea` for 7 and
  30 days; on start, overviews used in the last 30 days are recomputed in the
  background if expired. Overviews may lag appended data by up to 7 days;
  zooms read live.
- Raw casts locate their window by bisecting the stream's time chunks (~4
  chunk reads) rather than loading its time index.
- Callbacks run on 4 threads (`pn.config.nthreads`), so a slow S3 read in one
  session doesn't stall others.
- Time Machine shifts prior years by 365-day multiples (~1 day leap drift per
  4 years, as in rca-data-tools).

## Roadmap

1. POST drafted annotations to M2M behind a confirmation step
2. Annotation table row → plot span linking
3. `localRanges.yaml` as a third y-range option
4. Nearest-neighbor comparison tab
   - Pick an instrument; neighbors listed from `sitesDictionary.csv` (`nearestNeighbors`)
   - Overlay of both series on a shared hourly-median grid, plus a difference panel with deployment markers
   - Variables paired by `variableParameters.csv` standard names (e.g. PHSENH `ph_total` ↔ PHSEN `ph_seawater`)
   - Prototype on hard-coded pairs for science-lead review, then restore the `nearestNeighbors` column
   - Candidate pairs: co-located CTDs, PHSENH ↔ PHSEN, shallow-profiler CTD ↔ 200 m platform CTD, CTD oxygen ↔ standalone optodes
   - Once pairs settle: PNGs per pair in the static dashboard, and a Prefect difference check feeding alerts
5. Curation review: follow the rca-cicoes-cresst release work
   - Before/after curation lights up once a release publishes `ooi` / `qartod` levels
   - Check `hitl_masks` spans against the first release that carries them (v2026.10 doesn't)
   - Release notes and `build_report.json` (% removed per level and year) beside a build comparison, filtered to the variable
6. Precomputed stream summaries (min/max/mean/count per 1 min / 1 h / 1 day bin) written by the harvest; see [docs/on_sharding_and_viz_performance.md](docs/on_sharding_and_viz_performance.md)

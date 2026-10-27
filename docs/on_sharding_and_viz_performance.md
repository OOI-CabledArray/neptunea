# Sharding and visualization performance

Notes from profiling the Full Time Series view, October 2026.

**Bottleneck: IO volume, set by chunk shape.** Decompression and rendering take
milliseconds. Fewer bytes per view is where the gains are.

## Measurements

Stream: `RS03AXPS-SF03A-2A-CTDPFA302-streamed-ctdpf_sbe43_sample` (`ooi-data`).
318M points per variable, 27 chunks of 12M values (~140 days at 1 Hz), float64,
Blosc zstd level 3 + bitshuffle, 15.5 GB store, zarr v2 format.

Per 12M-point chunk (laptop):

| Step | Cost |
|---|---|
| S3 object size | 25 MB `time`, 56.5 MB `sea_water_temperature` (96 MB each decoded) |
| Decompress | ~20 ms |
| Datashader aggregation (6000×400) | ~24 ms |

Compute on the r7g.large (2 vCPU) is a few times slower, still well under a second.
Laptop S3 throughput isn't representative of EC2, so per-view costs are given as bytes:

| View | Bytes read |
|---|---|
| Open stream (full time index) | ~675 MB fetched, ~2.5 GB RAM |
| Full-range overview | ~2.2 GB |
| Any zoom narrower than ~140 days | ~80 MB (one whole chunk per variable) |

Notes on [neptunea.py](../rca_panel_app/neptunea.py):

- `get_dataset` cache is unbounded; a few dense streams can fill the 16 GB instance.
- `stream_overview` runs on dask's default threaded scheduler: 2 workers, so ~2 chunk fetches in flight.
- `load_slice` stride doesn't reduce IO; zarr fetches every chunk the slice touches.
  The cache keys on the exact index range, so pans re-fetch.
- All caches are in memory and cleared by the nightly stop.

## Zarr v3

| Change | Effect |
|---|---|
| zarr-python 3 library | Already in use (3.1.6) |
| v3 format, same chunks | Same bytes read; no meaningful change |
| v3 format with sharding | Large gain for zoom |

Sharding: keep 12M-value shards as the S3 objects, with inner chunks of ~100k values
(~1 day at 1 Hz). A one-hour zoom reads ~0.5 MB by byte-range request instead of ~80 MB,
without the thousands of small objects per variable that plain small chunks would create.
Daily appends rewrite the trailing shard.

Sharding doesn't help the full-range overview or the time-index load at open; those need
precomputed summaries (item 6).

## Improvements

App:

1. **Zoom callbacks off the event loop.** `nthreads=4` in `pn.serve`. DynamicMap
   callbacks currently run on Bokeh's event loop, so one session's chunk fetch blocks all sessions.
2. **Chunk-level cache.** Round slices out to chunk boundaries and keep the last few
   decoded chunks in RAM. Pan/zoom within a 140-day block then needs no S3 reads after the first.
3. **Overview concurrency.** Run the overview with ~8 dask threads to approach the
   instance's burst network bandwidth.
4. **Crop the overview to the visible range.** The 6000×400 count image (~9.6 MB) is
   re-sent in full on each zoom-out fallback.
5. **Persist across the nightly stop.** `pn.cache(to_disk=True)` for `stream_overview`
   and `bulk_range`; warm common streams after the 07:00 start.

Pipeline:

6. **Precomputed pyramid.** The Prefect harvest writes a small companion zarr per stream
   with per-bin min/max/mean/count and QARTOD flag counts at a few bin sizes (1 min, 1 h, 1 day).
   - Overview becomes a ~100 KB read.
   - Each zoom reads the level matching the pixel width.
   - Raw data is read only for the final detail window, located through the pyramid's bin
     edges, which also removes the full time-index load.
   - Daily appends only touch the last bins.

   Variant: store the Datashader count grid itself (time × value bins on the standard
   y-range) to keep the density rendering, like map tiles for time series.

## Stack

Panel + HoloViews + Datashader + Bokeh fits this data. At hundreds of millions of points
per stream, data must be reduced server-side before reaching a browser, and HoloViz is the
most mature Python option for that with native xarray/zarr support. Rendering costs
milliseconds, so Plotly/Dash with plotly-resampler or a JS front end (uPlot, deck.gl) would
hit the same IO limit.

## Open items

- EC2 timings: run the chunk profile on the instance for real S3 throughput.

# heimkaup-data

Scheduled tracker for Heimkaup.is inventory. Runs 28 times a day between
08:00 and 23:55 UTC, triggered by a Vercel cron in heimkaup-inventory (see
`src/lib/schedule.ts` there), and writes its output to a **public Vercel Blob store**,
which the [heimkaup-inventory](https://github.com/HlynurHalldorsson/heimkaup-inventory)
dashboard reads from.

Base URL: `https://hgpjw17ir7ovwbei.public.blob.vercel-storage.com`

## Why the data is not in this repo

It used to be. Every run committed the full data set back here, including an
append-only `inventory_all.csv`. On 2026-08-19 that file reached 100 MiB —
GitHub's hard per-file limit — and every push from then on was rejected. The
job still reported success, because the tracker swallowed git's error and the
workflow's own commit step then found a clean tree, so three days of scrapes
were scraped and silently discarded before anyone noticed.

Storing append-only time-series in git was the underlying problem: the repo had
grown to 2 GB and roughly 75 MB of blob content was rewritten every run. It now
lives in Blob, where only the changed files are written.

## Layout in the blob store

| Path | Written | Notes |
|------|---------|-------|
| `current_inventory.json` | every run | Full product list, ~1 MB |
| `current_snapshot.json` | every run | Compact `{id: [stock, price]}`, ~15 KB |
| `product_histories/product_history_<id>.json` | on change | One point per stock/price/name change |
| `sales_events/<YYYY-MM>.json` | on change | Sales sharded by month. `pricePerUnit` is the price paid (discount if active) since 2026-10-25; earlier events used list price |
| `sales_events/index.json` | on change | List of available months |
| `locations.json` | on change | Tracked locations and their path prefixes |

### Locations

Each Heimkaup location is a separate Jiffy warehouse, scraped one after the
other in the same run (see `WAREHOUSES` in the tracker):

| Code | Location | Files |
|------|----------|-------|
| `H495` | Mjódd: Wolt delivery + Nettó Mjódd click & collect | store root (above) |
| `H4600` | Prís Smáratorg, Kópavogur | `warehouses/H4600/…` |
| `H4435` | Nettó Höfn | `warehouses/H4435/…` |

`H495` predates multi-location support, so its files stay at the root. Other
locations use the same layout under their prefix. `locations.json` at the root
lists them all for the dashboard. Warehouse codes come from the Jiffy
`eta/v2/api/zones` endpoint; add a new location there to `WAREHOUSES`.

History files record a point only when something actually changes. Appending
every run stored ~98% duplicate points (961,604 points collapsed to 21,421 in
the migration) and forced a read-modify-write of all ~800 files each run.
Consumers extend the last point to the present using `current_snapshot.json`.

## How a run works

1. `heimkaup_inventory_tracker.py` scrapes the Jiffy API and reads previous
   state back from the public store over plain HTTPS (no token needed).
2. It stages only changed files into `tracker-output/` and lists them in
   `_upload_manifest.json`.
3. `upload_blobs.mjs` pushes exactly those files, using the
   `BLOB_READ_WRITE_TOKEN` repository secret.

A failed upload fails the job. Do not reintroduce error swallowing here — a
green run that persisted nothing is how the outage above went unnoticed.

## Running locally

```bash
pip install requests
python3 heimkaup_inventory_tracker.py \
  --data-dir tracker-output \
  --blob-base https://hgpjw17ir7ovwbei.public.blob.vercel-storage.com

BLOB_READ_WRITE_TOKEN=... node upload_blobs.mjs tracker-output
```

Omit `--blob-base` to run fully against a local directory instead.

# PPP FOIA → Fabric Eventhouse + Lakehouse scripts

## Files

| File | Purpose |
|------|---------|
| `ppp_foia_to_eventhouse.py` | Download source CSVs, schema, read, transform, full-refresh write to Eventhouse **and** a lakehouse Delta table |

A single transformed Spark DataFrame feeds both sinks. When both writes are
enabled it is cached and materialized once, so the CSV read + transforms run a
single time instead of once per destination.

## Configuration

All configurable values live in the **CONFIGURATION** block at the top of
`ppp_foia_to_eventhouse.py`. Edit them in place (or override per call via
`run_pipeline(...)` / `download_dataset(...)` arguments):

| Variable | Purpose |
|----------|---------|
| `SBA_DATASET_ID`, `SBA_CKAN_API_BASE` | Source dataset on [data.sba.gov](https://data.sba.gov/dataset/ppp-foia) (via CKAN) |
| `DOWNLOAD_DIR` | Where files are downloaded (e.g. `/lakehouse/default/Files/ppp_foia`) |
| `DOWNLOAD_FORMATS` | Resource formats to fetch (`("CSV",)`; add `"XLSX"` for the data dictionary) |
| `DOWNLOAD_OVERWRITE`, `DOWNLOAD_FILE_LIMIT`, `DOWNLOAD_CHUNK_BYTES`, `DOWNLOAD_MAX_WORKERS` | Download behavior (incl. number of parallel downloads) |
| `DOWNLOAD_BEFORE_RUN` | If `True`, `run_pipeline()` downloads before reading |
| `CSV_PATH`, `ROW_LIMIT`, `SPARK_SHUFFLE_PARTITIONS` | Spark read settings (defaults to `DOWNLOAD_DIR`) |
| `KUSTO_URI`, `KUSTO_DATABASE`, `KUSTO_TABLE`, `WRITE_TO_EVENTHOUSE` | Target Eventhouse / KQL database + table (and whether to write it) |
| `LAKEHOUSE_TABLE`, `WRITE_TO_LAKEHOUSE` | Target managed Delta table in the attached lakehouse (and whether to write it) |

## Downloading the source data

The script discovers the current download URLs from the SBA CKAN API
(`package_show`), so the date-suffixed filenames (e.g. `public_150k_plus_240930.csv`)
don't have to be hardcoded — it stays correct when SBA refreshes the dataset.

```python
%run ppp_foia_to_eventhouse
download_dataset()                       # all CSVs → DOWNLOAD_DIR (~5+ GB total)
download_dataset(file_limit=1)           # grab one file for a quick test
download_dataset(max_workers=16)         # more parallelism on a fast link
```

Files download concurrently (up to `DOWNLOAD_MAX_WORKERS` threads, default 8),
which is much faster than serial fetching for this multi-file, multi-GB dataset.
Each download streams to a `.part` file and atomically renames on completion;
same-size files already present are skipped unless `DOWNLOAD_OVERWRITE=True`. If
any file fails, the rest still finish and a single error lists what failed.

## Fabric setup

1. Upload `ppp_foia_to_eventhouse.py` to your Lakehouse **Files**.
2. Edit the **CONFIGURATION** block (at minimum `KUSTO_URI` and `KUSTO_DATABASE`).
3. Install the Kusto management SDK once per environment (for `.clear table` before append):

```python
!pip install azure-kusto-data --quiet
```

4. Attach a default lakehouse to the notebook (for the Delta table) and `%run` the script:

```python
%run ppp_foia_to_eventhouse
download_dataset()                               # pull source CSVs
# sample test — transform only, no writes:
df = run_pipeline(spark, write_to_eventhouse=False, write_to_lakehouse=False)
df.show(5, truncate=False)
```

Production (full refresh of both targets — Eventhouse `.clear` + Spark append,
lakehouse Delta `overwrite`):

```python
df = run_pipeline(spark)                          # both writes (config defaults)
# or download + load in one step:
df = run_pipeline(spark, download_first=True)
```

Write to just one target by toggling the flags (per call or in CONFIGURATION):

```python
df = run_pipeline(spark, write_to_lakehouse=False)   # Eventhouse only
df = run_pipeline(spark, write_to_eventhouse=False)  # lakehouse Delta only
```

`write_staging=True/False` still works as an alias for `write_to_eventhouse`.

## Sample validation

Point `CSV_PATH` at a folder or `ppp_foia_sample.csv` and use `write_to_eventhouse=False`:

- `Recipient_Display` is uppercase.
- Rows with `ForgivenessDate` show `LoanStatus_Display` like `Forgiven as of Jan. 13, 2022` (AP style months).
- `searchtext` for loan `9677497701` contains `synovus` and `29456`.

## Power BI search

Filter with `CONTAINSSTRING(ppp_loans[searchtext], LOWER(SearchInput))` on a single search parameter or slicer.

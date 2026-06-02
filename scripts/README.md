# PPP FOIA → Fabric Eventhouse scripts

## Files

| File | Purpose |
|------|---------|
| `ppp_foia_to_eventhouse.py` | Schema, read CSV, transform, full-refresh write to Eventhouse |

## Fabric setup

1. Upload `ppp_foia_to_eventhouse.py` to your Lakehouse **Files**.
2. Upload the full PPP CSV to Fabric (already done per your environment).
3. In a **prior notebook cell**, set `CSV_PATH`, `KUSTO_URI`, `KUSTO_DATABASE`, `KUSTO_TABLE`, and optionally `SPARK_SHUFFLE_PARTITIONS` / `ROW_LIMIT`.
4. Install the Kusto management SDK once per environment (for `.clear table` before append):

```python
!pip install azure-kusto-data --quiet
```

5. Open an Eventhouse-related notebook and `%run` the script:

```python
%run ppp_foia_to_eventhouse
df = run_pipeline(spark, write_to_eventhouse=False)  # sample test
df.show(5, truncate=False)
```

Production (clears `KUSTO_TABLE`, then Spark append — no separate KQL cell):

```python
df = run_pipeline(spark, write_to_eventhouse=True)
```

`write_staging=True/False` still works as an alias for `write_to_eventhouse`.

## Sample validation

Point `CSV_PATH` at a folder or `ppp_foia_sample.csv` and use `write_to_eventhouse=False`:

- `Recipient` is uppercase.
- Rows with `ForgivenessDate` show `LoanStatusDisplay` like `Forgiven as of Jan. 13, 2022`.
- `searchtext` for loan `9677497701` contains `synovus` and `29456`.

## Power BI search

Filter with `CONTAINSSTRING(ppp_loans[searchtext], LOWER(SearchInput))` on a single search parameter or slicer.

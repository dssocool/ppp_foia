# PPP FOIA → Fabric Eventhouse scripts

## Files

| File | Purpose |
|------|---------|
| `ppp_foia_to_eventhouse.py` | Schema, read CSV, transform, write KQL staging, full-refresh KQL |

## Fabric setup

1. Upload `ppp_foia_to_eventhouse.py` to your Lakehouse **Files**.
2. Upload the full PPP CSV to Fabric (already done per your environment).
3. In a **prior notebook cell**, set `CSV_PATH`, `KUSTO_URI`, `KUSTO_DATABASE`, `KUSTO_TABLE`, `STAGING_TABLE`, and optionally `SPARK_SHUFFLE_PARTITIONS` / `ROW_LIMIT`.
4. Open an Eventhouse-related notebook and `%run` the script:

```python
%run ppp_foia_to_eventhouse
df = run_pipeline(spark, write_staging=False)  # sample test
df.show(5, truncate=False)
```

Production:

```python
df = run_pipeline(spark, write_staging=True)
```

Then in a **KQL** cell:

```kusto
.set-or-replace ppp_loans with (recreate_schema = true) <| ppp_loans_staging
.drop table ppp_loans_staging ifexists
```

## Sample validation

Point `CSV_PATH` at a folder or `ppp_foia_sample.csv` and use `write_staging=False`:

- `Recipient` is uppercase.
- Rows with `ForgivenessDate` show `LoanStatusDisplay` like `Forgiven as of Jan. 13, 2022`.
- `searchtext` for loan `9677497701` contains `synovus` and `29456`.

## Power BI search

Filter with `CONTAINSSTRING(ppp_loans[searchtext], LOWER(SearchInput))` on a single search parameter or slicer.

"""
PPP FOIA CSV → Fabric Eventhouse (KQL) + Lakehouse (Delta) pipeline.

Run in a Fabric notebook (PySpark). All configurable values live in the
CONFIGURATION block below — edit them there (or override per call via
run_pipeline(...) arguments). Then:

    download_dataset()          # pull the latest CSVs from data.sba.gov
    df = run_pipeline(spark)    # read + transform + full-refresh both targets

Source data: https://data.sba.gov/dataset/ppp-foia

One transform feeds both sinks: the transformed Spark DataFrame is cached and
materialized once, then written to the Eventhouse KQL table and the lakehouse
Delta table (so the CSV read + transforms aren't recomputed per write).

Full refresh: .clear table on the Eventhouse target (azure-kusto-data) then
Spark append; overwrite (overwriteSchema) for the lakehouse Delta table.
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# ---------------------------------------------------------------------------
# Imports (Fabric notebook provides `spark` and `mssparkutils`)
# ---------------------------------------------------------------------------
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ===========================================================================
# CONFIGURATION — edit these (or pass overrides to run_pipeline / download_*).
# ===========================================================================

# --- Source download (SBA PPP FOIA dataset on data.sba.gov / CKAN) ---------
SBA_DATASET_PAGE_URL = "https://data.sba.gov/dataset/ppp-foia"
# CKAN API used to discover the current resource (file) download URLs so the
# date-suffixed filenames don't have to be hardcoded.
SBA_CKAN_API_BASE = "https://data.sba.gov/api/3/action"
SBA_DATASET_ID = "ppp-foia"

# Where downloaded files land. Use a real/mounted path (open() writes here).
# In Fabric, the default lakehouse Files folder is mounted at /lakehouse/default/Files.
DOWNLOAD_DIR = "/lakehouse/default/Files/ppp_foia"
# Resource formats to download (as reported by CKAN). Add "XLSX" to also grab
# the "PPP Data Dictionary". Set to None/() to download every resource.
DOWNLOAD_FORMATS = ("CSV",)
DOWNLOAD_OVERWRITE = False  # re-download even if a same-size file already exists
DOWNLOAD_FILE_LIMIT = None  # cap number of files (handy for a quick test)
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024  # streaming read/write chunk size
DOWNLOAD_MAX_WORKERS = 8  # parallel file downloads (network I/O-bound)
DOWNLOAD_BEFORE_RUN = False  # if True, run_pipeline() downloads first
_DOWNLOAD_USER_AGENT = "ppp-foia-pipeline/1.0 (+https://data.sba.gov/dataset/ppp-foia)"

# --- Source read (Spark) ----------------------------------------------------
# Folder of CSVs, a base folder with dated subfolders, or a single .csv file.
# Defaults to the download target so download_dataset() + run_pipeline() align.
CSV_PATH = DOWNLOAD_DIR
ROW_LIMIT = None  # set an int (e.g. 1000) for a sample run; None = all rows
SPARK_SHUFFLE_PARTITIONS = 200

# --- Target Eventhouse / KQL database --------------------------------------
KUSTO_URI = ""  # e.g. "https://<cluster>.kusto.fabric.microsoft.com"
KUSTO_DATABASE = ""  # Eventhouse / KQL database name
KUSTO_TABLE = "ppp_loans"  # target KQL table
WRITE_TO_EVENTHOUSE = True  # full-refresh write to the KQL table

KUSTO_FORMAT = "com.microsoft.kusto.spark.synapse.datasource"

# --- Target Lakehouse (Delta) table ----------------------------------------
# Managed Delta table in the attached default lakehouse (Tables/<name>). Fed by
# the SAME transformed Spark DataFrame as the Eventhouse write (see run_pipeline).
LAKEHOUSE_TABLE = "ppp_loans"  # target Delta table name
WRITE_TO_LAKEHOUSE = True  # full-refresh (overwrite) write to the Delta table

# Sentinel so run_pipeline can tell "argument not passed" from an explicit None.
_UNSET = object()

# ---------------------------------------------------------------------------
# Explicit Spark schema for SBA PPP FOIA CSV (53 columns)
# ---------------------------------------------------------------------------
PPP_DATE_COLUMNS = ("DateApproved", "LoanStatusDate", "ForgivenessDate")

PPP_SCHEMA = StructType(
    [
        StructField("LoanNumber", StringType(), True),
        StructField("DateApproved", DateType(), True),
        StructField("SBAOfficeCode", StringType(), True),
        StructField("ProcessingMethod", StringType(), True),
        StructField("BorrowerName", StringType(), True),
        StructField("BorrowerAddress", StringType(), True),
        StructField("BorrowerCity", StringType(), True),
        StructField("BorrowerState", StringType(), True),
        StructField("BorrowerZip", StringType(), True),
        StructField("LoanStatusDate", DateType(), True),
        StructField("LoanStatus", StringType(), True),
        StructField("Term", IntegerType(), True),
        StructField("SBAGuarantyPercentage", DoubleType(), True),
        StructField("InitialApprovalAmount", DoubleType(), True),
        StructField("CurrentApprovalAmount", DoubleType(), True),
        StructField("UndisbursedAmount", DoubleType(), True),
        StructField("FranchiseName", StringType(), True),
        StructField("ServicingLenderLocationID", StringType(), True),
        StructField("ServicingLenderName", StringType(), True),
        StructField("ServicingLenderAddress", StringType(), True),
        StructField("ServicingLenderCity", StringType(), True),
        StructField("ServicingLenderState", StringType(), True),
        StructField("ServicingLenderZip", StringType(), True),
        StructField("RuralUrbanIndicator", StringType(), True),
        StructField("HubzoneIndicator", StringType(), True),
        StructField("LMIIndicator", StringType(), True),
        StructField("BusinessAgeDescription", StringType(), True),
        StructField("ProjectCity", StringType(), True),
        StructField("ProjectCountyName", StringType(), True),
        StructField("ProjectState", StringType(), True),
        StructField("ProjectZip", StringType(), True),
        StructField("CD", StringType(), True),
        StructField("JobsReported", IntegerType(), True),
        StructField("NAICSCode", StringType(), True),
        StructField("Race", StringType(), True),
        StructField("Ethnicity", StringType(), True),
        StructField("UTILITIES_PROCEED", DoubleType(), True),
        StructField("PAYROLL_PROCEED", DoubleType(), True),
        StructField("MORTGAGE_INTEREST_PROCEED", DoubleType(), True),
        StructField("RENT_PROCEED", DoubleType(), True),
        StructField("REFINANCE_EIDL_PROCEED", DoubleType(), True),
        StructField("HEALTH_CARE_PROCEED", DoubleType(), True),
        StructField("DEBT_INTEREST_PROCEED", DoubleType(), True),
        StructField("BusinessType", StringType(), True),
        StructField("OriginatingLenderLocationID", StringType(), True),
        StructField("OriginatingLender", StringType(), True),
        StructField("OriginatingLenderCity", StringType(), True),
        StructField("OriginatingLenderState", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Veteran", StringType(), True),
        StructField("NonProfit", StringType(), True),
        StructField("ForgivenessAmount", DoubleType(), True),
        StructField("ForgivenessDate", DateType(), True),
    ]
)

PROCEED_COLUMNS = (
    ("PAYROLL_PROCEED", "Payroll_Display"),
    ("UTILITIES_PROCEED", "Utilities_Display"),
    ("MORTGAGE_INTEREST_PROCEED", "MortgageInterest_Display"),
    ("HEALTH_CARE_PROCEED", "HealthCare_Display"),
    ("RENT_PROCEED", "Rent_Display"),
    ("REFINANCE_EIDL_PROCEED", "RefinanceEidl_Display"),
    ("DEBT_INTEREST_PROCEED", "DebtInterest_Display"),
)

# AP style spells out March–July and abbreviates the rest (e.g. Feb., Sept.).
_AP_MONTH_NAMES = {
    1: "Jan.",
    2: "Feb.",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "Aug.",
    9: "Sept.",
    10: "Oct.",
    11: "Nov.",
    12: "Dec.",
}

# Markers Fabric uses when mssparkutils.fs.ls returns absolute mount paths.
_LAKEHOUSE_FILE_MARKERS = ("/Files/", "/files/")


def _normalize_fabric_path(path: str) -> str:
    """
    Normalize paths for mssparkutils.fs and Spark in Fabric.

    mssparkutils.fs.ls often returns absolute mount paths like
    /lakehouse/default/Files/... which fail on the next ls() unless prefixed
    with file:. Prefer lakehouse-relative paths (Files/...) when possible.
    """
    normalized = path.rstrip("/")
    if normalized.startswith(("abfss://", "file:", "Files/", "Tables/")):
        return normalized

    if normalized.startswith("/"):
        lower = normalized.lower()
        for marker in _LAKEHOUSE_FILE_MARKERS:
            idx = lower.find(marker.lower())
            if idx != -1:
                return "Files/" + normalized[idx + len(marker) :]
        return f"file:{normalized}"

    return normalized


def _fs_list_dir(path: str) -> list[tuple[str, bool, float]]:
    """Return (entry_path, is_dir, modify_time) for each child of path."""
    try:
        entries = mssparkutils.fs.ls(_normalize_fabric_path(path))  # noqa: F821 — Fabric global
        return [
            (_normalize_fabric_path(entry.path), entry.isDir, float(entry.modifyTime))
            for entry in entries
        ]
    except NameError:
        base = Path(path)
        if not base.is_dir():
            raise ValueError(f"Expected a directory: {path}") from None
        return [
            (str(child), child.is_dir(), child.stat().st_mtime)
            for child in base.iterdir()
        ]


def _find_latest_subdirectory(base_path: str) -> str:
    """Pick the most recently modified subdirectory under base_path."""
    normalized = base_path.rstrip("/")
    subdirs = [
        (entry_path, modify_time)
        for entry_path, is_dir, modify_time in _fs_list_dir(normalized)
        if is_dir
    ]
    if not subdirs:
        raise ValueError(f"No subdirectories found under {normalized}")

    latest_path, _ = max(subdirs, key=lambda item: item[1])
    return latest_path


def _list_csv_files(directory: str) -> list[str]:
    """Return sorted paths to .csv files in directory (non-recursive)."""
    csv_files = sorted(
        entry_path
        for entry_path, is_dir, _modify_time in _fs_list_dir(directory)
        if not is_dir and entry_path.lower().endswith(".csv")
    )
    if not csv_files:
        raise ValueError(f"No .csv files found in {directory}")
    return csv_files


def resolve_csv_paths(csv_path: str) -> tuple[str, list[str]]:
    """
    Resolve csv_path to concrete CSV file paths.

    - If csv_path ends with .csv, use that file directly.
    - If csv_path is a directory that directly contains .csv files (e.g. the
      download target), load every .csv file in it.
    - Otherwise treat csv_path as a base directory, pick the latest subfolder
      (by modify time), and load every .csv file in that subfolder.
    """
    normalized = _normalize_fabric_path(csv_path)
    if normalized.lower().endswith(".csv"):
        return normalized, [normalized]

    try:
        return normalized, _list_csv_files(normalized)
    except ValueError:
        latest_dir = _find_latest_subdirectory(normalized)
        return latest_dir, _list_csv_files(latest_dir)


def _clean_location_field(column):
    """Treat blank and N/A values as null for city/state."""
    trimmed = F.trim(column)
    return F.when(
        (trimmed.isNull())
        | (trimmed == "")
        | (F.upper(trimmed) == "N/A")
        | (F.upper(trimmed) == "NA"),
        F.lit(None),
    ).otherwise(trimmed)


def _currency_display(amount_column):
    """Format amount as $1,234,567 (no decimal cents)."""
    rounded = F.round(amount_column)
    return F.when(
        amount_column.isNull(),
        F.lit("$0"),
    ).otherwise(F.concat(F.lit("$"), F.format_number(rounded, 0)))


def _ap_month(date_column):
    """Month in AP style: 'Feb.', 'March', 'June', 'Sept.', etc."""
    month = F.month(date_column)
    column = None
    for number, label in _AP_MONTH_NAMES.items():
        condition = month == number
        column = (
            F.when(condition, F.lit(label))
            if column is None
            else column.when(condition, F.lit(label))
        )
    return column


def _ap_date_display(date_column):
    """Format a date in AP style, e.g. 'Feb. 13, 2021' or 'June 29, 2022'."""
    return F.concat(
        _ap_month(date_column),
        F.lit(" "),
        F.date_format(date_column, "d, yyyy"),
    )


# ---------------------------------------------------------------------------
# Download (data.sba.gov / CKAN) — discover resources, then stream to disk
# ---------------------------------------------------------------------------
def _human_bytes(num_bytes: float) -> str:
    """Format a byte count as a short human-readable string (e.g. '431.2 MB')."""
    value = float(num_bytes or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def fetch_dataset_resources(
    dataset_id: str | None = None,
    ckan_api_base: str | None = None,
    formats=_UNSET,
) -> list[dict]:
    """
    Look up the dataset via the CKAN package_show API and return its files.

    Each item is {name, url, size, format}. `formats` filters by CKAN format
    (case-insensitive); pass None/() to return every resource.
    """
    dataset_id = SBA_DATASET_ID if dataset_id is None else dataset_id
    ckan_api_base = SBA_CKAN_API_BASE if ckan_api_base is None else ckan_api_base
    formats = DOWNLOAD_FORMATS if formats is _UNSET else formats

    api_url = (
        f"{ckan_api_base.rstrip('/')}/package_show"
        f"?id={urllib.parse.quote(dataset_id)}"
    )
    request = urllib.request.Request(
        api_url, headers={"User-Agent": _DOWNLOAD_USER_AGENT}
    )
    with urllib.request.urlopen(request) as response:
        payload = json.loads(response.read().decode("utf-8"))

    if not payload.get("success"):
        raise RuntimeError(f"CKAN package_show failed for dataset {dataset_id!r}")

    wanted = {fmt.upper() for fmt in formats} if formats else None
    resources: list[dict] = []
    for resource in payload["result"]["resources"]:
        fmt = (resource.get("format") or "").upper()
        if wanted is not None and fmt not in wanted:
            continue
        resources.append(
            {
                "name": resource.get("name") or resource["id"],
                "url": resource["url"],
                "size": resource.get("size"),
                "format": fmt,
            }
        )
    return resources


def download_file(
    url: str,
    dest_path: str,
    expected_size: int | None = None,
    overwrite: bool | None = None,
    chunk_bytes: int | None = None,
) -> str:
    """
    Stream `url` to `dest_path`. Writes to a .part file then atomically renames.

    Skips the download when a same-size file already exists (unless overwrite).
    """
    overwrite = DOWNLOAD_OVERWRITE if overwrite is None else overwrite
    chunk_bytes = DOWNLOAD_CHUNK_BYTES if chunk_bytes is None else chunk_bytes
    name = os.path.basename(dest_path)

    if (
        not overwrite
        and expected_size
        and os.path.exists(dest_path)
        and os.path.getsize(dest_path) == expected_size
    ):
        print(f"  - skip (already downloaded): {name}")
        return dest_path

    tmp_path = f"{dest_path}.part"
    request = urllib.request.Request(
        url, headers={"User-Agent": _DOWNLOAD_USER_AGENT}
    )
    downloaded = 0
    last_report = time.monotonic()
    with urllib.request.urlopen(request) as response, open(tmp_path, "wb") as handle:
        while True:
            chunk = response.read(chunk_bytes)
            if not chunk:
                break
            handle.write(chunk)
            downloaded += len(chunk)
            now = time.monotonic()
            if now - last_report >= 2:
                if expected_size:
                    pct = downloaded / expected_size * 100
                    print(
                        f"    {name}: {pct:5.1f}% "
                        f"({_human_bytes(downloaded)} / {_human_bytes(expected_size)})"
                    )
                else:
                    print(f"    {name}: {_human_bytes(downloaded)}")
                last_report = now

    os.replace(tmp_path, dest_path)
    print(f"  - done: {name} ({_human_bytes(downloaded)})")
    return dest_path


def download_dataset(
    download_dir: str | None = None,
    dataset_id: str | None = None,
    ckan_api_base: str | None = None,
    formats=_UNSET,
    overwrite: bool | None = None,
    file_limit: int | None = _UNSET,
    chunk_bytes: int | None = None,
    max_workers: int | None = None,
) -> str:
    """
    Download the PPP FOIA files from data.sba.gov into `download_dir`.

    Files are fetched concurrently (network I/O-bound) with up to `max_workers`
    threads. Returns the directory containing the files (ready as csv_path).
    """
    download_dir = DOWNLOAD_DIR if download_dir is None else download_dir
    file_limit = DOWNLOAD_FILE_LIMIT if file_limit is _UNSET else file_limit
    max_workers = DOWNLOAD_MAX_WORKERS if max_workers is None else max_workers

    os.makedirs(download_dir, exist_ok=True)
    resources = fetch_dataset_resources(dataset_id, ckan_api_base, formats)
    if file_limit is not None:
        resources = resources[:file_limit]
    if not resources:
        raise ValueError(
            "No matching resources to download (check DOWNLOAD_FORMATS)."
        )

    total = len(resources)
    total_size = sum(resource["size"] or 0 for resource in resources)
    worker_count = max(1, min(max_workers, total))
    print(
        f"Downloading {total} file(s) (~{_human_bytes(total_size)}) to "
        f"{download_dir} using {worker_count} parallel worker(s)"
    )

    def _download_one(index: int, resource: dict) -> str:
        dest = os.path.join(download_dir, resource["name"])
        print(
            f"[{index}/{total}] start {resource['name']} "
            f"({_human_bytes(resource['size'] or 0)})"
        )
        return download_file(
            resource["url"],
            dest,
            expected_size=resource["size"],
            overwrite=overwrite,
            chunk_bytes=chunk_bytes,
        )

    errors: list[tuple[str, BaseException]] = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_resource = {
            executor.submit(_download_one, index, resource): resource
            for index, resource in enumerate(resources, start=1)
        }
        for future in as_completed(future_to_resource):
            resource = future_to_resource[future]
            try:
                future.result()
            except Exception as exc:  # noqa: BLE001 — collect and report all failures
                errors.append((resource["name"], exc))
                print(f"  - FAILED: {resource['name']}: {exc}")

    if errors:
        names = ", ".join(name for name, _ in errors)
        raise RuntimeError(
            f"Failed to download {len(errors)} of {total} file(s): {names}"
        )

    return download_dir


def read_ppp_csv(
    spark: SparkSession,
    csv_path: str,
    row_limit: int | None = None,
    spark_shuffle_partitions: int = 200,
) -> DataFrame:
    """Read PPP FOIA CSV(s) with explicit schema (no inferSchema)."""
    spark.conf.set("spark.sql.shuffle.partitions", str(spark_shuffle_partitions))

    source_dir, csv_files = resolve_csv_paths(csv_path)
    print(f"Reading {len(csv_files)} CSV file(s) from {source_dir}:")
    for path in csv_files:
        print(f"  - {path}")

    reader = (
        spark.read.format("csv")
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("dateFormat", "MM/dd/yyyy")
        .schema(PPP_SCHEMA)
    )
    df = reader.load(csv_files)

    if row_limit is not None:
        df = df.limit(row_limit)

    return df


def transform_ppp(df: DataFrame) -> DataFrame:
    """Add display columns, searchtext, and retain numeric/date fields for Power BI."""
    project_city = _clean_location_field(F.col("ProjectCity"))
    borrower_city = _clean_location_field(F.col("BorrowerCity"))
    project_state = _clean_location_field(F.col("ProjectState"))
    borrower_state = _clean_location_field(F.col("BorrowerState"))

    city = F.coalesce(project_city, borrower_city)
    state = F.coalesce(project_state, borrower_state)
    zip_raw = F.coalesce(F.trim(F.col("ProjectZip")), F.trim(F.col("BorrowerZip")))
    zip5 = F.regexp_extract(zip_raw, r"^(\d{5})", 1)

    loan_round_suffix = (
        F.when(F.col("ProcessingMethod") == "PPP", F.lit(" (First Round)"))
        .when(F.col("ProcessingMethod") == "PPS", F.lit(" (Second Round)"))
        .otherwise(F.lit(""))
    )

    forgiven_display = F.concat(
        F.lit("Forgiven as of "),
        _ap_date_display(F.col("ForgivenessDate")),
    )

    loan_status_display = (
        F.when(F.col("ForgivenessDate").isNotNull(), forgiven_display)
        .when(
            (F.col("UndisbursedAmount") == 0) & (F.col("LoanStatus") == "Exemption 4"),
            F.lit("Fully Disbursed"),
        )
        .otherwise(F.coalesce(F.col("LoanStatus"), F.lit("Unknown")))
    )

    df = df.withColumn("Recipient_Display", F.upper(F.trim(F.col("BorrowerName"))))
    df = df.withColumn(
        "Location_Display",
        F.when(
            city.isNull() & state.isNull(),
            F.lit(None),
        ).otherwise(
            F.concat_ws(
                ", ",
                F.when(city.isNull(), F.lit(None)).otherwise(city),
                F.when(state.isNull(), F.lit(None)).otherwise(state),
            )
        ),
    )
    df = df.withColumn(
        "AreaType_Display",
        F.when(F.col("RuralUrbanIndicator") == "U", F.lit("Urban"))
        .when(F.col("RuralUrbanIndicator") == "R", F.lit("Rural"))
        .otherwise(F.lit(None)),
    )
    df = df.withColumn("Lender_Display", F.coalesce(F.col("OriginatingLender"), F.col("ServicingLenderName")))
    df = df.withColumn("BusinessAge_Display", F.col("BusinessAgeDescription"))
    # NOTE: source CSV has only NAICSCode (no industry title); displays the code,
    # not the description (e.g. "722513" rather than "Limited-Service Restaurants").
    df = df.withColumn("Industry_Display", F.col("NAICSCode").cast("string"))
    df = df.withColumn("JobsReported_Display", F.col("JobsReported").cast("string"))
    df = df.withColumn("BusinessType_Display", F.trim(F.col("BusinessType")))
    df = df.withColumn("LoanAmount_Display", _currency_display(F.col("CurrentApprovalAmount")))
    df = df.withColumn(
        "ForgivenessAmount_Display",
        F.when(
            F.col("ForgivenessAmount").isNull(),
            F.lit(None),
        ).otherwise(_currency_display(F.col("ForgivenessAmount"))),
    )
    df = df.withColumn(
        "DateApproved_Display",
        F.when(
            F.col("DateApproved").isNull(),
            F.lit(None),
        ).otherwise(
            F.concat(
                _ap_date_display(F.col("DateApproved")),
                loan_round_suffix,
            )
        ),
    )
    df = df.withColumn("LoanStatus_Display", loan_status_display)

    for source_col, display_col in PROCEED_COLUMNS:
        df = df.withColumn(display_col, _currency_display(F.col(source_col)))

    df = df.withColumn(
        "searchtext",
        F.lower(
            F.concat_ws(
                " ",
                F.col("BorrowerName"),
                F.col("OriginatingLender"),
                F.col("ServicingLenderName"),
                zip5,
                F.col("BusinessType"),
                project_city,
                borrower_city,
                project_state,
                borrower_state,
                F.col("NAICSCode").cast("string"),
            )
        ),
    )

    output_columns = [
        "LoanNumber",
        "Recipient_Display",
        "Location_Display",
        "AreaType_Display",
        "Lender_Display",
        "BusinessAge_Display",
        "Industry_Display",
        "JobsReported_Display",
        "BusinessType_Display",
        "LoanAmount_Display",
        "ForgivenessAmount_Display",
        "DateApproved_Display",
        "LoanStatus_Display",
        "searchtext",
        "Payroll_Display",
        "Utilities_Display",
        "MortgageInterest_Display",
        "HealthCare_Display",
        "Rent_Display",
        "RefinanceEidl_Display",
        "DebtInterest_Display",
        "CurrentApprovalAmount",
        "InitialApprovalAmount",
        "ForgivenessAmount",
        "UndisbursedAmount",
        "PAYROLL_PROCEED",
        "UTILITIES_PROCEED",
        "MORTGAGE_INTEREST_PROCEED",
        "RENT_PROCEED",
        "REFINANCE_EIDL_PROCEED",
        "HEALTH_CARE_PROCEED",
        "DEBT_INTEREST_PROCEED",
        "DateApproved",
        "ForgivenessDate",
        "LoanStatusDate",
        "ProcessingMethod",
        "BorrowerName",
        "BorrowerCity",
        "BorrowerState",
        "BorrowerZip",
        "ProjectCity",
        "ProjectState",
        "ProjectZip",
        "OriginatingLender",
        "ServicingLenderName",
        "BusinessType",
        "NAICSCode",
        "JobsReported",
        "LoanStatus",
    ]

    return df.select(*output_columns)


def get_kusto_access_token(kusto_uri: str) -> str:
    """Fabric AAD token for Kusto query/management APIs."""
    try:
        return mssparkutils.credentials.getToken(kusto_uri)  # noqa: F821 — Fabric global
    except Exception:
        return mssparkutils.credentials.getToken("kusto")  # noqa: F821 — Fabric global


_KUSTO_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_kusto_table_name(table: str) -> str:
    if not _KUSTO_TABLE_NAME_RE.match(table):
        raise ValueError(f"Invalid Kusto table name: {table!r}")
    return table


def _kusto_error_texts(exc: BaseException) -> list[str]:
    """Collect human-readable fragments from Kusto exceptions (incl. nested API errors)."""
    texts: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        texts.append(str(current))
        try:
            from azure.kusto.data.exceptions import KustoApiError, KustoMultiApiError, KustoServiceError
        except ImportError:
            current = current.__cause__
            continue

        if isinstance(current, KustoApiError):
            api_error = current.get_api_error()
            for part in (api_error.code, api_error.message, api_error.description, api_error.type):
                if part:
                    texts.append(part)
        elif isinstance(current, KustoMultiApiError):
            for api_error in current.get_api_errors():
                for part in (api_error.code, api_error.message, api_error.description, api_error.type):
                    if part:
                        texts.append(part)
        elif isinstance(current, KustoServiceError):
            texts.append(current.message_text)

        current = current.__cause__

    return texts


def _is_kusto_table_not_found(exc: BaseException) -> bool:
    combined = " ".join(_kusto_error_texts(exc)).lower()
    return (
        "badrequest_entitynotfound" in combined
        or "entitynotfoundexception" in combined
        or ("entity id" in combined and "was not found" in combined)
        or "doesn't exist" in combined
        or "does not exist" in combined
        or "wasn't found" in combined
        or "not found" in combined
        or "unknown table" in combined
        or "notfound" in combined
        or "entitynotfound" in combined
    )


def _import_kusto_client():
    try:
        from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
    except ImportError as exc:
        raise ImportError(
            "Kusto management requires azure-kusto-data. "
            "In Fabric: !pip install azure-kusto-data --quiet"
        ) from exc
    return KustoClient, KustoConnectionStringBuilder


def _open_kusto_client(kusto_uri: str, access_token: str | None = None):
    KustoClient, KustoConnectionStringBuilder = _import_kusto_client()
    token = access_token or get_kusto_access_token(kusto_uri)
    kcsb = KustoConnectionStringBuilder.with_aad_application_token_authentication(
        kusto_uri, token
    )
    return KustoClient(kcsb), token


def _spark_field_to_kql_type(field: StructField) -> str:
    data_type = field.dataType
    if isinstance(data_type, StringType):
        return "string"
    if isinstance(data_type, IntegerType):
        return "int"
    if isinstance(data_type, DoubleType):
        return "real"
    if isinstance(data_type, DateType):
        return "datetime"
    raise ValueError(f"Unsupported Spark type for Kusto: {field.name} ({data_type})")


def _kusto_table_exists(client, database: str, table: str) -> bool:
    table = _validate_kusto_table_name(table)
    try:
        client.execute_mgmt(database, f".show table {table} cslschema")
        return True
    except Exception as exc:
        if _is_kusto_table_not_found(exc):
            return False
        raise


def ensure_kusto_table(
    client,
    database: str,
    table: str,
    df: DataFrame,
) -> bool:
    """
    Create the target table from the DataFrame schema when missing.

    Returns True if a create command was issued, False if the table already existed.
    """
    table = _validate_kusto_table_name(table)
    if _kusto_table_exists(client, database, table):
        return False

    columns = ", ".join(
        f"{field.name}:{_spark_field_to_kql_type(field)}" for field in df.schema.fields
    )
    client.execute_mgmt(database, f".create-merge table {table} ({columns})")
    return True


def clear_kusto_table(
    kusto_uri: str,
    database: str,
    table: str,
    access_token: str | None = None,
    *,
    client=None,
) -> None:
    """
    Remove all rows from a KQL table via management API.

    Skips quietly if the table does not exist yet (first run).
    Requires azure-kusto-data in the notebook environment:
      !pip install azure-kusto-data --quiet
    """
    table = _validate_kusto_table_name(table)
    command = f".clear table {table} data"

    if client is not None:
        if not _kusto_table_exists(client, database, table):
            return
        try:
            client.execute_mgmt(database, command)
        except Exception as exc:
            if _is_kusto_table_not_found(exc):
                return
            raise
        return

    with _open_kusto_client(kusto_uri, access_token)[0] as kusto_client:
        clear_kusto_table(
            kusto_uri,
            database,
            table,
            access_token=access_token,
            client=kusto_client,
        )


def write_to_kusto_table(
    df: DataFrame,
    kusto_uri: str,
    database: str,
    table: str,
    spark_shuffle_partitions: int = 200,
    access_token: str | None = None,
) -> None:
    """Append a DataFrame to a KQL table (Spark Kusto connector)."""
    token = access_token or get_kusto_access_token(kusto_uri)

    (
        df.repartition(spark_shuffle_partitions)
        .write.format(KUSTO_FORMAT)
        .option("kustoCluster", kusto_uri)
        .option("kustoDatabase", database)
        .option("kustoTable", table)
        .option("accessToken", token)
        .option("tableCreateOptions", "CreateIfNotExist")
        .option("clientBatchingLimit", "1024")
        .mode("Append")
        .save()
    )


def write_to_kusto_full_refresh(
    df: DataFrame,
    kusto_uri: str,
    database: str,
    table: str,
    spark_shuffle_partitions: int = 200,
) -> None:
    """Full refresh: clear existing table (if any), ensure table exists, then append."""
    table = _validate_kusto_table_name(table)
    client, token = _open_kusto_client(kusto_uri)
    with client:
        if _kusto_table_exists(client, database, table):
            print(f"Clearing existing rows in '{table}'...")
            clear_kusto_table(
                kusto_uri,
                database,
                table,
                access_token=token,
                client=client,
            )
        else:
            print(f"Table '{table}' not found; creating from DataFrame schema...")
            ensure_kusto_table(client, database, table, df)

        write_to_kusto_table(
            df,
            kusto_uri,
            database,
            table,
            spark_shuffle_partitions=spark_shuffle_partitions,
            access_token=token,
        )


def write_to_lakehouse_table(
    df: DataFrame,
    table: str,
    mode: str = "overwrite",
) -> None:
    """
    Full-refresh a managed Delta table in the attached default lakehouse.

    Uses overwrite + overwriteSchema so a changed output schema replaces the
    table cleanly (mirrors the Eventhouse full-refresh semantics). Writes to
    Tables/<table> via the Spark metastore (saveAsTable).
    """
    (
        df.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )


def run_pipeline(
    spark: SparkSession,
    csv_path: str | None = None,
    kusto_uri: str | None = None,
    kusto_database: str | None = None,
    kusto_table: str | None = None,
    lakehouse_table: str | None = None,
    row_limit: int | None = _UNSET,
    download_first: bool | None = None,
    write_to_eventhouse: bool | None = None,
    write_to_lakehouse: bool | None = None,
    write_staging: bool | None = None,
    spark_shuffle_partitions: int | None = None,
) -> DataFrame:
    """Optionally download, then read CSV, transform, and full-refresh the targets.

    The transformed DataFrame is written to the Eventhouse (KQL) table and/or a
    lakehouse Delta table. When both targets are enabled the DataFrame is cached
    and materialized once, so the CSV read + transforms aren't recomputed per write.

    Unset arguments fall back to the CONFIGURATION values at the top of the file,
    so they pick up edits/reassignments made before the call.
    """
    csv_path = CSV_PATH if csv_path is None else csv_path
    kusto_uri = KUSTO_URI if kusto_uri is None else kusto_uri
    kusto_database = KUSTO_DATABASE if kusto_database is None else kusto_database
    kusto_table = KUSTO_TABLE if kusto_table is None else kusto_table
    lakehouse_table = LAKEHOUSE_TABLE if lakehouse_table is None else lakehouse_table
    row_limit = ROW_LIMIT if row_limit is _UNSET else row_limit
    download_first = DOWNLOAD_BEFORE_RUN if download_first is None else download_first
    write_to_eventhouse = (
        WRITE_TO_EVENTHOUSE if write_to_eventhouse is None else write_to_eventhouse
    )
    write_to_lakehouse = (
        WRITE_TO_LAKEHOUSE if write_to_lakehouse is None else write_to_lakehouse
    )

    if write_staging is not None:
        write_to_eventhouse = write_staging

    if spark_shuffle_partitions is None:
        spark_shuffle_partitions = SPARK_SHUFFLE_PARTITIONS

    if download_first:
        csv_path = download_dataset()

    raw = read_ppp_csv(
        spark,
        csv_path,
        row_limit=row_limit,
        spark_shuffle_partitions=spark_shuffle_partitions,
    )
    df = transform_ppp(raw)

    # Cache + materialize once when feeding multiple sinks so the (expensive)
    # CSV read and transforms run a single time instead of once per write.
    cached = write_to_eventhouse and write_to_lakehouse
    if cached:
        df = df.cache()
        row_count = df.count()  # action that populates the cache before writes
        print(f"Cached {row_count:,} transformed rows for multi-target write.")

    try:
        if write_to_eventhouse:
            if not kusto_uri or not kusto_database:
                raise ValueError(
                    "Set kusto_uri and kusto_database before writing to Eventhouse"
                )
            print(f"Full refresh: clear + append to Eventhouse '{kusto_table}'...")
            write_to_kusto_full_refresh(
                df,
                kusto_uri,
                kusto_database,
                kusto_table,
                spark_shuffle_partitions=spark_shuffle_partitions,
            )

        if write_to_lakehouse:
            print(f"Full refresh: overwrite lakehouse table '{lakehouse_table}'...")
            write_to_lakehouse_table(df, lakehouse_table)
    finally:
        if cached:
            df.unpersist()

    return df


# ---------------------------------------------------------------------------
# Fabric notebook — edit the CONFIGURATION block at the top, then:
#   !pip install azure-kusto-data --quiet   # once per environment
#   %run ppp_foia_to_eventhouse
#   download_dataset()                       # pull CSVs from data.sba.gov
#   df = run_pipeline(spark)                 # or run_pipeline(spark, download_first=True)
#   df.show(5, truncate=False)
# ---------------------------------------------------------------------------

# =============================================================================
# VALIDATION (sample run)
# =============================================================================
# 1. Point CSV_PATH at the download folder, a dated subfolder, or a single .csv.
# 2. Disable the writes to inspect only:
#      df = run_pipeline(spark, write_to_eventhouse=False, write_to_lakehouse=False)
# 3. Checks on sample data:
#    - Recipient_Display is uppercase (e.g. NORTH CHARLESTON HOSPITALITY GROUP LLC)
#    - LoanStatus_Display starts with "Forgiven as of" when ForgivenessDate is set
#      (AP style months, e.g. "Forgiven as of June 29, 2022")
#    - searchtext contains "synovus" and zip fragment "29456" for row 9677497701
#
# Full run: restore full CSV path, ROW_LIMIT=None, both writes enabled.
# If the output schema changes: for Eventhouse, drop the target table once in the
# query UI so CreateIfNotExist recreates it; the lakehouse write uses
# overwriteSchema and replaces the Delta table schema automatically.
#
# =============================================================================
# POWER BI — single search bar on searchtext
# =============================================================================
# Use a slicer or parameter SearchInput on the report page, then filter visuals:
#
#   SearchFilter =
#   VAR q = LOWER( TRIM( SearchInput ) )
#   RETURN
#       IF ( q = "", TRUE(), CONTAINSSTRING ( ppp_loans[searchtext], q ) )
#
# Or as a table filter: keep rows where CONTAINSSTRING(ppp_loans[searchtext], LOWER(q)).
# Bind list cards to: Recipient_Display, Location_Display, LoanStatus_Display,
# LoanAmount_Display, DateApproved_Display; detail page uses the proceed *_Display
# columns plus ForgivenessAmount_Display, Lender_Display, AreaType_Display,
# BusinessAge_Display, Industry_Display (NAICS code), BusinessType_Display,
# JobsReported_Display.
# =============================================================================

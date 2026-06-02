"""
PPP FOIA CSV → Fabric Eventhouse (KQL) pipeline.

Run in a Fabric notebook (PySpark). Set CSV_PATH, KUSTO_URI, etc. in a cell
before %run; then call run_pipeline(spark).

Full refresh: .clear table on the target (azure-kusto-data), then Spark append
to the target table. No staging table or separate KQL notebook cell.
"""

from __future__ import annotations

import re
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

KUSTO_FORMAT = "com.microsoft.kusto.spark.synapse.datasource"

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
    ("PAYROLL_PROCEED", "PayrollDisplay"),
    ("UTILITIES_PROCEED", "UtilitiesDisplay"),
    ("MORTGAGE_INTEREST_PROCEED", "MortgageInterestDisplay"),
    ("HEALTH_CARE_PROCEED", "HealthCareDisplay"),
    ("RENT_PROCEED", "RentDisplay"),
    ("REFINANCE_EIDL_PROCEED", "RefinanceEidlDisplay"),
    ("DEBT_INTEREST_PROCEED", "DebtInterestDisplay"),
)

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
    - Otherwise treat csv_path as a base directory, pick the latest subfolder
      (by modify time), and load every .csv file in that subfolder.
    """
    normalized = _normalize_fabric_path(csv_path)
    if normalized.lower().endswith(".csv"):
        return normalized, [normalized]

    latest_dir = _find_latest_subdirectory(normalized)
    csv_files = _list_csv_files(latest_dir)
    return latest_dir, csv_files


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
        F.date_format(F.col("ForgivenessDate"), "MMM"),
        F.lit(". "),
        F.date_format(F.col("ForgivenessDate"), "d, yyyy"),
    )

    loan_status_display = (
        F.when(F.col("ForgivenessDate").isNotNull(), forgiven_display)
        .when(
            (F.col("UndisbursedAmount") == 0) & (F.col("LoanStatus") == "Exemption 4"),
            F.lit("Fully Disbursed"),
        )
        .otherwise(F.coalesce(F.col("LoanStatus"), F.lit("Unknown")))
    )

    df = df.withColumn("Recipient", F.upper(F.trim(F.col("BorrowerName"))))
    df = df.withColumn(
        "Location",
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
        "AreaType",
        F.when(F.col("RuralUrbanIndicator") == "U", F.lit("Urban"))
        .when(F.col("RuralUrbanIndicator") == "R", F.lit("Rural"))
        .otherwise(F.lit(None)),
    )
    df = df.withColumn("Lender", F.coalesce(F.col("OriginatingLender"), F.col("ServicingLenderName")))
    df = df.withColumn("BusinessAge", F.col("BusinessAgeDescription"))
    df = df.withColumn("Industry", F.col("NAICSCode").cast("string"))
    df = df.withColumn("JobsReportedDisplay", F.col("JobsReported").cast("string"))
    df = df.withColumn("BusinessTypeDisplay", F.trim(F.col("BusinessType")))
    df = df.withColumn("LoanAmount", _currency_display(F.col("CurrentApprovalAmount")))
    df = df.withColumn(
        "DateApprovedDisplay",
        F.when(
            F.col("DateApproved").isNull(),
            F.lit(None),
        ).otherwise(
            F.concat(
                F.date_format(F.col("DateApproved"), "MMMM d, yyyy"),
                loan_round_suffix,
            )
        ),
    )
    df = df.withColumn("LoanStatusDisplay", loan_status_display)

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
        "Recipient",
        "Location",
        "AreaType",
        "Lender",
        "BusinessAge",
        "Industry",
        "JobsReportedDisplay",
        "BusinessTypeDisplay",
        "LoanAmount",
        "DateApprovedDisplay",
        "LoanStatusDisplay",
        "searchtext",
        "PayrollDisplay",
        "UtilitiesDisplay",
        "MortgageInterestDisplay",
        "HealthCareDisplay",
        "RentDisplay",
        "RefinanceEidlDisplay",
        "DebtInterestDisplay",
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


def run_pipeline(
    spark: SparkSession,
    csv_path: str = CSV_PATH,
    kusto_uri: str = KUSTO_URI,
    kusto_database: str = KUSTO_DATABASE,
    kusto_table: str = KUSTO_TABLE,
    row_limit: int | None = ROW_LIMIT,
    write_to_eventhouse: bool = True,
    write_staging: bool | None = None,
    spark_shuffle_partitions: int | None = None,
) -> DataFrame:
    """Read CSV, transform, optionally full-refresh write to the target KQL table."""
    if write_staging is not None:
        write_to_eventhouse = write_staging

    if spark_shuffle_partitions is None:
        spark_shuffle_partitions = globals().get("SPARK_SHUFFLE_PARTITIONS", 200)
    raw = read_ppp_csv(
        spark,
        csv_path,
        row_limit=row_limit,
        spark_shuffle_partitions=spark_shuffle_partitions,
    )
    df = transform_ppp(raw)

    if write_to_eventhouse:
        if not kusto_uri or not kusto_database:
            raise ValueError("Set kusto_uri and kusto_database before writing to Eventhouse")
        print(f"Full refresh: clear + append to '{kusto_table}'...")
        write_to_kusto_full_refresh(
            df,
            kusto_uri,
            kusto_database,
            kusto_table,
            spark_shuffle_partitions=spark_shuffle_partitions,
        )

    return df


# ---------------------------------------------------------------------------
# Fabric notebook — set config in a prior cell, then:
#   !pip install azure-kusto-data --quiet   # once per environment
#   %run ppp_foia_to_eventhouse
#   df = run_pipeline(spark)
#   df.show(5, truncate=False)
# ---------------------------------------------------------------------------

# =============================================================================
# VALIDATION (sample run)
# =============================================================================
# 1. Set csv_path (CSV_PATH) to a folder or a single .csv file in a prior cell.
# 2. Set write_to_eventhouse=False to inspect:
#      df = run_pipeline(spark, write_to_eventhouse=False)
# 3. Checks on sample data:
#    - Recipient is uppercase (e.g. NORTH CHARLESTON HOSPITALITY GROUP LLC)
#    - LoanStatusDisplay starts with "Forgiven as of" when ForgivenessDate is set
#    - searchtext contains "synovus" and zip fragment "29456" for row 9677497701
#
# Full run: restore full CSV path, ROW_LIMIT=None, write_to_eventhouse=True.
# If the output schema changes, drop the target table once in Eventhouse query UI,
# then re-run so CreateIfNotExist recreates it from the DataFrame schema.
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
# Bind list cards to: Recipient, Location, LoanStatusDisplay, LoanAmount,
# DateApprovedDisplay; detail page uses proceed *Display columns and Lender, AreaType,
# BusinessAge, Industry (NAICS code), BusinessTypeDisplay, JobsReportedDisplay.
# =============================================================================

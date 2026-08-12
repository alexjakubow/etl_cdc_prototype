from pyspark import pipelines as dp
from pyspark.sql import functions as F

# =============================================================================
# Generic Silver Layer Pipeline
# =============================================================================
# Reads from bronze streaming tables, applies deduplication and null removal,
# then upserts (SCD Type 1) into silver target tables named {table_name}_silver.
#
# Pipeline Parameters (set in pipeline configuration):
#   group_code  - Integer. Fetches table list from control table. Takes precedence.
#   table_name  - Comma-separated list of table names (one or more).
#
# Validation:
#   - At least one of group_code or table_name must be provided.
#   - group_code must be a single integer value.
#   - group_code takes precedence over table_name when both are provided.
# =============================================================================

# --- Constants ---
CONTROL_TABLE = "catadb360dev.utilities.job_control_info"

import json

# --- Read pipeline parameters ---
group_code_param = spark.conf.get("group_code", "")
table_name_param = spark.conf.get("table_name", "")

# --- Parameter Validation ---
if not group_code_param and not table_name_param:
    raise ValueError(
        "At least one of 'group_code' or 'table_name' pipeline parameter must be provided."
    )

group_code = None
if group_code_param:
    if "," in group_code_param:
        raise ValueError(
            f"Only one 'group_code' value is allowed. Got: '{group_code_param}'"
        )
    try:
        group_code = int(group_code_param.strip())
    except ValueError:
        raise ValueError(
            f"'group_code' must be an integer value. Got: '{group_code_param}'"
        )

# --- Resolve table list ---
tables_to_ingest = []  # list of (table_name, catalog, schema, metadata) tuples

if group_code is not None:
    control_rows = (
        spark.read.table(CONTROL_TABLE)
        .filter(F.col("group_code") == group_code)
        .orderBy("seq_num")
        .select("table_name", "catalog", "schema", "metadata")
        .collect()
    )
    if not control_rows:
        raise ValueError(
            f"No tables found in control table for group_code={group_code}"
        )
    tables_to_ingest = [(row.table_name, row.catalog, row.schema, row.metadata) for row in control_rows]
else:
    table_names = [t.strip() for t in table_name_param.split(",") if t.strip()]
    if not table_names:
        raise ValueError("'table_name' parameter is empty or contains only whitespace.")

    control_rows = (
        spark.read.table(CONTROL_TABLE)
        .filter(F.col("table_name").isin(table_names))
        .select("table_name", "catalog", "schema", "metadata")
        .collect()
    )
    found_map = {row.table_name: (row.catalog, row.schema, row.metadata) for row in control_rows}
    missing = [t for t in table_names if t not in found_map]
    if missing:
        raise ValueError(
            f"Tables not found in control table: {missing}"
        )
    tables_to_ingest = [(name, found_map[name][0], found_map[name][1], found_map[name][2]) for name in table_names]

# --- Create silver streaming tables with dedup, null removal, and upsert ---
for tbl_name, tbl_catalog, tbl_schema, tbl_metadata in tables_to_ingest:
    source_table = f"{tbl_catalog}.{tbl_schema}.{tbl_name}_bronze"
    target_table = f"{tbl_catalog}.{tbl_schema}.{tbl_name}_silver"
    # Parse primary key from metadata JSON column
    if not tbl_metadata:
        raise ValueError(
            f"No metadata defined for table '{tbl_name}' in control table."
        )
    metadata = json.loads(tbl_metadata)
    primary_key = metadata.get("primary_key")
    if not primary_key:
        raise ValueError(
            f"No 'primary_key' found in metadata for table '{tbl_name}'."
        )

    # Temporary view: read from bronze, add processing time, remove nulls, deduplicate
    @dp.temporary_view(name=f"{tbl_name}_cleaned")
    def create_cleaned_view(src=source_table, pk=primary_key):
        return (
            spark.readStream.table(src)
            .withColumn("_processing_time", F.current_timestamp())
            .filter(F.col(pk).isNotNull())
            .withWatermark("_processing_time", "5 minutes")
            .dropDuplicatesWithinWatermark([pk])
        )

    # Create silver target streaming table
    dp.create_streaming_table(
        name=target_table,
        expect_all_or_drop={"valid_primary_key": f"{primary_key} IS NOT NULL"}
    )

    # Auto CDC (SCD Type 1) upsert from cleaned view into silver table
    dp.create_auto_cdc_flow(
        target=target_table,
        source=f"{tbl_name}_cleaned",
        keys=[primary_key],
        sequence_by="_processing_time",
        stored_as_scd_type=1,
        except_column_list=["_rescued_data"]
    )

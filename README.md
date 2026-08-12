# ETL CDC Prototype with Databricks Lakeflow

A prototype data engineering project demonstrating change data capture (CDC) pipelines on Databricks using the Lakeflow Declarative Pipelines framework and a medallion architecture.

> **Repository status:** This is an early-stage prototype focused on reusable pipeline patterns, synthetic source-data generation, control-table-driven orchestration, and CDC processing.

## Overview

```text
Synthetic JSON files in Databricks Volumes
                 |
                 v
        Bronze streaming tables
      (Auto Loader / append flows)
                 |
                 v
        Silver streaming tables
 (validation, deduplication, Auto CDC)
                 |
                 v
     Curated SCD Type 1 representations
```

The project demonstrates metadata-driven ingestion of multiple source tables without duplicating pipeline logic for each entity. Table names, target catalogs and schemas, grouping, ordering, and primary-key metadata are maintained in a control table.

## Key capabilities

- Databricks Lakeflow pipelines using `pyspark.pipelines` and declarative dataset definitions.
- Auto Loader ingestion from Unity Catalog Volume paths using JSON file discovery.
- Metadata-driven processing through `utilities.job_control_info`.
- CDC handling using `dp.create_auto_cdc_flow` with SCD Type 1 semantics.
- Data-quality enforcement through expectations and primary-key validation.
- Streaming deduplication using watermarks and `dropDuplicatesWithinWatermark`.
- Synthetic data generation with Faker.
- PII masking examples in generated employee data.

## Repository structure

| Path | Responsibility |
|---|---|
| `explorations/synthetic_setupdata_generation.py` | Creates synthetic customers, orders, products, employees, departments, and workplaces as JSON files in Databricks Volumes. |
| `utilities/create_job_control_info.py` | Creates and populates the metadata/control table used to resolve tables, destinations, groups, sequence, and primary-key metadata. |
| `transformations/ingest_into_bronze_generic.py` | Generic Bronze-layer ingestion using Auto Loader and dynamically generated append flows. |
| `transformations/bronze_to_silver_generic.py` | Generic Silver-layer cleansing, validation, deduplication, and Auto CDC processing. |
| `tests/test_new_2026_08_12_08_25_58.py` | Placeholder test module; implementation is not yet present. |

## Medallion architecture analysis

### Source and landing zone

The synthetic setup notebook creates representative source entities and writes JSON data into Unity Catalog Volumes. The generated domains include:

- **Sales:** `customers`, `orders`, and `products`
- **HR:** `employees`, `departments`, and `workplaces`

Customer records include `APPEND`, `UPDATE`, and `DELETE` operation values, making them suitable for CDC demonstrations. The employee generator also illustrates masking of names, email addresses, and phone numbers.

### Bronze layer: raw streaming ingestion

`transformations/ingest_into_bronze_generic.py` creates Bronze streaming tables and append flows dynamically. For each configured table, it:

1. Resolves the catalog and schema from the control table.
2. Builds a Volume path.
3. Creates a `{table_name}_bronze` streaming table.
4. Reads JSON files with Auto Loader (`cloudFiles`).
5. Appends discovered records to the Bronze table.

The Bronze layer stays close to the source shape, preserving ingestion fidelity and providing a replayable foundation for downstream transformations.

### Silver layer: quality, deduplication, and CDC

`transformations/bronze_to_silver_generic.py` reads Bronze streams and creates `{table_name}_silver` targets. The transformation includes:

- A processing timestamp named `_processing_time`.
- Filtering of records with null primary keys.
- A five-minute watermark.
- Duplicate removal within the watermark window.
- A data-quality expectation for valid primary keys.
- `create_auto_cdc_flow` configured for **SCD Type 1**, so Silver reflects the latest version of each business key.

Primary keys are read from the control table's JSON `metadata` column instead of being hard-coded, which is the core reusable design pattern in this prototype.

### Gold layer: future extension point

The current implementation establishes Bronze and Silver processing but does not yet define a separate Gold layer. Gold tables could provide customer and order metrics, inventory summaries, workforce reporting, or other business-ready data products built from the cleansed Silver tables.

## Metadata-driven orchestration

The control table contains:

- `table_name`
- `catalog`
- `schema`
- `group_code`
- `seq_num`
- `metadata`

The pipelines support two selection modes:

1. **`group_code`** — resolves all tables in a group and processes them in `seq_num` order.
2. **`table_name`** — processes a comma-separated list of selected tables.

When both parameters are present, `group_code` takes precedence. The transformation code validates missing parameters, malformed group codes, unknown tables, missing metadata, and missing primary keys before defining flows.

## Example pipeline configuration

```text
group_code = 1
```

or:

```text
table_name = employees,departments,workplaces
```

The prototype currently uses the `catadb360dev` catalog and the `utilities`, `hrdata`, and `schemaadb360dev` schemas. These values should be adapted for the target workspace.

## Getting started

### Prerequisites

- A Databricks workspace with Lakeflow Declarative Pipelines support.
- Unity Catalog enabled.
- Permissions to create schemas, tables, and Volumes.
- A cluster capable of running Spark Structured Streaming code.
- The `faker` Python package, installed by the synthetic-data notebook.

### Suggested execution order

1. Run `utilities/create_job_control_info.py` to create and populate `job_control_info`.
2. Run `explorations/synthetic_setupdata_generation.py` to create JSON files in the configured Volumes.
3. Configure a Lakeflow pipeline with `transformations/ingest_into_bronze_generic.py` and provide `group_code` or `table_name`.
4. Configure a downstream Lakeflow pipeline with `transformations/bronze_to_silver_generic.py`.
5. Validate Bronze ingestion, Silver data-quality expectations, deduplication, and SCD Type 1 CDC behavior.

## Design strengths

- **Reusable:** One ingestion implementation supports multiple entities.
- **Configurable:** Table routing and key metadata are externalized.
- **Incremental:** Auto Loader and streaming tables support continuous ingestion.
- **CDC-aware:** Auto CDC provides key-based upsert behavior.
- **Demonstrable:** Faker-generated data avoids requiring an external source system.
- **Governance-oriented:** Unity Catalog naming and PII masking are included.

## Current limitations and recommended next steps

- Add automated tests for parameter validation, metadata resolution, and transformations.
- Add explicit Gold-layer datasets and business-facing data products.
- Parameterize catalog, schema, Volume, and control-table names.
- Document the source CDC contract, including delete semantics and sequence requirements.
- Prefer native Spark expressions over Python UDFs where practical.
- Use a source-system change timestamp or monotonically increasing version for CDC ordering when available; the current flow uses `_processing_time`.
- Add schema-evolution policies and operational monitoring for malformed records.
- Add quarantine flows for invalid records beyond null primary-key checks.
- Add CI checks, linting, and executable tests for Databricks notebook source files.
- Document required Unity Catalog grants and Lakeflow pipeline settings.

## Technology stack

- Python
- PySpark
- Databricks Lakeflow Declarative Pipelines
- Apache Spark Structured Streaming
- Auto Loader (`cloudFiles`)
- Delta Lake streaming tables and Auto CDC
- Unity Catalog Volumes
- Faker

## License

This repository is distributed under the Apache License 2.0. See the repository license file for details.

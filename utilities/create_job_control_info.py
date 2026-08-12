# Databricks notebook source
# Utility script to create and populate the job_control_info control table
# Catalog: catadb360dev | Schema: utilities | Table: job_control_info

catalog = "catadb360dev"
schema = "utilities"

# Create schema if it does not exist
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{schema}`")
spark.sql(f"USE SCHEMA `{schema}`")

# Drop and recreate table with renamed column (domain -> schema)
spark.sql("DROP TABLE IF EXISTS job_control_info")

spark.sql("""
    CREATE TABLE IF NOT EXISTS job_control_info (
        table_name STRING NOT NULL,
        catalog STRING NOT NULL,
        schema STRING NOT NULL,
        group_code INT NOT NULL,
        seq_num INT,
        metadata STRING,
        CONSTRAINT pk_job_control PRIMARY KEY (table_name, schema)
    )
""")

# Insert rows for HRData group (group_code = 1)
spark.sql("""
    MERGE INTO job_control_info AS target
    USING (
        SELECT 'employees' AS table_name, 'catadb360dev' AS catalog, 'hrdata' AS schema, 1 AS group_code, 1 AS seq_num,
               '{"primary_key": "employee_id", "secondary_keys": ["department_id", "workplace_id"]}' AS metadata
        UNION ALL
        SELECT 'departments', 'catadb360dev', 'hrdata', 1, 2,
               '{"primary_key": "department_id", "secondary_keys": []}'
        UNION ALL
        SELECT 'workplaces', 'catadb360dev', 'hrdata', 1, 3,
               '{"primary_key": "workplace_id", "secondary_keys": []}'
    ) AS source
    ON target.table_name = source.table_name AND target.schema = source.schema
    WHEN NOT MATCHED THEN INSERT *
""")

# Insert rows for Sales group (group_code = 2)
spark.sql("""
    MERGE INTO job_control_info AS target
    USING (
        SELECT 'customers' AS table_name, 'catadb360dev' AS catalog, 'schemaadb360dev' AS schema, 2 AS group_code, 1 AS seq_num
        UNION ALL
        SELECT 'orders','catadb360dev', 'schemaadb360dev', 2, 2
        UNION ALL
        SELECT 'products','catadb360dev', 'schemaadb360dev', 2, 3
    ) AS source
    ON target.table_name = source.table_name AND target.schema = source.schema
    WHEN NOT MATCHED THEN INSERT *
""")

print("job_control_info table created and populated successfully.")
spark.sql("SELECT * FROM job_control_info ORDER BY group_code, seq_num").show(truncate=False)

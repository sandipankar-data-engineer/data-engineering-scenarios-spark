# Databricks File Metadata Columns

## 1. What are file metadata columns?

When Databricks reads supported files from cloud storage, it exposes a
special hidden column named `_metadata`.

This column contains information about the source file from which each
row was read. You can select individual fields from `_metadata` and
persist them as ordinary columns in a target table.

File metadata is useful in Bronze ingestion because it preserves source
context alongside business data.

**Common use cases**

- **Lineage:** identify the file that produced a row.
- **Auditing:** retain source-file details for traceability.
- **Troubleshooting:** locate the input file responsible for unexpected
  records.
- **Reconciliation:** compare ingested records with source files.
- **Operational analysis:** inspect file sizes and modification times.

## 2. Common `_metadata` fields

| Metadata field                     | Description                                                          |
|------------------------------------|----------------------------------------------------------------------|
| `_metadata.file_name`              | Name of the source file, including its extension.                    |
| `_metadata.file_path`              | Full path of the source file.                                        |
| `_metadata.file_size`              | Source file size in bytes.                                           |
| `_metadata.file_modification_time` | Last-modified timestamp reported for the source file.                |
| `_metadata.file_block_start`       | Starting byte offset of the file block being read, where applicable. |
| `_metadata.file_block_length`      | Length in bytes of the file block being read, where applicable.      |

For most ingestion pipelines, `file_name`, `file_path`, and
`file_modification_time` are the most useful fields. The fields
available can depend on the file source and read mechanism.

## 3. Reading and selecting metadata

`_metadata` is not automatically added to the target table. Explicitly
select the fields that you want to retain.

### 3.1 SQL: batch ingestion with `read_files()`

``` sql
CREATE OR REPLACE TABLE main.bronze.orders AS
SELECT
    *,
    _metadata.file_name AS source_file,
    _metadata.file_path AS source_file_path,
    _metadata.file_modification_time AS source_file_modified_at
FROM read_files(
    '/Volumes/main/raw/orders/',
    format => 'csv',
    header => true,
    inferColumnTypes => true
);
```

This creates a table containing the input columns plus three
source-metadata columns.

### 3.2 SQL: Auto Loader with a streaming table

``` sql
CREATE OR REFRESH STREAMING TABLE main.bronze.orders
AS
SELECT
    *,
    _metadata.file_name AS source_file,
    _metadata.file_modification_time AS source_file_modified_at
FROM STREAM read_files(
    '/Volumes/main/raw/orders/',
    format => 'csv',
    header => true,
    inferColumnTypes => true
);
```

This is a Lakeflow Spark Declarative Pipelines streaming-table
definition. The pipeline manages the streaming table's execution and
progress.

### 3.3 PySpark: Auto Loader

``` python
from pyspark.sql.functions import col

source_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/main/ops/schema/orders")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/main/raw/orders/")
)

bronze_df = source_df.select(
    "*",
    col("_metadata.file_name").alias("source_file"),
    col("_metadata.file_path").alias("source_file_path"),
    col("_metadata.file_modification_time").alias("source_file_modified_at")
)

(
    bronze_df.writeStream
        .option("checkpointLocation", "/Volumes/main/ops/checkpoints/orders")
        .toTable("main.bronze.orders")
)
```

Use persistent schema and checkpoint locations appropriate to the
stream. Do not share one checkpoint between independent streaming
queries.

## 4. Example of the resulting Bronze table

Suppose the source folder contains these files.

**`orders_01.csv`**

| order_id | amount |
|---------:|-------:|
|      101 |    500 |
|      102 |    700 |

**`orders_02.csv`**

| order_id | amount |
|---------:|-------:|
|      103 |    900 |

After selecting file metadata, the Bronze table could look like this.

| order_id | amount | source_file     | source_file_modified_at |
|---------:|-------:|-----------------|-------------------------|
|      101 |    500 | `orders_01.csv` | 2026-11-10 10:30        |
|      102 |    700 | `orders_01.csv` | 2026-11-10 10:30        |
|      103 |    900 | `orders_02.csv` | 2026-11-11 09:15        |

*Illustrative values. File metadata is generally repeated for every row
read from the same file.*

## 5. File modification time vs. ingestion time

These timestamps represent different events.

| Column or expression               | What it represents                                                          |
|------------------------------------|-----------------------------------------------------------------------------|
| `_metadata.file_modification_time` | When the source file was last modified in storage.                          |
| `current_timestamp()`              | The timestamp at which the query expression is evaluated during processing. |

If you need both, capture them separately.

``` sql
SELECT
    *,
    _metadata.file_name AS source_file,
    _metadata.file_modification_time AS source_file_modified_at,
    current_timestamp() AS ingested_at
FROM read_files(
    '/Volumes/main/raw/orders/',
    format => 'csv',
    header => true
);
```

Do not label `file_modification_time` as the ingestion timestamp. A file
may have been modified well before it was ingested.

## 6. Important considerations

1.  **Select metadata explicitly.** `_metadata` is a hidden input
    column; its fields become stored target columns only when you select
    them.
2.  **Use clear aliases.** Names such as `source_file`,
    `source_file_path`, and `source_file_modified_at` make the target
    schema easier to understand.
3.  **Metadata is file-level context.** Rows from the same file normally
    carry the same file name, path, size, and modification time.
4.  **Keep technical metadata separate from business data.** Treat these
    as audit or operational columns.
5.  **Retain the path when needed.** If files with the same name can
    arrive from different folders, retain `file_path` as well as
    `file_name`.
6.  **Protect source paths.** Full paths can expose storage layout or
    operational details. Apply appropriate access controls.
7.  **Modification time is not arrival time.** Copying a file can
    preserve or change its modification timestamp depending on the
    storage and copy process.
8.  **Do not use block metadata as a row key.** Block start and length
    describe file-read blocks where applicable; they are not
    general-purpose unique row identifiers.
9.  **Check source support.** `_metadata` is available for supported
    file-based reads, but availability and individual fields depend on
    the source and read mechanism. It is not a universal metadata column
    for every table or streaming source.
10. **Be deliberate with `SELECT *`.** Explicitly reference the metadata
    fields you want. If the source schema changes, review the target
    schema and downstream consumers.

## 7. Quick reference

| Requirement                    | Expression                         |
|--------------------------------|------------------------------------|
| Source file name               | `_metadata.file_name`              |
| Full source file path          | `_metadata.file_path`              |
| Source file size               | `_metadata.file_size`              |
| Source file modification time  | `_metadata.file_modification_time` |
| Ingestion/processing timestamp | `current_timestamp()`              |

## Key takeaway

When ingesting files into a Bronze Delta table, explicitly select useful
fields from `_metadata` and store them as technical columns. Retaining
the source file name and modification time improves traceability. Retain
the full path and a separate ingestion timestamp when your audit and
operational requirements call for them.

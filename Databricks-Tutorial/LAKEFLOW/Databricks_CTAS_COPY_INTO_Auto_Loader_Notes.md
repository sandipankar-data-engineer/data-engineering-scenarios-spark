# Databricks File Ingestion Methods: CTAS, COPY INTO, and Auto Loader

> **Purpose:** A consolidated learning reference for three Databricks methods discussed: `CREATE TABLE AS SELECT` (CTAS), `COPY INTO`, and Auto Loader.
>
> **Scope:** The examples use Unity Catalog names and `/Volumes/...` paths. Replace catalog, schema, volume, paths, and table names with your own. Exact feature availability can vary by cloud, Databricks Runtime, and workspace configuration.

---

# 1. CTAS — CREATE TABLE AS SELECT

## 1.1 Definition

`CTAS` stands for **Create Table As Select**. It creates a table and populates it with the result of a SQL query.

It is a query-based table creation operation. The source can be an existing table, view, or a file-reading expression such as `read_files()`.

**Key idea:** CTAS materializes the result of a query at the time the statement runs. It does not automatically monitor the source or ingest later-arriving files.

## 1.2 Core concepts, properties and aspects

1. **Create and populate in one operation:** CTAS combines table creation and data insertion.
2. **Query-based:** The `SELECT` statement determines which rows and columns are written.
3. **Snapshot behaviour:** The table contains the result produced when CTAS executes.
4. **No automatic incremental tracking:** CTAS does not maintain a list of source files already ingested.
5. **No automatic refresh:** Changes to the source do not automatically appear in the CTAS-created table.
6. **Target format:** In Databricks, managed tables default to Delta unless another supported provider is specified.
7. **Schema derivation:** The target schema is derived from the output of the `SELECT` query.
8. **Transformations:** The query can filter, join, aggregate, rename, cast, and derive columns.
9. **Replacement:** `CREATE OR REPLACE TABLE ... AS SELECT` replaces the existing table definition/data with the query result.
10. **Existing-table behaviour:** Plain `CREATE TABLE ... AS SELECT` fails if the target table already exists. `IF NOT EXISTS` does not refresh or repopulate an existing table.
11. **External location:** A `LOCATION` clause can be used to create a table at a specified storage location, subject to the applicable table and storage rules.
12. **Not a streaming ingestion engine:** CTAS is a batch SQL operation, not a Structured Streaming source or sink.
13. **Use with files:** CTAS can read files through functions such as `read_files()`, but the resulting table is still a snapshot unless a separate refresh/ingestion mechanism is used.
14. **Use with existing tables:** CTAS is not limited to files. It can create a table from another Delta table or a complex SQL query.

## 1.3 Internal working

1. Databricks parses and analyzes the `CREATE TABLE ... AS SELECT` statement.
2. The query planner builds an execution plan for the `SELECT` query.
3. Spark reads the source data required by the query.
4. Any requested filtering, projection, joins, aggregations, and expressions are executed.
5. Databricks creates the target table using the output schema of the query.
6. The query result is written to the target table, commonly in Delta format.
7. For a Delta target, the write is committed as a new table state through the Delta transaction log.
8. After the statement completes, the target represents the result of that execution. Later source changes are not automatically incorporated.

**Important:** CTAS does not provide Auto Loader-style file discovery or COPY INTO-style processed-file tracking.

## 1.4 Important options and configurations

Common CTAS clauses and related considerations:

| Clause / property | Purpose |
|---|---|
| `OR REPLACE` | Replaces an existing table with the newly generated table result. |
| `IF NOT EXISTS` | Creates the table only if it does not already exist; it does not refresh an existing table. |
| `USING` | Specifies the table provider, such as `DELTA`. Delta is the Databricks default for standard managed tables. |
| `LOCATION` | Specifies a storage location for the table. |
| `COMMENT` | Adds a description to the table. |
| `TBLPROPERTIES` | Sets table properties. |
| `PARTITIONED BY` | Defines partition columns where supported and appropriate. |
| `CLUSTER BY` | Defines clustering columns for supported table configurations. |
| `AS SELECT` | Supplies the query whose output populates the table. |

Additional points:

- The table schema is normally inferred from the query output. If you need precise column types, use explicit casts in the `SELECT`.
- `OR REPLACE` is not the same as incremental append. It replaces the table contents with the result of the query.
- `IF NOT EXISTS` and `OR REPLACE` are alternative behaviours and should not be combined.
- Do not use partitioning automatically for every table. Choose partitioning or liquid clustering based on table size, query patterns, and current Databricks guidance.
- CTAS is different from a materialized view. A materialized view is a managed query result that can be refreshed; a CTAS table is not automatically refreshed.

## 1.5 Syntax

### Basic CTAS

```sql
CREATE TABLE catalog.schema.target_table AS
SELECT *
FROM catalog.schema.source_table;
```

### Create or replace

```sql
CREATE OR REPLACE TABLE catalog.schema.target_table AS
SELECT
    customer_id,
    UPPER(customer_name) AS customer_name,
    CAST(order_amount AS DECIMAL(12, 2)) AS order_amount
FROM catalog.schema.source_table
WHERE order_amount > 0;
```

### CTAS from files using `read_files()`

```sql
CREATE TABLE catalog.schema.nykcovid AS
SELECT
    *,
    _metadata.file_path AS source_file_path
FROM read_files(
    '/Volumes/catalog/schema/volume/COVID-19_NYT',
    format => 'csv',
    header => true
);
```

> Use the actual source format. `read_files()` is for supported file formats; it is not the way to read an existing Delta table. To read Delta by path, use `delta.\`/path\`` or read the registered table.

### CTAS from a Delta path

```sql
CREATE TABLE catalog.schema.target_table AS
SELECT *
FROM delta.`/Volumes/catalog/schema/volume/delta_source`;
```

### CTAS with table properties

```sql
CREATE TABLE catalog.schema.target_table
USING DELTA
COMMENT 'Snapshot of source customer data'
TBLPROPERTIES (
    'quality' = 'bronze'
)
AS
SELECT *
FROM catalog.schema.source_table;
```

### CTAS with a custom location

```sql
CREATE TABLE catalog.schema.target_table
USING DELTA
LOCATION '/path/to/managed-data-location'
AS
SELECT *
FROM catalog.schema.source_table;
```

Check the applicable managed/external table rules before specifying a custom location.

---

# 2. COPY INTO

## 2.1 Definition

`COPY INTO` is a Databricks SQL command that loads files from a cloud storage location into an **existing Delta table**.

It is designed for retryable, incremental batch file ingestion. Databricks tracks files that have been loaded and normally skips those files in subsequent executions.

**Key idea:** COPY INTO loads eligible new files when the command runs. It does not continuously watch the directory by itself.

## 2.2 Core concepts, properties and aspects

1. **File-based ingestion:** Loads data from supported files in cloud object storage or supported storage paths.
2. **Existing target:** The target Delta table must exist before the command runs. In supported configurations, an empty schemaless target can be created and its schema inferred during loading.
3. **Bulk loading:** It is suitable for loading a collection of files in a batch.
4. **Incremental behaviour:** Re-running the command can load files not previously loaded.
5. **Idempotency:** By default, files that have already been loaded are skipped.
6. **Retryable:** The command can be retried after failures; successfully loaded files are normally not loaded again.
7. **Modified files:** A file that has already been loaded is normally skipped even if its contents are later modified.
8. **Force reload:** `COPY_OPTIONS ('force' = 'true')` disables normal file idempotency and allows previously loaded files to be loaded again.
9. **Source formats:** Common supported formats include CSV, JSON, Avro, ORC, Parquet, TEXT, and BINARYFILE. Confirm format support and options for your Databricks environment.
10. **File selection:** You can load all eligible files in a source location, selected file names, or files matching a pattern.
11. **Parsing options:** `FORMAT_OPTIONS` passes file-reader options to the underlying Spark data source reader.
12. **Operation options:** `COPY_OPTIONS` controls COPY INTO behaviour, including `force` and target schema evolution through `mergeSchema`.
13. **Schema inference and evolution:** Input schema inference, merging schemas across source files, and evolving the target Delta schema are separate concerns.
14. **Source transformation:** A `SELECT` query can be used in the source clause to transform data before it is loaded.
15. **Scheduling:** COPY INTO can be scheduled using a Databricks Job or another orchestrator.
16. **Not continuous streaming:** A new file is not loaded merely because it arrives. The COPY INTO command must run again.
17. **Source retention:** COPY INTO does not automatically delete source files after loading.
18. **Not for Delta-to-Delta copying:** COPY INTO is intended for file ingestion, not as a general mechanism to copy an existing Delta table into another Delta table.

## 2.3 Internal working

1. The command identifies the target Delta table and source location.
2. Databricks discovers eligible files in the source location, subject to `FILES`, `PATTERN`, and other applicable filters.
3. It checks its file-ingestion tracking information to identify files already loaded by COPY INTO.
4. Previously loaded files are skipped by default, including files that have been modified since they were loaded.
5. New eligible files are read using the specified `FILEFORMAT` and `FORMAT_OPTIONS`.
6. If a source `SELECT` is provided, its expressions and transformations are applied.
7. The resulting records are written to the target Delta table.
8. The successful operation updates the ingestion tracking information.
9. On a later execution, the command uses that tracking information to avoid normally reloading the same files.

**Important:** `force = true` disables the normal file-skipping/idempotency behaviour. Reloading files can create duplicate rows unless the target loading logic accounts for them.

## 2.4 Important options and configurations

### Main clauses

| Clause | Purpose |
|---|---|
| `FROM` | Specifies the source location or source query. |
| `FILEFORMAT` | Specifies the source format, such as CSV, JSON, or PARQUET. |
| `FORMAT_OPTIONS` | Passes reader options for parsing the source format. |
| `COPY_OPTIONS` | Controls COPY INTO behaviour. |
| `FILES` | Specifies particular file names to load. |
| `PATTERN` | Selects files using a glob pattern. |
| `VALIDATE` | Validates source data without performing the normal load. |
| `BY POSITION` | Maps source columns to target columns by ordinal position. |
| `(col1, col2, ...)` | Specifies target column mapping by name. |
| `FROM (SELECT ...)` | Applies a query to the source data before loading. |

### Important COPY_OPTIONS

| Option | Default | Behaviour |
|---|---:|---|
| `force` | `false` | When `true`, disables idempotent file skipping and reloads files even if previously loaded. |
| `mergeSchema` | `false` | When `true`, allows the target Delta schema to evolve based on incoming data. |

### Important FORMAT_OPTIONS examples

| Format | Example options |
|---|---|
| CSV | `header`, `delimiter`, `inferSchema`, `encoding`, `multiLine` |
| JSON | `multiLine`, `allowComments`, `dateFormat`, `timestampFormat` |
| Parquet | Applicable Parquet reader options |
| Avro | Applicable Avro reader options |

**Do not confuse these two `mergeSchema` uses:**

- `FORMAT_OPTIONS ('mergeSchema' = 'true')` can merge/infer a schema across multiple source files where supported by the reader.
- `COPY_OPTIONS ('mergeSchema' = 'true')` allows the target Delta table schema to evolve.

### Other considerations

- `FILES` and `PATTERN` are alternative file-selection mechanisms; they cannot be used together.
- `FILES` has a documented limit of 1,000 file names.
- `VALIDATE ALL` or `VALIDATE n ROWS` can be used to validate data before loading.
- Reader options depend on the source file format.
- Source access and target-table privileges must be configured correctly.

## 2.5 Syntax

### Basic syntax

```sql
COPY INTO catalog.schema.target_table
FROM '/Volumes/catalog/schema/volume/source_files/'
FILEFORMAT = JSON;
```

### CSV with format options

```sql
COPY INTO catalog.schema.customers
FROM '/Volumes/catalog/schema/volume/customers/'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'delimiter' = ',',
    'inferSchema' = 'true'
);
```

### Select specific files

```sql
COPY INTO catalog.schema.customers
FROM '/Volumes/catalog/schema/volume/customers/'
FILEFORMAT = CSV
FILES = ('customers_01.csv', 'customers_02.csv')
FORMAT_OPTIONS ('header' = 'true');
```

### Select files using a pattern

```sql
COPY INTO catalog.schema.customers
FROM '/Volumes/catalog/schema/volume/customers/'
FILEFORMAT = CSV
PATTERN = 'customers_*.csv'
FORMAT_OPTIONS ('header' = 'true');
```

### Enable target schema evolution

```sql
COPY INTO catalog.schema.customers
FROM '/Volumes/catalog/schema/volume/customers/'
FILEFORMAT = JSON
COPY_OPTIONS ('mergeSchema' = 'true');
```

### Force reload

```sql
COPY INTO catalog.schema.customers
FROM '/Volumes/catalog/schema/volume/customers/'
FILEFORMAT = JSON
COPY_OPTIONS ('force' = 'true');
```

### Transform source data before loading

```sql
COPY INTO catalog.schema.customers
FROM (
    SELECT
        CAST(customer_id AS BIGINT) AS customer_id,
        UPPER(customer_name) AS customer_name
    FROM '/Volumes/catalog/schema/volume/customers/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true');
```

### Validate source data

```sql
COPY INTO catalog.schema.customers
FROM '/Volumes/catalog/schema/volume/customers/'
FILEFORMAT = CSV
VALIDATE ALL
FORMAT_OPTIONS ('header' = 'true');
```

---

# 3. AUTO LOADER

## 3.1 Definition

Auto Loader is a Databricks capability for incrementally and efficiently ingesting new files from cloud object storage.

It uses the Structured Streaming source format `cloudFiles` to discover and process files as they arrive. It supports scalable file discovery, schema inference and evolution, and checkpoint-based progress tracking.

**Key idea:** Auto Loader automatically discovers and processes new files through a streaming workload. It can run continuously or in triggered/available-now mode.

## 3.2 Core concepts, properties and aspects

1. **Incremental ingestion:** Processes new files arriving in cloud storage without requiring the user to manually list each new file.
2. **Structured Streaming:** Auto Loader is built on Apache Spark Structured Streaming.
3. **Source format:** In PySpark, `.format("cloudFiles")` enables Auto Loader.
4. **Underlying file format:** `cloudFiles.format` specifies the actual file format, such as JSON, CSV, or Parquet.
5. **Cloud storage:** Supports cloud object storage such as ADLS, Amazon S3, and Google Cloud Storage.
6. **Large scale:** Designed to scale to very large file volumes, including billions of files.
7. **Existing files:** By default, files already present in the source directory are eligible for initial ingestion.
8. **New files:** Auto Loader discovers and processes files that arrive after the stream starts.
9. **File discovery:** Supports directory-listing and file-notification/event-based discovery approaches.
10. **Managed file events:** Can use Databricks-managed file events when the relevant Unity Catalog external location and environment are configured.
11. **Schema inference:** Can infer the schema from a sample of source files.
12. **Schema evolution:** Can detect new columns and evolve the tracked schema according to the configured evolution mode.
13. **Rescued data:** Unexpected columns or incompatible values can be captured in a rescued data column such as `_rescued_data`.
14. **Checkpointing:** Uses a checkpoint location to track stream progress and file-processing state.
15. **Schema location:** In classic PySpark usage, `cloudFiles.schemaLocation` stores schema information. Lakeflow pipelines manage relevant schema and checkpoint state for pipeline sources.
16. **Processing modes:** Can run continuously or use triggers such as `availableNow=True` to process available data and stop.
17. **Destination:** Commonly writes to Delta tables using `writeStream.toTable()` or a Lakeflow streaming table.
18. **Multiple sources:** Independent ingestion streams should use independent checkpoint locations.
19. **File order:** Do not assume files are processed in arrival-time or business-event order.
20. **Not a message-broker connector:** Auto Loader is for files in cloud object storage. Use the appropriate Structured Streaming connector for Kafka, Event Hubs, and other message-based systems.

### File formats and schema support

Auto Loader supports common file formats including CSV, JSON, XML, Avro, Parquet, ORC, TEXT, and BINARYFILE. However, schema inference/evolution support differs by format and Databricks Runtime version:

| Format | Schema inference/evolution notes |
|---|---|
| JSON | Supported |
| CSV | Supported |
| XML | Supported in Databricks Runtime 14.3 LTS and above |
| Avro | Supported in Databricks Runtime 10.4 LTS and above |
| Parquet | Supported in Databricks Runtime 11.3 LTS and above |
| ORC | Schema inference/evolution not supported by Auto Loader |
| TEXT | Fixed schema |
| BINARYFILE | Fixed schema |

Always check the current documentation for the runtime and format in use.

## 3.3 Internal working

1. **Source configuration:** The user specifies the cloud storage directory and the actual file format.
2. **File discovery:** Auto Loader discovers files using directory listing or file-notification/event-based mechanisms.
3. **Initial discovery:** By default, existing files are eligible for ingestion when the stream starts.
4. **Schema inference:** If configured, Auto Loader samples source data and infers a schema.
5. **Schema persistence:** Schema information is stored in the schema location in classic PySpark implementations.
6. **Micro-batch processing:** Structured Streaming processes discovered files in micro-batches.
7. **Data parsing:** The source files are parsed using the configured file format and reader options.
8. **Schema handling:** New columns and unexpected data are handled according to the schema evolution mode, schema hints, and rescued-data configuration.
9. **Writing:** The processed records are written to the target Delta table or another supported streaming sink.
10. **Checkpoint update:** The streaming checkpoint records progress and state needed for recovery.
11. **New arrivals:** Later files are discovered and processed in subsequent micro-batches or triggered runs.
12. **Recovery:** Restarting with the same checkpoint allows the stream to resume from its recorded progress.

**Important:** The checkpoint and schema location serve different purposes. The checkpoint tracks streaming progress/state; the schema location tracks inferred/evolved schema. Do not delete or casually replace either location.

## 3.4 Important options and configurations

### Source and schema options

| Option | Purpose |
|---|---|
| `cloudFiles.format` | Specifies the actual source format, such as `json`, `csv`, or `parquet`. |
| `cloudFiles.schemaLocation` | Stores schema inference and evolution information in classic Auto Loader workloads. |
| `cloudFiles.inferColumnTypes` | Enables data-type inference for formats such as JSON, CSV, and XML. |
| `cloudFiles.schemaHints` | Provides types for selected columns while allowing the remaining schema to be inferred. |
| `cloudFiles.schemaEvolutionMode` | Controls the behaviour when new columns are detected. |
| `cloudFiles.rescuedDataColumn` | Specifies a column for unexpected or mismatched data. |
| `cloudFiles.partitionColumns` | Specifies Hive-style partition columns in directory paths. |

### Schema evolution modes

| Mode | Behaviour |
|---|---|
| `addNewColumns` | Adds new columns to the schema; the stream fails with an unknown-field exception and must restart. |
| `addNewColumnsWithTypeWidening` | Similar to `addNewColumns`, with supported type widening in eligible runtimes. |
| `rescue` | Does not evolve the schema; new columns are captured in the rescued data column. |
| `failOnNewColumns` | Fails when new columns are detected and requires schema/data correction. |
| `none` | Does not evolve the schema; new columns are ignored unless rescued-data handling is configured. |

Defaults depend on whether a schema is supplied. With no explicit schema, `addNewColumns` is generally the default; with an explicit schema, `none` is generally the default.

### Discovery and processing options

| Option | Purpose |
|---|---|
| `cloudFiles.includeExistingFiles` | Controls whether existing files are included when ingestion starts. |
| `cloudFiles.allowOverwrites` | Allows processing of files that are overwritten; requires careful duplicate handling. |
| `cloudFiles.maxFilesPerTrigger` | Limits files processed per micro-batch. |
| `cloudFiles.maxBytesPerTrigger` | Limits the approximate data volume processed per micro-batch. |
| `cloudFiles.useManagedFileEvents` | Uses managed file events where supported and configured. |
| `cloudFiles.useNotifications` | Enables file-notification-based discovery in supported configurations. |
| `cloudFiles.backfillInterval` | Configures periodic backfill discovery to help catch missed notifications. |
| `cloudFiles.useStrictGlobber` | Enables strict glob matching behaviour. |
| `pathGlobFilter` | Filters source files using a glob pattern. |
| `modifiedAfter` / `modifiedBefore` | Filters files using modification timestamps. |
| `ignoreCorruptFiles` / `ignoreMissingFiles` | Controls handling of corrupt or missing files. |

### Streaming write options

| Option / method | Purpose |
|---|---|
| `checkpointLocation` | Stores Structured Streaming progress and state. |
| `trigger(processingTime=...)` | Runs micro-batches at the specified processing interval. |
| `trigger(availableNow=True)` | Processes all data available to the query, potentially across multiple micro-batches, and then stops. |
| `trigger(once=True)` | Runs a single micro-batch and then stops; distinct from `availableNow`. |
| `toTable()` | Writes the streaming DataFrame to a table. |
| `start()` | Starts the streaming query using the configured sink. |

### Operational considerations

- Keep checkpoint locations stable across restarts of the same stream.
- Use a unique checkpoint for each independent ingestion workload.
- Use file events/notifications for large-scale workloads where supported.
- Use `availableNow` with a scheduled job for triggered incremental batch ingestion.
- Use continuous processing when low-latency ingestion is required and justified.
- Configure job retries/restarts to handle expected stream restarts after schema evolution.
- Treat `allowOverwrites` carefully because reprocessing changed files can introduce duplicate records.
- Source cleanup options, where used, must be designed around all consumers of the source directory.

## 3.5 Syntax

### PySpark Auto Loader — basic streaming ingestion

```python
(
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.schemaLocation",
            "/Volumes/catalog/schema/volume/_schemas/orders"
        )
        .load("/Volumes/catalog/schema/volume/source/orders")

    .writeStream
        .option(
            "checkpointLocation",
            "/Volumes/catalog/schema/volume/_checkpoints/orders"
        )
        .trigger(processingTime="5 seconds")
        .toTable("catalog.schema.orders")
)
```

### Triggered incremental ingestion using `availableNow`

```python
(
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/path/schema")
        .load("/path/source")

    .writeStream
        .option("checkpointLocation", "/path/checkpoint")
        .trigger(availableNow=True)
        .toTable("catalog.schema.orders")
)
```

### Explicit schema

```python
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_name", StringType()),
    StructField("amount", DoubleType())
])

(
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(schema)
        .option("cloudFiles.schemaLocation", "/path/schema")
        .load("/path/source")

    .writeStream
        .option("checkpointLocation", "/path/checkpoint")
        .toTable("catalog.schema.orders")
)
```

### Schema hints and evolution

```python
(
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/path/schema")
        .option(
            "cloudFiles.schemaHints",
            "order_id BIGINT, amount DECIMAL(12,2)"
        )
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load("/path/source")

    .writeStream
        .option("checkpointLocation", "/path/checkpoint")
        .toTable("catalog.schema.orders")
)
```

### SQL with Lakeflow Spark Declarative Pipelines

```sql
CREATE OR REFRESH STREAMING TABLE catalog.schema.orders
AS
SELECT *
FROM STREAM read_files(
    '/Volumes/catalog/schema/volume/source/orders',
    format => 'json'
);
```

### SQL with a refresh schedule

```sql
CREATE OR REFRESH STREAMING TABLE catalog.schema.orders
SCHEDULE EVERY 1 HOUR
AS
SELECT *
FROM STREAM read_files(
    '/Volumes/catalog/schema/volume/source/orders',
    format => 'json'
);
```

### SQL with schema hints

```sql
CREATE OR REFRESH STREAMING TABLE catalog.schema.orders
AS
SELECT *
FROM STREAM read_files(
    '/Volumes/catalog/schema/volume/source/orders',
    format => 'json',
    schemaHints => 'order_id BIGINT, amount DECIMAL(12,2)'
);
```

> In Lakeflow pipelines, Databricks manages pipeline checkpointing and schema state. Do not copy classic PySpark checkpoint/schema-location configuration into a pipeline definition unless the specific interface requires it.

---

# 4. Difference — CTAS vs COPY INTO vs Auto Loader

## 4.1 Side-by-side comparison

| Feature | CTAS | COPY INTO | Auto Loader |
|---|---|---|---|
| Primary purpose | Create a table from a query | Load files into a Delta table | Incrementally ingest new cloud files |
| Input | Query result: tables, views, files, etc. | Files in a source location | Files in cloud object storage |
| Processing model | Batch query | Batch command | Structured Streaming; continuous or triggered |
| Creates target table | Yes | Normally no; target must exist | Yes with `toTable()` or a streaming-table declaration |
| Existing files | Reads files selected by query | Loads eligible files | Included by default on initial ingestion |
| Discovers future files automatically | No | No; command must run again | Yes |
| Tracks processed files | No ingestion-level tracking | Yes, for idempotent file loading | Yes, through streaming checkpoint/state |
| Checkpoint required | No | No | Yes for classic Structured Streaming |
| Schema inference | Derived from query output/source reader | Supported through reader options | Supported |
| Schema evolution | Depends on the table operation used | Supported through `COPY_OPTIONS` | Supported through schema evolution configuration |
| Source transformations | Full SQL query | Source `SELECT` is supported | Transform with downstream SQL/PySpark |
| Continuous ingestion | No | No | Yes |
| Scheduled execution | Only if repeated snapshots are required | Commonly scheduled | Optional; triggered or continuous operation |
| Typical destination | Delta table by default | Delta table | Commonly Delta table/streaming table |
| Best fit | Snapshot/materialized query result | Periodic or one-time bulk file loads | Production incremental file ingestion |

## 4.2 Visual summary

```text
CTAS
Source table / view / files
          |
          v
     SELECT query
          |
          v
   Create and populate table
          |
          v
   Snapshot at execution time


COPY INTO
Cloud storage files
          |
          v
   Discover eligible files
          |
          v
  Skip files already loaded
          |
          v
    Load new files
          |
          v
     Delta table
  (run command again later)


AUTO LOADER
Cloud storage files
          |
          v
  File discovery mechanism
          |
          v
 Schema inference/evolution
          |
          v
 Structured Streaming
          |
          v
 Checkpoint/progress tracking
          |
          v
     Delta table
 (continuous or triggered)
```

## 4.3 Practical example

Assume the source directory receives one new file each day:

```text
customers/
    customers_day1.json
    customers_day2.json
    customers_day3.json
```

### CTAS

**Day 1:** Run CTAS. The table contains Day 1 data.

**Day 2:** A new file arrives. CTAS does nothing automatically. You must execute another query. Replacing the table with a query over the folder creates a new snapshot; it is not incremental file tracking.

### COPY INTO

**Day 1:** Run COPY INTO. Day 1 file is loaded.

**Day 2:** Run COPY INTO again. Day 1 is skipped and Day 2 is loaded.

**Day 3:** Run it again. Day 1 and Day 2 are skipped; Day 3 is loaded.

### Auto Loader

**Day 1:** Start the Auto Loader stream. It discovers and processes eligible files.

**Day 2:** A new file arrives. Auto Loader discovers and processes it in a subsequent micro-batch or triggered run.

**Day 3:** The next file is processed without manually listing or selecting it.

> Auto Loader can also use `availableNow=True`, process the files currently available, and stop. A scheduled job can start it again later.

## 4.4 How they handle previously processed data

| Method | Mechanism |
|---|---|
| CTAS | No built-in processed-file tracking; the query reads the data selected at execution time. |
| COPY INTO | Maintains file-ingestion tracking and skips previously loaded files by default. |
| Auto Loader | Uses Structured Streaming checkpoint/state to track file discovery and processing progress. |

**Important:** These mechanisms are not interchangeable. COPY INTO file tracking and Auto Loader checkpoints have different operational lifecycles and recovery behaviour.

---

# 5. Unique points of each method

## 5.1 CTAS — what makes it unique?

1. Creates and populates a table in one SQL statement.
2. Works with almost any query result, not just cloud files.
3. Supports complex SQL transformations during table creation.
4. Derives the target schema from the query output.
5. Useful for one-time snapshots, staging tables, and materializing query results.
6. Does not require streaming infrastructure or checkpoints.
7. Does not automatically refresh or incrementally track source changes.
8. `CREATE OR REPLACE TABLE AS SELECT` replaces the table result; it is not an incremental append operation.

## 5.2 COPY INTO — what makes it unique?

1. Purpose-built SQL command for loading files into Delta tables.
2. Idempotent by default at the file-ingestion level.
3. Can be safely scheduled and retried for recurring batch ingestion.
4. Supports `FILES` and `PATTERN` for selective ingestion.
5. Supports `VALIDATE` for source-data validation.
6. Supports `force` to reload files that have already been processed.
7. Supports target schema evolution using `COPY_OPTIONS ('mergeSchema' = 'true')`.
8. Does not require a Structured Streaming checkpoint.
9. Does not automatically monitor the directory; the command must be run again to discover later files.

## 5.3 Auto Loader — what makes it unique?

1. Uses the `cloudFiles` Structured Streaming source.
2. Supports scalable file discovery, including file-event/notification approaches.
3. Uses checkpointing to track progress and recover streaming workloads.
4. Supports schema inference, schema hints, schema evolution, and rescued data.
5. Supports continuous and triggered/available-now ingestion.
6. Integrates with PySpark and Lakeflow Spark Declarative Pipelines.
7. Designed for large-scale, continuously growing cloud-file datasets.
8. Supports incremental processing without repeatedly running a manual file-load command.
9. Requires careful checkpoint lifecycle management.
10. Can be used to build a production Bronze ingestion pipeline.

---

# 6. When to use what?

| Requirement / scenario | Recommended method | Reason |
|---|---|---|
| Create a table from an existing SQL query | CTAS | Simple query-based table creation |
| Create a one-time snapshot from source files | CTAS | Reads and materializes the query result |
| Create a transformed table using joins and aggregations | CTAS | Full SQL query support |
| Load a folder of files once | COPY INTO | Purpose-built file ingestion |
| Load newly arrived files every hour/day using a scheduled SQL task | COPY INTO | Idempotent batch loading |
| Reload previously processed files intentionally | COPY INTO with `force = true` | Explicit force-reload behaviour |
| Continuously ingest cloud files | Auto Loader | Automatic incremental discovery |
| Ingest very large volumes of files | Auto Loader | Designed for scalable file discovery and processing |
| Handle additive schema drift in incoming files | Auto Loader | Schema evolution and rescued-data capabilities |
| Run incremental file ingestion as scheduled micro-batches | Auto Loader with `availableNow` | Processes available files and stops |
| Build a managed Bronze streaming table | Auto Loader with Lakeflow pipelines | Pipeline-managed incremental ingestion |

## 6.1 Recommended Bronze-layer usage

A common medallion architecture can use the methods as follows:

```text
                 SOURCE SYSTEMS
                       |
                       v
             CLOUD OBJECT STORAGE
                       |
            +----------+----------+
            |                     |
            v                     v
       AUTO LOADER             COPY INTO
     (continuous or          (periodic batch
      triggered)                ingestion)
            |                     |
            +----------+----------+
                       |
                       v
                  BRONZE DELTA
                       |
                       v
                  SILVER DELTA
           (cleaning, validation,
             deduplication, joins)
                       |
                       v
                   GOLD DELTA
            (business aggregates,
              reporting, analytics)
```

CTAS can be used whenever a snapshot or query-derived table is required at any stage. It is not necessarily the primary ingestion mechanism for a continuously growing Bronze source.

## 6.2 Final decision rule

- **Use CTAS** when the requirement is to create a table from the result of a query.
- **Use COPY INTO** when the requirement is to load files in batches and you are comfortable scheduling or rerunning a SQL command.
- **Use Auto Loader** when the requirement is to incrementally discover and ingest files as they arrive, especially at production scale.

---

# 7. Official Databricks documentation

1. [CREATE TABLE — Databricks SQL reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using)
2. [COPY INTO — Databricks SQL reference](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into)
3. [Get started using COPY INTO](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into)
4. [Common COPY INTO patterns](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into/examples)
5. [What is Auto Loader?](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
6. [Auto Loader schema inference and evolution](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/schema)
7. [Auto Loader options](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options)
8. [Auto Loader best practices](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/best-practices)

---

## Quick revision

| Remember this | Meaning |
|---|---|
| CTAS = Query result → Table | Snapshot/table creation |
| COPY INTO = Files → Delta table | Idempotent batch file ingestion |
| Auto Loader = New files → Streaming ingestion | Automatic incremental file discovery and processing |

**The most important distinction:** CTAS is a table-creation/query operation; COPY INTO and Auto Loader are file-ingestion mechanisms. COPY INTO is command-driven batch ingestion, whereas Auto Loader is a Structured Streaming-based incremental ingestion system.

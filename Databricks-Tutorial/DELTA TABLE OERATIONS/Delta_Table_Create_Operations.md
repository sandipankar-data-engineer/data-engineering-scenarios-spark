# Delta Table — CREATE TABLE Operation

## 1. Overview

Delta tables can be created through several APIs:

1. **Spark SQL**
2. **PySpark DeltaTable Builder API**
3. **PySpark DataFrameWriter (`df.write`)**
4. **PySpark DataFrameWriterV2 (`df.writeTo`)**
5. **PySpark `spark.sql()`** — Spark SQL executed from PySpark

These APIs overlap, but they are not identical. Some features are available only through table-definition APIs, while DataFrame write APIs derive the schema from the DataFrame.

---

# 2. Spark SQL — CREATE TABLE Syntax

## 2.1 Basic Delta Table

```sql
CREATE TABLE table_name (
    column_1 data_type,
    column_2 data_type,
    column_3 data_type
)
USING DELTA;
```

## 2.2 IF NOT EXISTS

```sql
CREATE TABLE IF NOT EXISTS table_name (
    column_1 data_type,
    column_2 data_type
)
USING DELTA;
```

## 2.3 OR REPLACE

```sql
CREATE OR REPLACE TABLE table_name (
    column_1 data_type,
    column_2 data_type
)
USING DELTA;
```

## 2.4 Table Comment

```sql
CREATE TABLE table_name (
    column_1 data_type,
    column_2 data_type
)
USING DELTA
COMMENT 'Table description';
```

## 2.5 Column Comments

```sql
CREATE TABLE table_name (
    column_1 INT COMMENT 'Employee ID',
    column_2 STRING COMMENT 'Employee name'
)
USING DELTA;
```

## 2.6 NOT NULL Columns

```sql
CREATE TABLE table_name (
    id INT NOT NULL,
    name STRING NOT NULL,
    salary DECIMAL(10,2)
)
USING DELTA;
```

## 2.7 Partitioned Table

```sql
CREATE TABLE table_name (
    id INT,
    name STRING,
    year INT,
    month INT
)
USING DELTA
PARTITIONED BY (year, month);
```

## 2.8 External Delta Table — LOCATION

```sql
CREATE TABLE table_name (
    id INT,
    name STRING
)
USING DELTA
LOCATION '/path/to/delta/table';
```

## 2.9 Partitioned + External

```sql
CREATE TABLE table_name (
    id INT,
    name STRING,
    year INT
)
USING DELTA
PARTITIONED BY (year)
LOCATION '/path/to/delta/table';
```

## 2.10 Table Properties

```sql
CREATE TABLE table_name (
    id INT,
    name STRING
)
USING DELTA
TBLPROPERTIES (
    'property1' = 'value1',
    'property2' = 'value2'
);
```

## 2.11 Partition + Location + Properties

```sql
CREATE TABLE table_name (
    id INT,
    name STRING,
    year INT
)
USING DELTA
PARTITIONED BY (year)
LOCATION '/path/to/delta/table'
TBLPROPERTIES (
    'property1' = 'value1'
);
```

---

# 3. CTAS — CREATE TABLE AS SELECT

CTAS derives the table schema from the SELECT query instead of explicitly defining the columns.

## 3.1 Basic CTAS

```sql
CREATE TABLE employees
USING DELTA
AS
SELECT
    id,
    name,
    department,
    salary
FROM source_employees;
```

## 3.2 CTAS with Transformation

```sql
CREATE TABLE employees
USING DELTA
AS
SELECT
    id,
    name,
    department,
    salary * 1.10 AS salary
FROM source_employees;
```

## 3.3 CTAS with Filter

```sql
CREATE TABLE employees
USING DELTA
AS
SELECT *
FROM source_employees
WHERE department = 'IT';
```

## 3.4 CTAS with Table Properties

```sql
CREATE TABLE employees
USING DELTA
TBLPROPERTIES (
    'property1' = 'value1'
)
AS
SELECT *
FROM source_employees;
```

## 3.5 CTAS with Partitioning

```sql
CREATE TABLE employees
USING DELTA
PARTITIONED BY (department)
AS
SELECT
    id,
    name,
    department,
    salary
FROM source_employees;
```

---

# 4. Generated Columns

```sql
CREATE TABLE employees (
    id INT,
    first_name STRING,
    last_name STRING,
    full_name STRING
        GENERATED ALWAYS AS (
            concat(first_name, ' ', last_name)
        )
)
USING DELTA;
```

---

# 5. Identity Columns

For supported Databricks/Delta configurations:

```sql
CREATE TABLE employees (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    name STRING,
    department STRING
)
USING DELTA;
```

---

# 6. Liquid Clustering

For Databricks Delta tables supporting liquid clustering:

```sql
CREATE TABLE employees (
    id INT,
    name STRING,
    department STRING,
    salary DECIMAL(10,2)
)
USING DELTA
CLUSTER BY (department, salary);
```

---

# 7. Partitioning + Clustering

Where supported:

```sql
CREATE TABLE employees (
    id INT,
    name STRING,
    department STRING,
    year INT
)
USING DELTA
PARTITIONED BY (year)
CLUSTER BY (department);
```

---

# 8. Extended Spark SQL CREATE TABLE Structure

```sql
CREATE [OR REPLACE] TABLE [IF NOT EXISTS] table_name
(
    column_definition
    [, column_definition ...]
)

USING DELTA

[PARTITIONED BY (partition_column [, ...])]

[CLUSTER BY (cluster_column [, ...])]

[LOCATION 'table_path']

[COMMENT 'table_comment']

[TBLPROPERTIES (
    'property_name' = 'property_value'
    [, ...]
)]

[AS SELECT query];
```

---

# 9. PySpark — DeltaTable Builder API

The DeltaTable Builder API is useful when explicitly defining an empty Delta table programmatically.

```python
from delta.tables import DeltaTable
```

## 9.1 Basic

```python
(
    DeltaTable.create(spark)
        .tableName("employees")
        .addColumn("id", "INT")
        .addColumn("name", "STRING")
        .addColumn("department", "STRING")
        .execute()
)
```

## 9.2 Create or Replace

```python
(
    DeltaTable.createOrReplace(spark)
        .tableName("employees")
        .addColumn("id", "INT")
        .addColumn("name", "STRING")
        .execute()
)
```

## 9.3 Table Comment

```python
(
    DeltaTable.create(spark)
        .tableName("employees")
        .comment("Employee master table")
        .addColumn("id", "INT")
        .addColumn("name", "STRING")
        .execute()
)
```

## 9.4 Column Comments

```python
(
    DeltaTable.create(spark)
        .tableName("employees")
        .addColumn("id", "INT", comment="Employee ID")
        .addColumn("name", "STRING", comment="Employee name")
        .execute()
)
```

## 9.5 NOT NULL

```python
(
    DeltaTable.create(spark)
        .tableName("employees")
        .addColumn("id", "INT", nullable=False)
        .addColumn("name", "STRING", nullable=False)
        .execute()
)
```

## 9.6 Generated Column

```python
(
    DeltaTable.create(spark)
        .tableName("employees")
        .addColumn("id", "INT")
        .addColumn("first_name", "STRING")
        .addColumn("last_name", "STRING")
        .addColumn(
            "full_name",
            "STRING",
            generatedAlwaysAs="concat(first_name, ' ', last_name)"
        )
        .execute()
)
```

## 9.7 Partitioned

```python
(
    DeltaTable.create(spark)
        .tableName("employees")
        .addColumn("id", "INT")
        .addColumn("name", "STRING")
        .addColumn("department", "STRING")
        .partitionedBy("department")
        .execute()
)
```

## 9.8 Liquid Clustering

```python
(
    DeltaTable.create(spark)
        .tableName("employees")
        .addColumn("id", "INT")
        .addColumn("name", "STRING")
        .addColumn("department", "STRING")
        .clusterBy("department")
        .execute()
)
```

## 9.9 Location

```python
(
    DeltaTable.create(spark)
        .tableName("employees")
        .location("/mnt/delta/employees")
        .addColumn("id", "INT")
        .addColumn("name", "STRING")
        .execute()
)
```

## 9.10 Table Properties

```python
(
    DeltaTable.create(spark)
        .tableName("employees")
        .addColumn("id", "INT")
        .addColumn("name", "STRING")
        .property("property1", "value1")
        .property("property2", "value2")
        .execute()
)
```

## 9.11 Everything Together

```python
(
    DeltaTable.createOrReplace(spark)
        .tableName("employees")
        .location("/mnt/delta/employees")
        .comment("Employee master table")
        .addColumn("id", "INT", nullable=False, comment="Employee ID")
        .addColumn("name", "STRING", comment="Employee name")
        .addColumn("department", "STRING")
        .addColumn("full_name", "STRING", generatedAlwaysAs="name")
        .partitionedBy("department")
        .clusterBy("id")
        .property("property1", "value1")
        .execute()
)
```

---

# 10. PySpark — DataFrameWriter (`df.write`) API

This approach uses the DataFrame's schema and data.

## 10.1 Basic

```python
df.write \\
    .format("delta") \\
    .mode("overwrite") \\
    .saveAsTable("employees")
```

## 10.2 Append

```python
df.write \\
    .format("delta") \\
    .mode("append") \\
    .saveAsTable("employees")
```

## 10.3 External Table / Explicit Path

```python
df.write \\
    .format("delta") \\
    .mode("overwrite") \\
    .option("path", "/mnt/delta/employees") \\
    .saveAsTable("employees")
```

## 10.4 Partitioned

```python
df.write \\
    .format("delta") \\
    .mode("overwrite") \\
    .partitionBy("department") \\
    .saveAsTable("employees")
```

## 10.5 Partitioned + External

```python
df.write \\
    .format("delta") \\
    .mode("overwrite") \\
    .option("path", "/mnt/delta/employees") \\
    .partitionBy("department") \\
    .saveAsTable("employees")
```

## 10.6 Schema Evolution

```python
df.write \\
    .format("delta") \\
    .mode("append") \\
    .option("mergeSchema", "true") \\
    .saveAsTable("employees")
```

---

# 11. PySpark — DataFrameWriterV2 (`df.writeTo`) API

The DataFrameWriterV2 API is more table-oriented than the classic `df.write` API.

## 11.1 Basic

```python
df.writeTo("employees") \\
    .using("delta") \\
    .create()
```

## 11.2 Create or Replace

```python
df.writeTo("employees") \\
    .using("delta") \\
    .createOrReplace()
```

## 11.3 Append

```python
df.writeTo("employees") \\
    .using("delta") \\
    .append()
```

## 11.4 Table Properties

```python
df.writeTo("employees") \\
    .using("delta") \\
    .tableProperty("property1", "value1") \\
    .tableProperty("property2", "value2") \\
    .create()
```

## 11.5 Partitioning

```python
df.writeTo("employees") \\
    .using("delta") \\
    .partitionedBy("department") \\
    .create()
```

## 11.6 Clustering

```python
df.writeTo("employees") \\
    .using("delta") \\
    .clusterBy("department") \\
    .create()
```

## 11.7 Partition + Properties

```python
df.writeTo("employees") \\
    .using("delta") \\
    .partitionedBy("department") \\
    .tableProperty("property1", "value1") \\
    .create()
```

---

# 12. PySpark `spark.sql()` — Execute Spark SQL

Any supported Spark SQL CREATE TABLE statement can also be executed from PySpark:

```python
spark.sql("""
    CREATE TABLE employees (
        id INT,
        name STRING,
        department STRING
    )
    USING DELTA
""")
```

Example:

```python
spark.sql("""
    CREATE TABLE employees (
        id INT NOT NULL,
        name STRING,
        department STRING
    )
    USING DELTA
    PARTITIONED BY (department)
    TBLPROPERTIES (
        'property1' = 'value1'
    )
""")
```

This is technically **PySpark executing Spark SQL**, not a DataFrame API.

---

# 13. Complete Variant Matrix

| Variant | Spark SQL | DeltaTable API | `df.write` | `df.writeTo` |
|---|---:|---:|---:|---:|
| Basic table | ✅ | ✅ | ✅ | ✅ |
| `IF NOT EXISTS` | ✅ | API equivalent varies | — | — |
| `OR REPLACE` | ✅ | ✅ `createOrReplace()` | overwrite | ✅ |
| Explicit schema | ✅ | ✅ | ❌ | ❌ |
| DataFrame-derived schema | CTAS | ❌ | ✅ | ✅ |
| CTAS | ✅ | ❌ | — | — |
| Column comments | ✅ | ✅ | ❌ | ❌ |
| Table comment | ✅ | ✅ | Limited | — |
| `NOT NULL` | ✅ | ✅ | ❌ | ❌ |
| Generated columns | ✅ | ✅ | ❌ | Limited/different |
| Identity columns | ✅ | Supported where API/runtime supports it | ❌ | Depends on support |
| Partitioning | ✅ | ✅ | ✅ | ✅ |
| Liquid clustering | ✅ | ✅ | Supported where API/runtime supports it | ✅ |
| `LOCATION` | ✅ | ✅ | ✅ via `option("path", ...)` | Different table semantics |
| `TBLPROPERTIES` | ✅ | ✅ `.property()` | Not equivalent to arbitrary table properties | ✅ `.tableProperty()` |
| Schema evolution | DDL/write mechanisms | Limited | ✅ `mergeSchema` | API-dependent |
| External Delta table | ✅ | ✅ | ✅ | Possible with supported API |
| Managed Delta table | ✅ | ✅ | ✅ | ✅ |

---

# 14. Mental Model

```text
                         CREATE DELTA TABLE
                                |
          ┌─────────────────────┼─────────────────────┐
          |                     |                     |
      Spark SQL           DeltaTable API       DataFrame APIs
          |                     |                     |
          |                     |              ┌──────┴──────┐
          |                     |              |             |
   CREATE TABLE          DeltaTable.create   df.write    df.writeTo
                         createOrReplace      saveAsTable
```

## Spark SQL

```sql
CREATE TABLE ...
USING DELTA ...
```

Best for DDL, explicit schema, SQL workflows, table properties, partitioning, clustering, and CTAS.

## DeltaTable Builder

```python
DeltaTable.create(spark) ...
```

Best for programmatic table definition, explicit schema, constraints, generated columns, comments, properties, partitioning, and clustering.

## DataFrameWriter

```python
df.write.format("delta") ...
```

Best when you already have a DataFrame and want to persist its data as a Delta table.

## DataFrameWriterV2

```python
df.writeTo("table").using("delta") ...
```

Best for modern table-oriented DataFrame writes, including table properties, partitioning, and create/replace/append semantics.

---

# 15. Quick Interview Cheat Sheet

```text
CREATE EMPTY DELTA TABLE
─────────────────────────
SQL:
CREATE TABLE t (...) USING DELTA;

PySpark:
DeltaTable.create(spark)...
    .addColumn(...)
    .execute()


CREATE DELTA TABLE FROM DATAFRAME
──────────────────────────────────
df.write
    .format("delta")
    .saveAsTable("t")


MODERN DATAFRAME TABLE API
──────────────────────────
df.writeTo("t")
    .using("delta")
    .create()


CREATE TABLE FROM QUERY
───────────────────────
CREATE TABLE t
USING DELTA
AS SELECT ...


EXTERNAL DELTA TABLE
────────────────────
CREATE TABLE t (...)
USING DELTA
LOCATION 'path';


PARTITIONED DELTA TABLE
───────────────────────
CREATE TABLE t (...)
USING DELTA
PARTITIONED BY (...);


LIQUID CLUSTERED DELTA TABLE
────────────────────────────
CREATE TABLE t (...)
USING DELTA
CLUSTER BY (...);


TABLE PROPERTIES
────────────────
TBLPROPERTIES (...)


GENERATED COLUMN
────────────────
column_name type
GENERATED ALWAYS AS (...)


IDENTITY COLUMN
───────────────
id BIGINT GENERATED ALWAYS AS IDENTITY
```

---

# 16. Key Distinction

```text
DeltaTable.create()
        ↓
Define the TABLE
        ↓
Schema is explicitly specified


df.write
        ↓
Write the DATAFRAME
        ↓
Schema comes from DataFrame


df.writeTo()
        ↓
Write DataFrame using TABLE-oriented API
        ↓
DataFrame schema + table operations


CREATE TABLE AS SELECT
        ↓
Create TABLE from QUERY
        ↓
Schema comes from SELECT


spark.sql("CREATE TABLE ...")
        ↓
Execute Spark SQL from PySpark
```

# Ingesting Semi-Structured Data: JSON in Databricks

This version includes **SQL and PySpark DataFrame API alternatives
wherever a practical equivalent exists**. Some Databricks SQL features,
such as the `read_files()` table-valued function, do not have a
one-to-one DataFrame API call; the corresponding Spark file reader or
Auto Loader source is shown instead.

## 1. JSON fundamentals

JSON (JavaScript Object Notation) is a text-based format for
representing semi-structured data. A JSON object contains key-value
pairs enclosed in `{}`. Values can be strings, numbers, booleans,
objects, arrays, or `null`.

``` json
{
  "customer_id": 101,
  "name": "John Doe",
  "is_active": true,
  "address": {"city": "New York", "state": "NY"},
  "skills": ["SQL", "Python", "Spark"]
}
```

This example contains flat fields, a nested object (`address`), and an
array (`skills`). JSON is semi-structured because records may have
optional fields, different nesting, or new fields as source systems
evolve.

## 2. Ingesting JSON files

### 2.1 Batch ingestion with SQL `read_files()`

``` sql
SELECT *
FROM read_files('/Volumes/main/raw/customers/', format => 'json');
```

Create a Delta table:

``` sql
CREATE OR REPLACE TABLE main.bronze.customers AS
SELECT *
FROM read_files('/Volumes/main/raw/customers/', format => 'json');
```

This is a batch read. Running the query again reads the files again; it
is not a continuously running ingestion process.

**PySpark DataFrame API alternative**

There is no direct `spark.read.read_files()` method. For a batch JSON
read, use Spark's JSON data source:

``` python
df = (
    spark.read
        .format("json")
        .load("/Volumes/main/raw/customers/")
)
display(df)
```

Create or replace a managed Delta table:

``` python
(
    df.write
      .format("delta")
      .mode("overwrite")
      .saveAsTable("main.bronze.customers")
)
```

This write example replaces the table's data. Choose the write mode and
table-management approach deliberately for production.

### 2.2 JSON Lines vs. multi-line JSON

Spark's default JSON reader expects JSON Lines / newline-delimited JSON:
one complete JSON object per line.

``` json
{"customer_id":101,"name":"John"}
{"customer_id":102,"name":"Mary"}
```

For a file containing one multi-line JSON document per record, set
`multiLine` to `true`:

``` python
df = (
    spark.read
        .format("json")
        .option("multiLine", "true")
        .load("/Volumes/main/raw/customers/")
)
```

SQL equivalent:

``` sql
SELECT *
FROM read_files(
    '/Volumes/main/raw/customers/',
    format => 'json',
    multiLine => true
);
```

Choose the setting based on the physical layout of the source files.

### 2.3 Incremental ingestion with Auto Loader

Auto Loader incrementally discovers and processes new files arriving in
cloud storage.

**PySpark**

``` python
df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/Volumes/main/ops/schema/customers")
        .load("/Volumes/main/raw/customers/")
)

(
    df.writeStream
        .option("checkpointLocation", "/Volumes/main/ops/checkpoints/customers")
        .toTable("main.bronze.customers")
)
```

- `cloudFiles.format` specifies the underlying file format.
- `cloudFiles.schemaLocation` stores schema information used by Auto
  Loader.
- `checkpointLocation` stores streaming progress and state.
- The streaming query must be running, or restarted by its job or
  pipeline, to process new files.
- Preserve schema and checkpoint locations across restarts. Do not share
  one checkpoint between independent streams.

**SQL / Lakeflow streaming-table equivalent**

``` sql
CREATE OR REFRESH STREAMING TABLE main.bronze.customers
AS
SELECT *
FROM STREAM read_files(
    '/Volumes/main/raw/customers/',
    format => 'json'
);
```

This is a Lakeflow Spark Declarative Pipelines definition. It is not a
general-purpose SQL statement that starts a notebook streaming query;
the pipeline manages execution and progress.

## 3. Schema handling

### 3.1 Schema inference when reading files

Spark can inspect JSON files and infer a schema when one is not
supplied.

**PySpark**

``` python
df = (
    spark.read
        .format("json")
        .load("/Volumes/main/raw/customers/")
)
df.printSchema()
```

**SQL**

``` sql
SELECT *
FROM read_files('/Volumes/main/raw/customers/', format => 'json');
```

Spark's JSON reader infers a schema by default when one is not supplied.
Auto Loader has its own schema inference and type-inference behaviour;
configure its options deliberately. Inference is convenient for
exploration, but inferred fields and types can depend on the input
encountered. For production pipelines, consider managing the schema
explicitly.

### 3.2 Define an explicit schema

**PySpark**

``` python
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, BooleanType, ArrayType
)

customer_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField(
        "address",
        StructType([
            StructField("city", StringType(), True),
            StructField("state", StringType(), True)
        ]),
        True
    ),
    StructField("skills", ArrayType(StringType()), True)
])

df = (
    spark.read
        .schema(customer_schema)
        .json("/Volumes/main/raw/customers/")
)
```

**SQL equivalent**

``` sql
SELECT *
FROM read_files(
    '/Volumes/main/raw/customers/',
    format => 'json',
    schema => '
      customer_id INT,
      name STRING,
      is_active BOOLEAN,
      address STRUCT<city: STRING, state: STRING>,
      skills ARRAY<STRING>
    '
);
```

A JSON object maps naturally to `STRUCT`; a JSON array maps naturally to
`ARRAY`.

## 4. Working with JSON stored in a STRING column

Sometimes an entire JSON document is stored as text in a STRING column
rather than being parsed into nested columns.

|  id | json_column                                                   |
|----:|---------------------------------------------------------------|
|   1 | `{"name":"John Doe","age":35,"address":{"city":"New York"}}`  |
|   2 | `{"name":"Kristi Doe","age":40,"address":{"city":"Chicago"}}` |

There are three common approaches: direct extraction, parsing into
STRUCT, or using VARIANT.

### 4.1 Approach 1: Extract fields directly from STRING

**Spark SQL / Databricks SQL**

``` sql
SELECT
    id,
    json_column:name::STRING AS name,
    json_column:age::INT AS age,
    json_column:address.city::STRING AS city
FROM customer_json;
```

The colon-path operator extracts values from JSON text. The extracted
values are VARIANT values, so cast them when a specific SQL type is
required.

**PySpark DataFrame API alternative**

Use `get_json_object()` with a JSONPath expression:

``` python
from pyspark.sql import functions as F

df_extracted = customer_df.select(
    "id",
    F.get_json_object("json_column", "$.name").alias("name"),
    F.get_json_object("json_column", "$.age").cast("int").alias("age"),
    F.get_json_object("json_column", "$.address.city").alias("city")
)
display(df_extracted)
```

`get_json_object()` returns STRING values. Cast numeric or Boolean
values to the required type.

### 4.2 Approach 2: Parse STRING into STRUCT with `from_json()`

A `STRUCT` is a typed collection of named fields. Use `from_json()` when
the expected JSON structure is known.

Example schema:

``` text
STRUCT<
    name: STRING,
    age: INT,
    address: STRUCT<
        city: STRING,
        state: STRING
    >
>
```

**Spark SQL**

``` sql
SELECT
    from_json(
        json_column,
        'STRUCT<
            name: STRING,
            age: INT,
            address: STRUCT<
                city: STRING,
                state: STRING
            >
        >'
    ) AS parsed_json
FROM customer_json;
```

**PySpark DataFrame API**

``` python
from pyspark.sql import functions as F

schema_ddl = """
    name STRING,
    age INT,
    address STRUCT<
        city: STRING,
        state: STRING
    >
"""

parsed_df = customer_df.withColumn(
    "parsed_json",
    F.from_json(F.col("json_column"), schema_ddl)
)

result_df = parsed_df.select(
    "id",
    F.col("parsed_json.name").alias("name"),
    F.col("parsed_json.age").alias("age"),
    F.col("parsed_json.address.city").alias("city")
)
display(result_df)
```

Use STRUCT when the structure is known and typed fields are useful for
downstream transformations.

### 4.3 Derive a schema with `schema_of_json()`

`schema_of_json()` derives a schema string in DDL format from a
representative JSON sample.

**Spark SQL**

``` sql
SELECT schema_of_json(
    '{"name":"John Doe","age":35,
      "address":{"city":"New York","state":"NY"}}'
);
```

**PySpark DataFrame API**

``` python
from pyspark.sql import functions as F

sample_json = (
    customer_df
        .where(F.col("id") == 1)
        .select("json_column")
        .first()["json_column"]
)

schema_ddl = (
    spark.range(1)
         .select(F.schema_of_json(F.lit(sample_json)).alias("schema"))
         .first()["schema"]
)

print(schema_ddl)
```

Important: `spark.sql("SELECT ...")` returns a DataFrame, not the JSON
string itself. Retrieve the sample value with an action such as
`.first()` before passing it to `schema_of_json()`.

`schema_of_json()` expects a JSON literal or foldable expression. It
derives a schema from the supplied sample; it does not guarantee that
every record in an evolving dataset conforms to that schema.

**Use the inferred DDL schema with `from_json()`**

``` python
parsed_df = customer_df.withColumn(
    "parsed_json",
    F.from_json(F.col("json_column"), schema_ddl)
)

result_df = parsed_df.select(
    "id",
    F.col("parsed_json.name").alias("name"),
    F.col("parsed_json.age").alias("age"),
    F.col("parsed_json.address.city").alias("city")
)
display(result_df)
```

You do not need to call `schema_of_json()` every time. If the schema is
already known, define the DDL schema once and pass it directly to
`from_json()`. This is usually simpler and more predictable for
production pipelines.

### 4.4 Alternative schema inference from a JSON RDD

You can infer a DataFrame schema from a collection of JSON strings:

``` python
json_rdd = (
    spark.table("json_practice")
         .select("json_column")
         .rdd
         .map(lambda row: row["json_column"])
)

inferred_df = spark.read.json(json_rdd)
inferred_df.printSchema()
display(inferred_df)
```

This parses the JSON strings into a DataFrame with an inferred schema.
It is useful for exploration, but it is not the same as applying a fixed
schema to an existing DataFrame column. For production, explicitly
manage the schema and parsing behaviour.

## 5. Approach 3: Use the VARIANT data type

`VARIANT` is a Databricks data type for semi-structured values,
including JSON. It allows JSON to be stored and queried without defining
a complete fixed STRUCT schema upfront.

### 5.1 Convert JSON STRING into VARIANT

**Spark SQL**

``` sql
SELECT
    id,
    parse_json(json_column) AS json_variant
FROM customer_json;
```

**PySpark DataFrame API**

``` python
from pyspark.sql import functions as F

variant_df = customer_df.withColumn(
    "json_variant",
    F.parse_json(F.col("json_column"))
)
display(variant_df)
```

`parse_json()` is available in supported Databricks runtimes / PySpark
APIs. Check the runtime version if the function is not recognized.

### 5.2 Extract fields from VARIANT

**Spark SQL**

``` sql
SELECT
    json_variant:name::STRING AS name,
    json_variant:age::INT AS age,
    json_variant:address.city::STRING AS city
FROM parsed_variant_table;
```

**PySpark DataFrame API**

``` python
from pyspark.sql import functions as F

result_df = variant_df.select(
    "id",
    F.variant_get("json_variant", "$.name", "STRING").alias("name"),
    F.variant_get("json_variant", "$.age", "INT").alias("age"),
    F.variant_get("json_variant", "$.address.city", "STRING").alias("city")
)
display(result_df)
```

`variant_get()` extracts the value at a JSON path and casts it to the
requested target type. It returns NULL when the path does not exist and
raises an error if the cast fails.

For NULL instead of a cast error, use `try_variant_get()`:

``` python
result_df = variant_df.select(
    "id",
    F.try_variant_get("json_variant", "$.age", "INT").alias("age")
)
```

**DataFrame API using SQL expressions**

Where supported by the Databricks runtime, you can also use SQL
expression syntax from the DataFrame API:

``` python
result_df = variant_df.selectExpr(
    "id",
    "json_variant:name::STRING AS name",
    "json_variant:age::INT AS age",
    "json_variant:address.city::STRING AS city"
)
```

`variant_get()` is the explicit PySpark function alternative.

## 6. STRING vs STRUCT vs VARIANT

| Aspect                        | STRING                             | STRUCT                  | VARIANT                    |
|-------------------------------|------------------------------------|-------------------------|----------------------------|
| Representation                | JSON text                          | Typed fields            | Semi-structured value      |
| Fixed schema required upfront | No                                 | Yes                     | No                         |
| Nested data                   | Extract from JSON text             | Native nested fields    | Query using JSON paths     |
| Data types                    | Text until extracted/cast          | Defined in schema       | Extract and cast as needed |
| Best suited for               | Raw JSON text or simple extraction | Known, stable structure | Flexible or evolving JSON  |

Practical selection:

- **STRING:** retain the original JSON text or extract only a few
  fields.
- **STRUCT:** use when the schema is known and consistent typed columns
  are needed.
- **VARIANT:** use when the JSON structure is flexible or may vary
  between records.

## 7. Important considerations

1.  **Understand the file layout.** Determine whether the source is JSON
    Lines (one JSON object per line) or a multi-line JSON document.
    Configure `multiLine` appropriately.
2.  **Choose inference or an explicit schema deliberately.** Inference
    is convenient for exploration; explicit schemas provide stronger
    control for production transformations.
3.  **A sample is not the whole dataset.** `schema_of_json()` derives a
    schema from the supplied sample. A field present only in another
    record may not appear in that schema.
4.  **Expect schema variation.** Optional fields may be absent, and new
    fields may appear as source systems evolve.
5.  **Preserve raw data when required.** A Bronze layer may retain the
    original JSON payload or raw files so records can be reprocessed if
    parsing rules change.
6.  **Use nested types appropriately.** JSON objects map naturally to
    STRUCT, and JSON arrays map naturally to ARRAY.
7.  **Cast extracted values.** JSON path extraction may return VARIANT
    values; cast them to STRING, INT, BOOLEAN, or another suitable SQL
    type for typed calculations.
8.  **Separate malformed JSON from schema variation.** Invalid JSON
    syntax is a parsing problem. A valid JSON document with an
    unexpected field is a schema variation.
9.  **For Auto Loader, persist operational state.** Use durable schema
    and checkpoint locations and preserve them across job restarts.
10. **Check runtime support.** VARIANT and its PySpark functions require
    a supported Databricks Runtime / SQL environment.

## 8. Quick reference: SQL and PySpark

| Requirement                  | SQL                                    | PySpark                                      |
|------------------------------|----------------------------------------|----------------------------------------------|
| Batch-read JSON files        | `read_files(..., format => 'json')`    | `spark.read.format("json").load(path)`       |
| Write a Delta table          | `CREATE TABLE AS SELECT`               | `df.write.format("delta").saveAsTable(name)` |
| Read JSON incrementally      | `STREAM read_files(...)` in a pipeline | `spark.readStream.format("cloudFiles")`      |
| Extract JSON text field      | `json_column:name`                     | `get_json_object(col, "$.name")`             |
| Infer schema from sample     | `schema_of_json(...)`                  | `F.schema_of_json(F.lit(sample_json))`       |
| Parse JSON text into STRUCT  | `from_json(...)`                       | `F.from_json(col, schema)`                   |
| Parse JSON text into VARIANT | `parse_json(...)`                      | `F.parse_json(col)`                          |
| Extract typed VARIANT value  | `variant_get(...)`                     | `F.variant_get(col, path, target_type)`      |
| Tolerant VARIANT extraction  | `try_variant_get(...)`                 | `F.try_variant_get(col, path, target_type)`  |

Note: SQL `read_files()` has no exact same-named DataFrame method. Use
Spark's JSON reader for batch file reads and Auto Loader (`cloudFiles`)
for incremental file ingestion.

## 9. Official documentation

- [Databricks PySpark
  `schema_of_json`](https://docs.databricks.com/aws/en/pyspark/reference/functions/schema_of_json)
- [Databricks PySpark
  `from_json`](https://docs.databricks.com/aws/en/pyspark/reference/functions/from_json)
- [Databricks PySpark functions, including VARIANT
  functions](https://docs.databricks.com/aws/en/pyspark/reference/functions/)
- [Databricks VARIANT data
  type](https://docs.databricks.com/aws/en/sql/language-manual/data-types/variant-type)
- [Databricks complex data
  types](https://docs.databricks.com/aws/en/semi-structured/complex-types)

## Key takeaway

JSON ingestion in Databricks involves two decisions: how to read the
JSON files and how to represent their structure for querying.

Use `read_files()` or Spark's JSON reader for batch ingestion, and Auto
Loader for incremental file ingestion. For JSON stored as text, choose
direct extraction, STRUCT parsing, or VARIANT according to how fixed or
flexible the data structure is. Use the DataFrame API alternatives in
this document when you prefer PySpark transformations over Spark SQL.

# Databricks Delta Lake — Overall Table Architecture

## 1. What Is a Delta Table?

At a high level, a Delta table can be understood as:

> **Data Files + Transaction Log + Table Metadata**

The most important architectural distinction is:

```text
Parquet
   ↓
Actual business data

_delta_log
   ↓
Transaction history + table state information

Metadata
   ↓
Schema + table properties + partition information
```

Delta Lake combines these pieces to provide database-like capabilities on top of cloud object storage.

---

# 2. Big-Picture Architecture

```text
                         DATABRICKS
                              │
                              ▼
                        UNITY CATALOG
                              │
                  Metadata / Governance
                              │
                              ▼
                         DELTA TABLE
                              │
              ┌───────────────┴───────────────┐
              │                               │
              ▼                               ▼
       TRANSACTION LOG                   DATA FILES
          _delta_log                       Parquet
              │                               │
        ┌─────┴─────┐                         │
        │           │                         │
       JSON     Checkpoints                   │
        │           │                         │
        └─────┬─────┘                         │
              │                               │
              └──────────────┬────────────────┘
                             ↓
                        TABLE STATE
                             ↓
                     Active Parquet Files
                             ↓
                         Actual Data
```

The key idea is:

> **The transaction log determines which Parquet files represent the table state; the Parquet files contain the actual data.**

---

# 3. Physical Structure of a Delta Table

Suppose we have:

```sql
CREATE TABLE employees (
    employee_id BIGINT,
    employee_name STRING,
    department STRING,
    salary DOUBLE
)
USING DELTA;
```

Conceptually, the storage might look like:

```text
employees/
│
├── _delta_log/
│   ├── 00000000000000000000.json
│   ├── 00000000000000000001.json
│   ├── 00000000000000000002.json
│   ├── 00000000000000000003.json
│   ├── ...
│   └── 00000000000000000010.checkpoint.parquet
│
├── part-00000-abc.parquet
├── part-00001-def.parquet
├── part-00002-ghi.parquet
└── part-00003-jkl.parquet
```

There are two major physical areas:

### Data files

```text
part-*.parquet
```

These contain the actual rows.

### `_delta_log`

This contains the transaction history and state information required to determine which files belong to the table.

---

# 4. The Three-Layer Mental Model

For interviews, a very useful way to remember Delta architecture is:

```text
┌──────────────────────────────────────────┐
│             TABLE METADATA               │
│                                          │
│   Schema / Partition / Properties        │
└────────────────────┬─────────────────────┘
                     │
┌────────────────────▼─────────────────────┐
│            TRANSACTION LOG               │
│                                          │
│   JSON + Checkpoints                     │
│   add / remove / metadata / protocol     │
│   versions / commits                     │
└────────────────────┬─────────────────────┘
                     │
┌────────────────────▼─────────────────────┐
│              DATA FILES                  │
│                                          │
│          Parquet files                   │
│          Actual data                     │
└──────────────────────────────────────────┘
```

This is one of the most useful mental models for understanding Delta.

---

# 5. Parquet Data Files

Parquet files store the **actual business data**.

For example:

```text
customer_id | name  | city
------------|-------|-------
1           | John  | Delhi
2           | Alex  | Mumbai
3           | Sara  | Pune
```

The data is physically stored in `.parquet` files.

```text
Delta Table
│
├── part-00000.parquet
├── part-00001.parquet
└── part-00002.parquet
```

Think:

> **Parquet = actual data**

---

# 6. Why Does Delta Use Parquet?

Parquet is a columnar format that is well suited for analytical workloads.

Important characteristics include:

- Columnar storage
- Compression
- Encoding
- Column pruning
- Predicate pushdown
- Efficient analytical scans
- Row groups and column chunks
- Data skipping capabilities when statistics are available

For example:

```sql
SELECT name, salary
FROM employees;
```

Because Parquet is columnar, the engine can focus on the required columns rather than unnecessarily reading every column.

This is called:

> **Column pruning**

---

# 7. High-Level Parquet Structure

A Parquet file can be thought of as:

```text
Parquet File
      ↓
  Row Groups
      ↓
Column Chunks
      ↓
   Pages
```

For Databricks interviews, the most important initial hierarchy is:

> **File → Row Group → Column Chunk**

Row groups allow the engine to reason about subsets of rows, while column chunks support efficient columnar access.

---

# 8. Partition Pruning, Column Pruning and Predicate Pushdown

These are different optimization concepts.

### Partition pruning

Avoids unnecessary partitions/files.

```text
year=2025  ❌
year=2026  ✅
```

### Column pruning

Avoids unnecessary columns.

```text
customer_id  ❌
name         ✅
city         ❌
salary       ✅
```

### Predicate pushdown / data skipping

Can avoid reading irrelevant data sections when filter conditions and available statistics show that they cannot contain matching rows.

Conceptually:

```text
Partition pruning
       ↓
Which partitions/files?

Column pruning
       ↓
Which columns?

Predicate/data skipping
       ↓
Which data sections/row groups?
```

---

# 9. Parquet Files Are Generally Treated as Immutable

This is critical for understanding `UPDATE`, `DELETE`, and `MERGE`.

Delta generally does not modify an existing Parquet file in place.

Suppose:

```text
file-A.parquet

ID | Name
---|-----
1  | John
2  | Alex
3  | Sara
```

An update such as:

```sql
UPDATE customers
SET Name = 'Johnny'
WHERE ID = 1;
```

can conceptually result in:

```text
file-A.parquet
      ↓
  old file
      ↓
REMOVE
      ↓
file-B.parquet
      ↓
  new file
      ↓
ADD
```

The new file contains the updated state.

This is one of the fundamental ideas behind Delta.

---

# 10. Transaction Log

The Delta transaction log lives under:

```text
_delta_log/
```

It contains information needed to reconstruct the table's committed state.

Conceptually:

```text
_delta_log
     │
     ├── Version 0
     ├── Version 1
     ├── Version 2
     ├── Version 3
     └── ...
```

Every successful transaction creates a new table version.

You can inspect table history with:

```sql
DESCRIBE HISTORY employees;
```

---

# 11. JSON Transaction Log Files

The transaction log contains numbered JSON files:

```text
_delta_log/
│
├── 00000000000000000000.json
├── 00000000000000000001.json
├── 00000000000000000002.json
├── 00000000000000000003.json
└── ...
```

The number corresponds to the Delta table version.

For example:

```text
00000000000000000000.json
        ↓
     Version 0

00000000000000000005.json
        ↓
     Version 5
```

Think:

> **Each JSON log file represents the committed changes for a particular version.**

---

# 12. Important Transaction Log Actions

The most important actions to understand are:

```text
add
remove
metaData
protocol
commitInfo
txn
```

### `add`

Indicates that a data file is part of the table state.

```text
add
 ↓
File becomes part of table state
```

### `remove`

Indicates that a file should no longer be considered part of the current logical table state.

```text
remove
 ↓
File leaves current table state
```

Important:

> **REMOVE FROM TABLE ≠ PHYSICALLY DELETE FILE**

### `metaData`

Contains table metadata such as:

- Schema
- Partition columns
- Table properties
- Configuration

### `protocol`

Defines the minimum reader/writer capabilities required to correctly interact with the table.

### `commitInfo`

Contains information about a transaction, such as operation and timestamp.

### `txn`

Contains transaction-related information used for transactional/idempotent behavior in relevant workloads.

---

# 13. The Transaction Log Is the Source of Truth

Suppose physical storage contains:

```text
A.parquet
B.parquet
C.parquet
D.parquet
```

You cannot simply assume all four belong to the current table.

The transaction log determines the current logical table state.

For example:

```text
Physical Storage
        │
        ├── A
        ├── B
        ├── C
        └── D
             │
             ▼
       Transaction Log
             │
             ▼
      Current Table State
             │
             ├── A
             ├── B
             └── D
```

`C` may be an obsolete file.

Therefore:

> **The transaction log is the authoritative record of the table's committed state.**

---

# 14. ADD and REMOVE — The Core Mechanism

Suppose:

```text
Version 0
ADD A
ADD B
```

Current state:

```text
A
B
```

Then:

```text
Version 1
ADD C
```

Current state:

```text
A
B
C
```

Then:

```text
Version 2
REMOVE B
ADD D
```

Current state:

```text
A
C
D
```

Conceptually:

```text
ADD / REMOVE actions
        ↓
Accumulated state
        ↓
Active Parquet files
        ↓
Current table
```

---

# 15. Checkpoints

If Delta had to replay every JSON log file from the beginning of the table's history each time it needed the current state, this would become increasingly expensive.

Imagine:

```text
Version 0
Version 1
Version 2
...
Version 100,000
```

Replaying all of these would involve:

- Reading many files
- Performing many I/O operations
- Parsing JSON
- Processing many actions
- Reconstructing the accumulated state

Delta therefore uses checkpoints.

A checkpoint is:

> **A compacted, persisted representation of the accumulated Delta table state resulting from transaction-log actions up to a particular version.**

---

# 16. What Does "Persisted" Mean?

Persisted simply means:

> **Saved to durable storage so it can be retrieved later.**

Conceptually:

```text
Table state @ Version 100
        ↓
Checkpoint created
        ↓
Written to storage
        ↓
100.checkpoint.parquet
```

The checkpoint is therefore not merely an in-memory object.

---

# 17. What Does a Checkpoint Represent?

A checkpoint is not simply a copy of all the JSON files.

Instead:

```text
Delta JSON Logs
      ↓
Individual actions
      ↓
Accumulate + resolve actions
      ↓
Resulting table state
      ↓
Checkpoint
(compacted representation)
```

For example:

```text
Version 0
ADD A
ADD B

Version 1
ADD C

Version 2
REMOVE B
ADD D
```

Resulting state:

```text
A
C
D
```

The checkpoint at Version 2 represents that accumulated state.

Therefore:

> **JSON logs record changes; checkpoints provide the accumulated state resulting from those changes.**

---

# 18. Why Checkpoints Improve Performance

Suppose there are many versions:

```text
Version 0
Version 1
...
Version 999,999
```

Without checkpoints, Delta would have a much larger transaction history to replay.

With a checkpoint:

```text
Checkpoint @ Version 999,000
          ↓
Already accumulated state
          +
999,001.json
999,002.json
...
999,999.json
          ↓
Current table state
```

So:

> **Checkpoints reduce the amount of transaction-log history Delta needs to replay, improving table-state reconstruction performance and reducing metadata-related I/O.**

The important optimization is not simply:

> "Parquet is faster than JSON."

The bigger optimization is:

> **Delta no longer needs to replay the entire transaction history.**

---

# 19. Checkpoint + Recent JSON Logs

A checkpoint does not replace JSON logs.

Suppose:

```text
Checkpoint @ Version 100
```

Then:

```text
Version 101 → ADD F
Version 102 → REMOVE A
Version 103 → ADD G
```

To determine Version 103:

```text
Checkpoint @ Version 100
        ↓
State @ Version 100
        +
101.json
        +
102.json
        +
103.json
        ↓
State @ Version 103
```

So:

> **Checkpoint provides the starting state, and JSON logs after the checkpoint provide the changes after that state.**

---

# 20. Multiple Checkpoints

A Delta table can have multiple checkpoints:

```text
JSON logs
    ↓
Checkpoint @ 10
    ↓
More JSON logs
    ↓
Checkpoint @ 20
    ↓
More JSON logs
    ↓
Checkpoint @ 30
    ↓
More JSON logs
```

This allows Delta to use a suitable checkpoint when reconstructing a table version.

---

# 21. Checkpoint Does Not Mean JSON Logs Are Immediately Deleted

Creating a checkpoint does not mean old JSON transaction-log files are immediately removed.

Conceptually:

```text
Old JSON logs
       +
Checkpoint
       +
Recent JSON logs
```

Checkpointing and log cleanup are separate concepts.

---

# 22. Data Parquet vs Checkpoint Parquet

Both are `.parquet` files, but they have different purposes.

| File | Purpose |
|---|---|
| Data `.parquet` | Stores actual business data |
| Checkpoint `.parquet` | Stores accumulated Delta table state/log information |

For example:

```text
Delta Table
│
├── part-00000.parquet
│      ↓
│   Customer data
│
├── part-00001.parquet
│      ↓
│   Customer data
│
└── _delta_log/
       │
       └── 00000000000000000010.checkpoint.parquet
              ↓
          Delta state information
```

Important:

> **Not every Parquet file in a Delta table is a business-data file.**

---

# 23. How Delta Reconstructs Table State

Conceptually:

```text
Read Delta Table
      ↓
Find target/current version
      ↓
Find suitable checkpoint
      ↓
Load checkpoint state
      ↓
Read JSON logs after checkpoint
      ↓
Apply new actions
      ↓
Current / Target table state
      ↓
Identify active Parquet files
      ↓
Read actual data
```

For example:

```text
Checkpoint @ Version 100
        ↓
State @ Version 100
        +
101.json
102.json
103.json
104.json
105.json
        ↓
State @ Version 105
        ↓
Active Parquet files
        ↓
Actual table data
```

---

# 24. Current Logical State vs Physical Storage

This distinction is extremely important.

Physical storage might contain:

```text
A.parquet
B.parquet
C.parquet
D.parquet
```

But the current logical table state might be:

```text
A
B
D
```

Why?

Because the transaction log says:

```text
A → active
B → active
C → removed
D → active
```

Therefore:

```text
Physical files
      ↓
Transaction Log
      ↓
Logical table state
      ↓
Active files
```

The physical presence of a file does not automatically mean that it belongs to the current logical table.

---

# 25. UPDATE Architecture

Suppose:

```text
Version 1

A.parquet
B.parquet
C.parquet
```

An update affects data in `C.parquet`.

Conceptually:

```text
C.parquet
    ↓
Read affected data
    ↓
Create D.parquet
    ↓
Transaction commit
    ↓
REMOVE C
ADD D
```

Current state becomes:

```text
A
B
D
```

While:

```text
C
```

may still physically exist.

---

# 26. DELETE Architecture

Suppose:

```text
A.parquet
B.parquet
C.parquet
```

A delete affects data in `C`.

Conceptually:

```text
C.parquet
    ↓
Create replacement file
    ↓
D.parquet
    ↓
REMOVE C
ADD D
```

Current logical state:

```text
A
B
D
```

Again, `C.parquet` may still physically exist until cleanup.

---

# 27. Logical Delete vs Physical Delete

This is a critical Delta concept.

```text
DELETE
   ↓
Logical removal from table state
```

does not necessarily mean:

```text
Physical file deletion
```

Later:

```text
VACUUM
   ↓
Physical cleanup of eligible obsolete files
```

So:

```text
DELETE
   ↓
Logical change

VACUUM
   ↓
Physical cleanup
```

---

# 28. Why Can Old Parquet Files Remain?

Old Parquet files can remain for reasons including:

- Time travel
- Recovery
- Historical versions
- Transaction safety

For example:

```text
Version 10
   ↓
file-A.parquet

Version 11
   ↓
file-B.parquet
```

If `file-A.parquet` is still retained, Delta may be able to reconstruct Version 10.

Once relevant old physical files are removed, that historical state may no longer be queryable.

---

# 29. VACUUM

`VACUUM` is a physical cleanup operation.

Conceptually:

```text
Obsolete Parquet files
        ↓
     VACUUM
        ↓
Physical cleanup
```

Important:

> **Checkpoint improves state reconstruction; VACUUM cleans up obsolete physical files.**

Therefore:

```text
Checkpoint ≠ VACUUM
```

---

# 30. Time Travel

Delta versions allow historical table states to be queried.

For example:

```sql
SELECT *
FROM employees
VERSION AS OF 10;
```

Conceptually:

```text
Transaction Log
      +
Checkpoint
      +
Required Parquet files
      ↓
Historical table state
```

Checkpoint helps with efficient state reconstruction.

Time Travel is the feature that allows the user to access a historical version.

Therefore:

```text
Checkpoint
    ↓
Performance / state reconstruction

Time Travel
    ↓
Access historical table state
```

---

# 31. Why Delta Can Provide ACID Transactions

The architecture of:

```text
Parquet data files
        +
Transaction log
        +
Atomic commits
        +
Versioned table state
```

allows Delta to provide database-like transactional behavior on object storage.

### Atomicity

A transaction is committed as a unit.

```text
Transaction
 ├── file A
 ├── file B
 └── file C
       ↓
    Commit
```

### Consistency

Delta maintains valid table state according to its schema, protocol, and applicable rules.

### Isolation

Readers see committed table versions rather than partially written states.

```text
Writer
  ↓
Write files
  ↓
Commit
  ↓
New table version
```

### Durability

Once a transaction is committed, its state is persisted in storage.

---

# 32. Optimistic Concurrency

Delta also supports transactional concurrency control.

Conceptually:

```text
             Version 10
              /      \
             /        \
         Job A        Job B
           ↓            ↓
       Make changes  Make changes
           ↓            ↓
        Commit        Commit
           ↓            ↓
       Version 11    Conflict
```

If Job A commits first and Job B's operation conflicts with the new table state, Job B may fail or need to retry depending on the operation.

This is one of the major differences between a transactional Delta table and a plain collection of Parquet files.

---

# 33. Write Architecture

A simplified write flow is:

```text
DataFrame / SQL
       ↓
Spark Engine
       ↓
Write Parquet files
       ↓
Transaction Commit
       ↓
_delta_log
       ↓
New Delta Table Version
```

The exact implementation is more sophisticated, but this is the correct conceptual model.

---

# 34. Read Architecture

When Spark reads a Delta table:

```text
SELECT
   ↓
Delta Table Reader
   ↓
Read transaction/state information
   ↓
Determine active files
   ↓
Apply partition pruning
   ↓
Apply file/data skipping where possible
   ↓
Apply column pruning
   ↓
Read required Parquet data
   ↓
Process query
```

The reader does not simply read every physical Parquet file in the directory.

It first determines which files belong to the requested table state.

---

# 35. Small Files Problem

A Delta table can accumulate many small Parquet files:

```text
part-001.parquet
part-002.parquet
part-003.parquet
...
part-100000.parquet
```

For example:

```text
10 KB
15 KB
8 KB
20 KB
...
```

A very large number of small files can create overhead from:

- File opening
- Metadata handling
- Scheduling
- Storage operations

Conceptually:

```text
1 large file
    ↓
Few file operations

vs.

1,000,000 small files
    ↓
Large file/metadata overhead
```

---

# 36. OPTIMIZE

Databricks provides `OPTIMIZE` to compact small files.

Example:

```sql
OPTIMIZE customers;
```

Conceptually:

```text
Before:

A.parquet
B.parquet
C.parquet
D.parquet
E.parquet
F.parquet

        ↓
     OPTIMIZE
        ↓

Large1.parquet
Large2.parquet
```

The transaction log updates the logical table state:

```text
REMOVE A
REMOVE B
REMOVE C
REMOVE D
REMOVE E
REMOVE F

ADD Large1
ADD Large2
```

So:

```text
Small files
    ↓
OPTIMIZE
    ↓
Larger files
    ↓
Potentially better read efficiency
```

---

# 37. Data File Size Matters

Very small files can be inefficient.

Extremely large files can also create problems depending on the workload and parallelism.

The general goal is:

> **Reasonably sized data files appropriate for the workload.**

Databricks provides mechanisms such as optimized writes and auto compaction in appropriate scenarios to help manage file sizes.

---

# 38. Partitioned Delta Tables

A Delta table can be physically partitioned.

For example:

```text
customers/
│
├── year=2025/
│   ├── month=01/
│   │   ├── part-001.parquet
│   │   └── part-002.parquet
│   │
│   └── month=02/
│       ├── part-003.parquet
│       └── part-004.parquet
│
└── year=2026/
    └── month=01/
        ├── part-005.parquet
        └── part-006.parquet
```

A query such as:

```sql
SELECT *
FROM customers
WHERE year = 2026
  AND month = 1;
```

can potentially avoid unrelated partitions.

This is:

> **Partition pruning**

---

# 39. Unity Catalog and Delta

In modern Databricks, Unity Catalog provides a layer above Delta.

Conceptually:

```text
                  Unity Catalog
                       │
           ┌───────────┴───────────┐
           │                       │
       Metadata               Governance
           │                       │
           ▼                       ▼
       Delta Table          Permissions / Lineage
           │
           ▼
       Delta Lake
           │
     ┌─────┴─────┐
     ▼           ▼
 _delta_log    Parquet
     │           │
     └─────┬─────┘
           ▼
     Cloud Storage
```

### Unity Catalog

Primarily concerned with:

- Governance
- Metadata
- Permissions
- Lineage
- Discovery

### Delta Lake

Primarily concerned with:

- Transactions
- Table state
- Data versioning
- Schema enforcement
- Time Travel
- Data files

### Cloud Storage

Actually stores:

- Parquet data files
- Delta transaction log
- Checkpoint files

---

# 40. Cloud Storage Layer

Delta Lake sits on top of cloud object storage such as:

```text
ADLS Gen2
S3
GCS
```

For Azure:

```text
Databricks
     ↓
Delta Lake
     ↓
ADLS Gen2
     │
     ├── _delta_log
     │
     └── *.parquet
```

The object storage physically stores the files.

Delta provides the transactional table layer above those files.

---

# 41. Managed Delta Table

With a managed table:

```sql
CREATE TABLE employees
USING DELTA;
```

Databricks manages the table's storage location.

Conceptually:

```text
Unity Catalog
      ↓
Catalog Metadata
      ↓
Managed Delta Table
      ↓
Managed Storage
      │
      ├── _delta_log
      └── Parquet files
```

---

# 42. External Delta Table

With an external table:

```sql
CREATE TABLE employees
USING DELTA
LOCATION 'abfss://...';
```

Conceptually:

```text
Unity Catalog
      ↓
Table Metadata
      ↓
External Location
      │
      ├── _delta_log
      └── Parquet files
```

The Delta architecture itself remains the same.

The major difference is:

> **Who manages the underlying storage/location.**

---

# 43. Delta and Medallion Architecture

Delta architecture fits directly into a Medallion architecture.

```text
                    DATA SOURCES
                         │
                         ▼
                    BRONZE DELTA
                         │
                ┌────────┴────────┐
                │                 │
             Parquet          _delta_log
                │                 │
                └────────┬────────┘
                         │
                         ▼
                    SILVER DELTA
                         │
                ┌────────┴────────┐
                │                 │
             Parquet          _delta_log
                │                 │
                └────────┬────────┘
                         │
                         ▼
                     GOLD DELTA
                         │
                ┌────────┴────────┐
                │                 │
             Parquet          _delta_log
```

Each Delta table independently has its own table state and history.

---

# 44. Delta Table Is More Than "Parquet"

This is one of the most important distinctions.

A directory containing Parquet files is not automatically a Delta table.

Conceptually:

```text
Plain Parquet Dataset
        ↓
Parquet files only

Delta Table
        ↓
Parquet data files
        +
_delta_log
        +
Delta transaction/state semantics
```

The `_delta_log` is what turns the collection of files into a transactional Delta table.

---

# 45. Why UPDATE / DELETE / MERGE Work

The overall mechanism can be summarized as:

```text
UPDATE / DELETE / MERGE
          ↓
Identify affected data files
          ↓
Read affected files
          ↓
Create new Parquet files
          ↓
Mark old files removed
          ↓
Add new files
          ↓
Commit transaction
          ↓
New Delta table version
```

This is why Delta can provide transactional behavior without modifying Parquet rows in place.

---

# 46. End-to-End Example

Let's put everything together.

## Initial State

```text
Version 0

_delta_log/
└── 00000000000000000000.json

Data:
├── A.parquet
└── B.parquet
```

Current table:

```text
A + B
```

---

## INSERT

```sql
INSERT INTO employees
VALUES (101, 'John', 'IT', 70000);
```

Delta creates new data:

```text
C.parquet
```

Transaction log:

```text
Version 1
ADD C.parquet
```

Current table:

```text
A + B + C
```

---

## UPDATE

```sql
UPDATE employees
SET salary = 80000
WHERE employee_id = 101;
```

Conceptually:

```text
C.parquet
     ↓
Read
     ↓
Create D.parquet
     ↓
REMOVE C
ADD D
```

Current table:

```text
A + B + D
```

`C.parquet` may still physically exist.

---

## DELETE

```sql
DELETE FROM employees
WHERE employee_id = 101;
```

Conceptually, the affected data is removed from the logical table state through new transaction-log actions and replacement data files where required.

The resulting table state no longer contains that employee.

---

## Time Travel

```sql
SELECT *
FROM employees
VERSION AS OF 1;
```

Delta reconstructs the table as it existed at Version 1.

Conceptually:

```text
Version 1
    ↓
A + B + C
```

---

## VACUUM

Later:

```sql
VACUUM employees;
```

Eligible obsolete physical files can be removed according to the applicable retention rules.

---

# 47. Complete Read Architecture

The complete conceptual read path is:

```text
                         SELECT
                           │
                           ▼
                    Delta Table Reader
                           │
                           ▼
                      _delta_log
                           │
                ┌──────────┴──────────┐
                │                     │
             Checkpoint          Recent JSON
                │                     │
                └──────────┬──────────┘
                           ▼
                    Table State
                           │
                           ▼
                  Active Parquet Files
                           │
             ┌─────────────┼─────────────┐
             ▼             ▼             ▼
          File A         File B        File D
             │             │             │
             └─────────────┼─────────────┘
                           ▼
                  Partition / File Pruning
                           │
                           ▼
                    Column Pruning
                           │
                           ▼
                 Data / Predicate Skipping
                           │
                           ▼
                       Query Result
```

---

# 48. Complete Write Architecture

```text
                 DataFrame / SQL
                        │
                        ▼
                  Spark Engine
                        │
                        ▼
                Identify / transform
                   affected data
                        │
                        ▼
                 Write Parquet
                        │
                        ▼
                Transaction Commit
                        │
                        ▼
                   _delta_log
                        │
                        ▼
                 New Table Version
```

For updates/deletes:

```text
Existing Parquet
       ↓
Read affected data
       ↓
Create replacement Parquet
       ↓
REMOVE old file
ADD new file
       ↓
Commit
       ↓
New table version
```

---

# 49. The Complete Delta Architecture

```text
                              DATABRICKS
                                  │
                                  ▼
                            UNITY CATALOG
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
                 Metadata                   Governance
                    │                           │
                    └─────────────┬─────────────┘
                                  ▼
                             DELTA TABLE
                                  │
                 ┌────────────────┴────────────────┐
                 │                                 │
                 ▼                                 ▼
          TRANSACTION LOG                     DATA FILES
             _delta_log                         Parquet
                 │                                 │
          ┌──────┴──────┐                          │
          │             │                          │
        JSON        Checkpoint                     │
          │             │                          │
          │       Accumulated state                │
          │             │                          │
          └──────┬──────┘                          │
                 │                                 │
                 └──────────────┬──────────────────┘
                                ▼
                         CURRENT TABLE STATE
                                │
                                ▼
                       ACTIVE PARQUET FILES
                                │
                                ▼
                          ACTUAL DATA
                                │
                                ▼
                          QUERY RESULT
```

---

# 50. The Most Important Relationship

Remember this:

```text
JSON Transaction Logs
        │
   "What changed?"
        │
        ▼
Checkpoint
   "Accumulated state"
        │
        ▼
Table State
   "Which files are active?"
        │
        ▼
Parquet Data Files
     "Actual data"
```

Or even more simply:

> **JSON = changes**  
> **Checkpoint = accumulated state**  
> **Parquet = actual data**  
> **Delta = the layer that ties them together**

---

# 51. One More Important Relationship

```text
Physical Storage
      ↓
Parquet files + _delta_log
      ↓
Transaction Log
      ↓
Current logical table state
      ↓
Active Parquet files
      ↓
Actual query data
```

The physical directory can contain files that are no longer part of the current logical table.

The transaction log determines the logical state.

---

# 52. Delta vs Traditional Database

A traditional database often works conceptually like:

```text
UPDATE row
    ↓
Modify existing database pages
```

Delta generally follows:

```text
UPDATE
   ↓
Read affected files
   ↓
Create new Parquet files
   ↓
Mark old files removed
   ↓
Commit transaction
   ↓
New table version
```

This immutable-file + transaction-log design explains a large portion of Delta Lake behavior.

---

# 53. The Five Things You Should Be Able to Explain in an Interview

Make sure you can clearly answer:

### 1. What is stored in Parquet vs `_delta_log`?

```text
Parquet
    ↓
Actual data

_delta_log
    ↓
Transaction/state information
```

### 2. How does Delta implement UPDATE/DELETE when Parquet is immutable?

```text
New Parquet files
      +
REMOVE old files
      +
ADD new files
      ↓
New table version
```

### 3. How does the transaction log help with ACID and concurrency?

```text
Transactions
    ↓
Committed versions
    ↓
Consistent table state
    ↓
Concurrency control
```

### 4. How do Time Travel and versions work?

```text
Table Version N
      ↓
Transaction log + required data files
      ↓
Historical table state
```

### 5. What is the difference between DELETE, REMOVE, and VACUUM?

```text
DELETE
  ↓
User-level data operation

REMOVE
  ↓
Marks file as no longer part of logical table state

VACUUM
  ↓
Physically removes eligible obsolete files
```

---

# 54. Final Mental Model

If you remember only one architecture, remember this:

```text
                         DELTA TABLE
                              │
             ┌────────────────┴────────────────┐
             │                                 │
             ▼                                 ▼
        TRANSACTION LOG                    DATA FILES
           _delta_log                        Parquet
             │                                 │
       ┌─────┴─────┐                           │
       │           │                           │
      JSON     Checkpoint                      │
       │           │                           │
   What changed? Accumulated state             │
       │           │                           │
       └─────┬─────┘                           │
             ▼                                 │
        TABLE STATE                             │
             │                                 │
             └──────────────┬──────────────────┘
                            ▼
                    ACTIVE PARQUET FILES
                            ▼
                       ACTUAL DATA
```

And the shortest possible summary:

> **Delta Lake is a transactional table layer built on Parquet files and a transaction log. Parquet stores the actual data, JSON logs record changes, checkpoints provide a compacted representation of accumulated table state, and the transaction log determines which Parquet files constitute the current logical table.**

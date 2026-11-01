# Databricks Delta Lake — `.parquet` Data Files

## 1. What is a `.parquet` Data File?

A `.parquet` file is where the **actual data/rows of a Delta table are stored**.

For example:

```text
customer_id | name  | city
------------|-------|-------
1           | John  | Delhi
2           | Alex  | Mumbai
3           | Sara  | Pune
```

The actual values above are stored inside Parquet data files.

A Delta table therefore has two major parts:

```text
Delta Table
│
├── .parquet files
│      ↓
│   Actual business data
│
└── _delta_log
       ↓
    Transaction/state information
```

The key distinction is:

> **Parquet stores the data. Delta Log stores information about the table and its changes.**

---

# 2. Where Do Parquet Files Exist?

Suppose your Delta table is stored at:

```text
abfss://container@storage.dfs.core.windows.net/sales/
```

Its structure might look conceptually like:

```text
sales/
│
├── part-00000-abc.parquet
├── part-00001-def.parquet
├── part-00002-xyz.parquet
├── part-00003-pqr.parquet
│
└── _delta_log/
    ├── 00000000000000000000.json
    ├── 00000000000000000001.json
    ├── 00000000000000000002.json
    └── 00000000000000000010.checkpoint.parquet
```

Notice:

```text
Parquet files
    ↓
Actual table data

_delta_log
    ↓
Transaction / state information
```

---

# 3. Why Does Delta Use Parquet?

Parquet is an efficient format for analytical workloads.

It provides features such as:

- Columnar storage
- Compression
- Encoding
- Efficient column reads
- Predicate pushdown
- Column pruning
- Splittable files
- Efficient analytical scans

This makes it very suitable for large-scale data engineering workloads.

---

# 4. Parquet Is a Columnar Format

This is one of the most important concepts.

Imagine this data:

```text
ID    Name    City
1     John    Delhi
2     Alex    Mumbai
3     Sara    Pune
```

A row-oriented representation is conceptually:

```text
Row 1 → 1, John, Delhi
Row 2 → 2, Alex, Mumbai
Row 3 → 3, Sara, Pune
```

Parquet is **column-oriented**.

Conceptually:

```text
ID
 ↓
1
2
3

Name
 ↓
John
Alex
Sara

City
 ↓
Delhi
Mumbai
Pune
```

This is extremely useful for analytical queries.

---

# 5. Why Is Columnar Storage Useful?

Suppose the table contains:

```text
customer_id
name
city
age
email
phone
salary
department
address
...
```

And the query is:

```sql
SELECT name, salary
FROM customers;
```

The engine does not necessarily need every column.

With a columnar format, it can focus on:

```text
name
salary
```

instead of reading all columns.

This is called:

> **Column pruning**

---

# 6. Column Pruning

Suppose a Parquet file contains:

```text
customer_id
name
city
age
salary
department
email
phone
```

Query:

```sql
SELECT name, salary
FROM customers;
```

Spark can effectively read:

```text
name
salary
```

rather than:

```text
customer_id
name
city
age
salary
department
email
phone
```

This reduces the amount of data that needs to be read from storage.

---

# 7. Predicate Pushdown

Another important feature is **predicate pushdown**.

Suppose:

```sql
SELECT *
FROM customers
WHERE age > 30;
```

Parquet stores statistics for data blocks/row groups, such as:

```text
min(age) = 20
max(age) = 25
```

If Spark knows:

```text
WHERE age > 30
```

it can determine that this particular section cannot contain matching rows.

So it can skip reading that section.

Conceptually:

```text
Parquet File
│
├── Row Group 1
│   age: 20 - 25
│       ↓
│   Skip ❌
│
├── Row Group 2
│   age: 31 - 45
│       ↓
│   Read ✅
│
└── Row Group 3
    age: 50 - 70
        ↓
    Read ✅
```

This is one reason Parquet is efficient for analytical queries.

---

# 8. Compression

Parquet supports compression.

For example:

```text
Uncompressed data
        ↓
      1 GB
        ↓
Compression
        ↓
    200 MB
```

The exact compression ratio depends heavily on the data.

Common compression codecs include:

- Snappy
- Zstandard
- GZIP
- LZ4

In many Spark/Databricks workloads, **Snappy** has traditionally been a common choice because it provides a good balance between compression and speed.

---

# 9. Encoding

Parquet doesn't simply store raw values.

It can use encoding techniques to make data smaller and faster to process.

For example:

```text
Delhi
Delhi
Delhi
Mumbai
Mumbai
Pune
Pune
Pune
Pune
```

There is a lot of repetition.

Parquet can use encoding techniques that take advantage of this repetition.

So there are two related concepts:

```text
Encoding
    ↓
Represent values efficiently

Compression
    ↓
Compress the encoded data
```

You don't need to memorize every encoding technique initially.

---

# 10. Parquet File Structure

At a high level, think of a Parquet file like this:

```text
Parquet File
│
├── Row Group 1
│     ├── Column Chunk: customer_id
│     ├── Column Chunk: name
│     └── Column Chunk: city
│
├── Row Group 2
│     ├── Column Chunk: customer_id
│     ├── Column Chunk: name
│     └── Column Chunk: city
│
└── File Metadata
```

The important hierarchy is:

```text
Parquet File
      ↓
  Row Groups
      ↓
Column Chunks
      ↓
   Pages
```

For Databricks interviews, understanding:

> **File → Row Group → Column Chunk**

is generally more useful initially than memorizing the lower-level page details.

---

# 11. Row Groups

A Parquet file can be divided into **row groups**.

For example:

```text
customer.parquet

┌─────────────────────┐
│ Row Group 1         │
│ rows 1 - 100,000   │
├─────────────────────┤
│ Row Group 2         │
│ rows 100k - 200k   │
├─────────────────────┤
│ Row Group 3         │
│ rows 200k - 300k   │
└─────────────────────┘
```

Each row group contains data for all columns for a subset of rows.

This allows engines to skip irrelevant row groups when possible.

---

# 12. Column Chunks

Within a row group, each column has its own chunk.

Conceptually:

```text
Row Group 1
│
├── customer_id column chunk
├── name column chunk
├── city column chunk
└── salary column chunk
```

This is part of what enables efficient columnar access.

---

# 13. Parquet Files Are Generally Treated as Immutable in Delta

This is very important for understanding Delta `UPDATE`, `DELETE`, and `MERGE`.

When you update a row, Delta generally does not simply open an existing Parquet file and modify that row in place.

Instead, Delta follows an immutable-file approach.

Suppose:

```text
file-A.parquet
```

contains:

```text
ID | Name
---|-----
1  | John
2  | Alex
3  | Sara
```

Now:

```sql
UPDATE customers
SET Name = 'Johnny'
WHERE ID = 1;
```

Conceptually:

```text
Old file
file-A.parquet
     ↓
REMOVE
     ↓
New file
file-B.parquet
     ↓
ADD
```

The new file contains:

```text
ID | Name
---|-------
1  | Johnny
2  | Alex
3  | Sara
```

The old physical file may still exist temporarily.

---

# 14. How This Connects to the Transaction Log

Suppose:

```text
Before UPDATE:

file-A.parquet
```

Then an UPDATE happens.

The transaction log can conceptually record:

```text
REMOVE file-A.parquet
ADD file-B.parquet
```

So:

```text
Delta Log
    ↓
REMOVE old file
ADD new file
    ↓
Current table state
    ↓
file-B.parquet
    ↓
Actual data
```

This leads to an important mental model:

> **The transaction log tells Delta which Parquet files belong to the current table state.**

---

# 15. Parquet Files and DELETE

Suppose:

```text
file-A.parquet

ID | Name
---|-----
1  | John
2  | Alex
3  | Sara
```

You execute:

```sql
DELETE FROM customers
WHERE ID = 2;
```

Conceptually:

```text
Old:
file-A.parquet

        ↓

REMOVE file-A.parquet
ADD file-B.parquet

        ↓

file-B.parquet

ID | Name
---|------
1  | John
3  | Sara
```

Again, the old file may remain physically until cleanup.

---

# 16. Logical Delete vs Physical Delete

This is an extremely important Delta concept.

Suppose:

```text
DELETE ID = 2
```

Delta first changes the **logical table state**:

```text
Transaction Log
     ↓
file-A removed from active state
file-B added to active state
```

But physically:

```text
Storage
│
├── file-A.parquet   ← may still exist
└── file-B.parquet   ← current
```

Later, `VACUUM` can physically remove obsolete files after the applicable retention period.

Therefore:

```text
DELETE
   ↓
Logical removal

VACUUM
   ↓
Physical cleanup
```

---

# 17. Why Can Old Parquet Files Remain?

Old Parquet files can remain because Delta may need them for:

- Time travel
- Recovery
- Transaction safety
- Historical versions

For example:

```text
Version 10
   ↓
file-A.parquet

Version 11
   ↓
file-B.parquet
```

If `file-A.parquet` is still retained, Delta can potentially reconstruct Version 10.

Once the relevant old files are physically removed, that historical state may no longer be queryable.

---

# 18. Parquet Data Files vs Checkpoint Parquet

This is a common source of confusion.

Both are `.parquet` files, but they serve **different purposes**.

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

So:

> **Not every Parquet file in a Delta table is a business-data file.**

---

# 19. The Small Files Problem

One of the most important practical Databricks topics is the **small-file problem**.

Suppose your pipeline continuously writes data:

```text
part-001.parquet
part-002.parquet
part-003.parquet
...
part-100000.parquet
```

If these files are very small:

```text
10 KB
15 KB
8 KB
20 KB
...
```

you can end up with a large number of small files.

Why is this bad?

Because reading thousands or millions of small files creates significant overhead.

Conceptually:

```text
1 large file
    ↓
Few file operations

vs.

1,000,000 small files
    ↓
Huge metadata + file-opening overhead
```

---

# 20. OPTIMIZE and Parquet Files

Databricks provides `OPTIMIZE` to compact small files.

For example:

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

LARGE-1.parquet
LARGE-2.parquet
```

The transaction log then records the appropriate file additions/removals.

So:

```text
Small Parquet files
        ↓
OPTIMIZE
        ↓
Larger Parquet files
        ↓
Better read efficiency
```

---

# 21. Data File Size Matters

Very tiny files are inefficient.

Very large files can also create problems depending on workload and parallelism.

The goal is generally to have **reasonably sized files**, rather than:

```text
Millions of tiny files
```

or:

```text
A few extremely huge files
```

Databricks provides mechanisms such as optimized writes and auto compaction in appropriate workloads to help manage file sizes.

---

# 22. Partitioning and Parquet Files

Suppose you partition a Delta table by:

```text
year
month
```

Your storage could look like:

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
    ├── month=01/
    │   ├── part-005.parquet
    │   └── part-006.parquet
```

Then a query such as:

```sql
SELECT *
FROM customers
WHERE year = 2026
  AND month = 1;
```

can potentially avoid scanning unrelated partition directories.

This is called:

> **Partition pruning**

---

# 23. Partition Pruning vs Column Pruning vs Predicate Pushdown

These are different concepts.

### Partition pruning

Avoids unnecessary **partitions/files**.

```text
year=2025  ❌
year=2026  ✅
```

### Column pruning

Avoids unnecessary **columns within files**.

```text
customer_id  ❌
name         ✅
city         ❌
salary       ✅
```

### Predicate pushdown

Can skip irrelevant **row groups/data sections** based on filter conditions and available statistics.

Conceptually:

```text
Partition pruning
       ↓
Which partitions/files?

Column pruning
       ↓
Which columns?

Predicate pushdown
       ↓
Which data sections/row groups?
```

These are important concepts for understanding why Parquet performs well.

---

# 24. How Spark Reads a Delta Table

Suppose:

```text
Delta Table
│
├── data files
│   ├── A.parquet
│   ├── B.parquet
│   └── C.parquet
│
└── _delta_log
    ├── JSON logs
    └── checkpoint
```

When Spark reads the Delta table, conceptually:

```text
1. Read Delta transaction/state information
             ↓
2. Determine current table state
             ↓
3. Identify active Parquet files
             ↓
4. Apply partition pruning
             ↓
5. Apply file/data skipping where possible
             ↓
6. Apply column pruning
             ↓
7. Read required Parquet data
             ↓
8. Process query
```

This connects the transaction log, checkpoints, and Parquet data files.

---

# 25. Complete Delta Architecture Mental Model

```text
                         DELTA TABLE
                              │
              ┌───────────────┴────────────────┐
              │                                │
         DATA FILES                         _delta_log
              │                                │
          Parquet                         ┌────┴─────┐
              │                           │          │
      Actual business data           JSON Logs   Checkpoint
                                      │             │
                                What changed?   Accumulated
                                                state
                                      │             │
                                      └──────┬──────┘
                                             ↓
                                      Table State
                                             ↓
                                  Active Parquet Files
                                             ↓
                                      Query Data
```

---

# 26. The Most Important Relationship

This is the key mental model connecting the three concepts:

```text
              JSON Transaction Logs
                       │
                  "What changed?"
                       │
                       ↓
                 Checkpoint
              "Accumulated state"
                       │
                       ↓
                Table State
                       │
               "Which files are
                 active?"
                       │
                       ↓
              Parquet Data Files
                "Actual data"
```

In other words:

> **The transaction log determines which Parquet files represent the current table state. The Parquet files contain the actual data.**

---

# 27. Interview Definition

If an interviewer asks:

### "What are Parquet files in a Delta table?"

A good answer is:

> **"Parquet files are the physical data files that store the actual rows of a Delta table. Delta uses Parquet because it is a columnar, compressed format that supports efficient analytical reads through techniques such as column pruning, predicate pushdown, and data skipping. The Delta transaction log tracks which Parquet files are part of the current table state."**

---

# 28. Key Points to Remember

1. **Parquet files store the actual business data.**
2. **Parquet is a columnar storage format.**
3. **Parquet supports compression and encoding.**
4. **Column pruning reduces unnecessary column reads.**
5. **Predicate pushdown can eliminate unnecessary data reads.**
6. **Partition pruning can eliminate unnecessary partitions.**
7. **Delta generally treats Parquet data files as immutable.**
8. **UPDATE/DELETE/MERGE can result in new Parquet files and removal of old files from the logical table state.**
9. **Old Parquet files may remain physically until cleanup.**
10. **VACUUM removes eligible obsolete physical files.**
11. **OPTIMIZE can compact small Parquet files.**
12. **Checkpoint Parquet and data Parquet serve different purposes.**
13. **The Delta transaction log determines which Parquet files are active.**

---

# 29. Simplest Mental Model

> **JSON = changes**  
> **Checkpoint = accumulated state**  
> **Parquet = actual data**  
> **Delta = the layer that ties them together**

```text
JSON Logs
"What changed?"
      ↓
Individual ADD / REMOVE / metadata actions
      ↓
Accumulate + resolve actions
      ↓
Checkpoint
"What is the accumulated state?"
      ↓
Recent JSON Logs
"What changed after the checkpoint?"
      ↓
Current / Target Table State
      ↓
Active Parquet Data Files
      ↓
Actual Data
```

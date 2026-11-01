# Databricks Delta Lake — Checkpoint Files

## 1. What is a Checkpoint?

A **checkpoint** is a persisted snapshot of the Delta table's state at a particular version.

In simple terms:

> **A checkpoint accumulates and compacts the actions from the Delta JSON logs, storing the resulting state needed to represent a particular table version.**

---

## 2. Why Do We Need Checkpoints?

Over time, a Delta table can accumulate many JSON transaction-log files:

```text
_delta_log/

00000000000000000000.json
00000000000000000001.json
00000000000000000002.json
00000000000000000003.json
...
00000000000000000100.json
```

Each JSON file contains the actions/changes committed at that version.

If Delta had to replay a very large number of JSON files every time it needed to reconstruct the table state, it would become increasingly expensive because it would involve:

- Reading many files
- Performing many I/O operations
- Parsing JSON
- Processing many individual actions
- Reconstructing the accumulated table state

Checkpoints solve this problem by providing a persisted representation of the accumulated state.

---

## 3. Simple Mental Model

```text
JSON Logs
   ↓
ADD / REMOVE / UPDATE actions
   ↓
Accumulate + resolve actions
   ↓
Checkpoint
   ↓
Resulting state
   ↓
Table state at Version N
```

The key idea is:

> **JSON logs record changes; checkpoints provide the accumulated state resulting from those changes.**

---

## 4. What Does "Persisted" Mean?

**Persisted** simply means:

> **Saved to durable storage so that it can be retrieved and used later.**

For example:

```text
Table state @ Version 100
        ↓
Checkpoint created
        ↓
Written to storage
        ↓
100.checkpoint.parquet
```

The checkpoint is persisted because it has been physically written to storage.

### Persisted vs In-Memory

| Term | Meaning |
|---|---|
| Persisted | Saved to durable storage |
| In memory | Temporarily held in RAM |
| Temporary | Exists only for a limited operation/session |

For Delta, a checkpoint is persisted under the `_delta_log` directory.

---

# 5. Where Is a Checkpoint Stored?

Checkpoints are stored inside the Delta table's `_delta_log` directory.

Example:

```text
my_delta_table/
│
├── part-00000-....parquet
├── part-00001-....parquet
├── part-00002-....parquet
│
└── _delta_log/
    │
    ├── 00000000000000000000.json
    ├── 00000000000000000001.json
    ├── 00000000000000000002.json
    │
    ├── 00000000000000000010.checkpoint.parquet
    │
    ├── 00000000000000000011.json
    └── 00000000000000000012.json
```

The actual business data is stored in the Parquet data files.

The checkpoint is part of the transaction-log/state-management layer.

---

# 6. What Does a Checkpoint Contain?

A checkpoint contains the **accumulated state resulting from Delta log actions up to a particular version**.

The important idea is that it is **not simply a copy of all the JSON files**.

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

The accumulated state at Version 2 is:

```text
A
C
D
```

Conceptually:

```text
Version 0
ADD A
ADD B
    ↓
Version 1
ADD C
    ↓
Version 2
REMOVE B
ADD D
    ↓
Checkpoint @ Version 2
    ↓
A, C, D
```

So:

> **JSON logs contain the individual changes, while the checkpoint represents the resulting accumulated state.**

---

# 7. Checkpoint Is Not Simply "Compacting JSON Files"

It is tempting to say:

> "All the JSON files are compacted into a Parquet file."

This is useful as a basic mental model, but technically it is not quite accurate.

A better statement is:

> **A checkpoint is a compacted representation of the accumulated state resulting from the Delta log actions up to a particular version.**

The JSON files themselves are not simply merged or converted into one Parquet file.

Instead:

```text
Delta JSON Logs
      ↓
Individual actions
      ↓
Accumulate / resolve actions
      ↓
Resulting table state
      ↓
Checkpoint
(compacted representation)
```

---

# 8. Why Parquet?

Checkpoints are stored using Parquet because Parquet is an efficient columnar storage format.

This provides benefits such as:

- Columnar storage
- Compression
- Efficient reading
- Efficient processing of structured state
- Better scalability for large metadata/state information

The important point is not simply:

> "Parquet is faster than JSON."

The bigger optimization is:

> **The checkpoint avoids replaying a long history of transaction-log changes.**

---

# 9. How Checkpoints Improve Reading Performance

Imagine a Delta table has 1,000,000 versions.

Without checkpoints, conceptually:

```text
Version 0
   ↓
Version 1
   ↓
Version 2
   ↓
...
   ↓
Version 999,999
   ↓
Current table state
```

That would require processing a huge amount of transaction history.

With a checkpoint:

```text
Checkpoint @ Version 999,000
          ↓
Already accumulated state
          +
Version 999,001 JSON
Version 999,002 JSON
...
Version 999,999 JSON
          ↓
Current table state
```

Instead of replaying the entire history, Delta can start from the checkpoint and process only the newer log files.

Therefore:

> **Checkpoints reduce the amount of transaction-log history that Delta needs to replay, improving table-state reconstruction performance and reducing metadata-related I/O.**

---

# 10. How Delta Reconstructs the Current Table State

Conceptually, when Delta needs to determine the current table state:

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
Current table state
      ↓
Identify active Parquet files
      ↓
Read the actual data
```

For example:

```text
Checkpoint @ Version 100
        ↓
State at Version 100
        +
101.json
102.json
103.json
104.json
105.json
        ↓
State at Version 105
        ↓
Active Parquet files
        ↓
Actual table data
```

---

# 11. ADD and REMOVE Actions

A useful way to understand Delta's table state is through `ADD` and `REMOVE` actions.

Suppose:

```text
Version 0
ADD A
ADD B
```

Current active files:

```text
A
B
```

Then:

```text
Version 1
ADD C
```

Current active files:

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

Current active files:

```text
A
C
D
```

A checkpoint at Version 2 can represent this accumulated state.

```text
JSON Actions
    ↓
ADD A
ADD B
ADD C
REMOVE B
ADD D
    ↓
Accumulated State
    ↓
A, C, D
```

This is why the checkpoint is best understood as the **resulting state**, rather than merely a copy of the historical actions.

---

# 12. Checkpoint + Recent JSON Logs

A checkpoint does not mean that Delta stops using JSON logs.

Suppose:

```text
Checkpoint @ Version 100
```

Then new transactions occur:

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

> **Checkpoint provides the starting state, and the JSON files after the checkpoint provide the changes after that state.**

---

# 13. Multiple Checkpoints

A Delta table can have multiple checkpoints over its lifetime.

For example:

```text
JSON files
    ↓
Checkpoint @ 10
    ↓
More JSON files
    ↓
Checkpoint @ 20
    ↓
More JSON files
    ↓
Checkpoint @ 30
    ↓
More JSON files
```

This allows Delta to choose a suitable checkpoint when reconstructing a particular table version.

---

# 14. Checkpoint Does Not Mean Old JSON Logs Are Immediately Deleted

Creating a checkpoint does **not** mean that all previous JSON transaction-log files are immediately deleted.

Conceptually:

```text
Old JSON logs
       +
Checkpoint
       +
Recent JSON logs
```

The transaction log has its own retention/cleanup mechanisms.

Therefore:

> **Checkpointing and log cleanup are separate concepts.**

---

# 15. Checkpoint vs VACUUM

These are very different.

### Checkpoint

Purpose:

> **Improve transaction-log/state reconstruction performance.**

```text
JSON Logs
   ↓
Checkpoint
   ↓
Faster state reconstruction
```

### VACUUM

Purpose:

> **Remove obsolete physical data files from storage after the applicable retention period.**

```text
Old/obsolete Parquet files
          ↓
        VACUUM
          ↓
Physical cleanup
```

Therefore:

```text
Checkpoint ≠ VACUUM
```

A checkpoint does not physically delete obsolete business-data files.

---

# 16. Checkpoint vs Time Travel

A checkpoint is not Time Travel.

### Checkpoint

Helps Delta efficiently reconstruct table state.

### Time Travel

Allows you to query a previous table version.

Example:

```sql
SELECT *
FROM my_table
VERSION AS OF 10;
```

Conceptually, Delta can use transaction-log information and checkpoints to reconstruct the requested version.

Therefore:

```text
Checkpoint
    ↓
Performance / state reconstruction

Time Travel
    ↓
Access a historical table version
```

---

# 17. Checkpoint Is Not a Backup

A checkpoint should not be considered a backup of the Delta table.

The actual business data is still stored in Parquet data files.

```text
Delta Table
│
├── Data files
│     └── Parquet
│
└── _delta_log
      ├── JSON transaction logs
      └── Checkpoints
```

The checkpoint primarily represents table-state/transaction-log information.

It is not a complete copy of the business data.

---

# 18. Important Distinction: Checkpoint vs Data Files

This distinction is extremely important.

### Parquet Data Files

Contain:

> **Actual business data**

Example:

```text
customer_id | name | city
------------|------|------
1           | John | Delhi
2           | Alex | Mumbai
```

### Checkpoint

Contains:

> **State/metadata information needed to understand the Delta table state**

For example, conceptually, information about which data files are currently active.

So:

```text
Data Parquet
    ↓
Actual rows

Checkpoint
    ↓
Table-state information
```

---

# 19. Complete Delta Architecture Mental Model

```text
                    Delta Table
                         │
            ┌────────────┴────────────┐
            │                         │
      Data Files                  _delta_log
            │                         │
        Parquet              ┌────────┴─────────┐
            │                │                  │
     Actual Data        JSON Logs         Checkpoints
                             │                  │
                       What changed?      Accumulated state
                             │                  │
                             └────────┬─────────┘
                                      ↓
                               Table State
                                      ↓
                              Active Data Files
                                      ↓
                                Read Data
```

---

# 20. The Most Important Mental Model

Remember this:

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

---

# 21. Recommended Definition for Notes

> **A checkpoint is a compacted, persisted representation of the accumulated Delta table state resulting from transaction-log actions up to a particular version. It allows Delta to reconstruct table state without replaying the entire transaction-log history.**

---

# 22. Interview-Friendly Explanation

If an interviewer asks:

### "Why does Delta Lake use checkpoint files?"

A strong answer is:

> **"Delta tables can accumulate a large number of JSON transaction-log files. Replaying all those logs to reconstruct the table state would become increasingly expensive. Delta periodically creates checkpoints that persist a compacted representation of the accumulated table state at a particular version. When reading the table, Delta can start from the latest suitable checkpoint and process only the JSON logs after that checkpoint, reducing metadata processing and I/O."**

---

# 23. Seven Things to Remember

1. **Checkpoint ≠ backup**
2. **Checkpoint ≠ actual business data**
3. **Checkpoint is primarily for efficient state reconstruction**
4. **Checkpoint represents accumulated state at a particular version**
5. **JSON logs record incremental changes**
6. **Checkpoint provides a compacted representation of accumulated state**
7. **Checkpoint ≠ VACUUM**

---

# 24. One-Line Summary

> **JSON logs tell Delta what changed; checkpoints give Delta a persisted, compacted representation of the resulting table state so it doesn't have to replay the entire history.**

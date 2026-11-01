# Delta Transaction JSON File 

## 1. What is a Delta JSON commit file?

A file such as:

```text
_delta_log/
└── 00000000000000000005.json
```

represents **Delta table Version 5 / Commit 5**.

It is typically **newline-delimited JSON (NDJSON)**, where each line is a separate action:

```text
{"protocol": {...}}
{"metaData": {...}}
{"add": {...}}
{"add": {...}}
{"commitInfo": {...}}
```

## 2. Main action types

```text
Delta JSON Commit
│
├── protocol
├── metaData
├── add
├── remove
├── commitInfo
└── txn
```

| Action | Purpose |
|---|---|
| `protocol` | Delta reader/writer protocol requirements |
| `metaData` | Table schema, partition columns, properties, etc. |
| `add` | Adds a Parquet file to the logical table |
| `remove` | Removes a Parquet file from the logical table |
| `commitInfo` | Information about the commit/operation |
| `txn` | Application/transaction information |

## 3. `add` — the key relationship

Example:

```json
{"add":{
  "path":"part-00000-abc.snappy.parquet",
  "partitionValues":{},
  "size":12456,
  "modificationTime":1761000000000,
  "dataChange":true
}}
```

The key relationship is:

```text
JSON commit
    │
    └── add.path
          ↓
    Parquet data file
          ↓
      Actual rows
```

Important `add` fields:

- `path` → physical Parquet file path
- `partitionValues` → partition information
- `size` → file size
- `modificationTime` → file modification time
- `dataChange` → whether it represents a logical data change
- `stats` → file-level statistics when present

## 4. `remove`

Example:

```json
{"remove":{
  "path":"part-00001-old.snappy.parquet",
  "deletionTimestamp":1761001000000,
  "dataChange":true
}}
```

This means the file is **no longer part of the current logical table**.

It does **not necessarily mean the physical Parquet file is immediately deleted**.

```text
REMOVE B.parquet
      ↓
No longer logically active
      ↓
May still physically exist
      ↓
Eventually VACUUM may delete it
```

## 5. `add` + `remove` and table changes

An UPDATE/DELETE/MERGE can conceptually result in:

```text
REMOVE B.parquet
ADD D.parquet
```

So:

```text
Version N
    A B C

Version N+1
    A C D
```

The old Parquet file is not modified in place; the transaction log changes which physical files belong to the logical table.

## 6. `metaData`

`metaData` describes the table itself:

```text
metaData
│
├── table ID
├── format
├── schema
├── partition columns
├── table properties
└── creation information
```

Important fields include:

- `id`
- `format`
- `schemaString`
- `partitionColumns`
- `configuration`
- `createdTime`

**`add` answers:** Which data file is part of the table?

**`metaData` answers:** What is the table's structure/configuration?

## 7. `protocol`

Example:

```json
{"protocol":{
  "minReaderVersion":1,
  "minWriterVersion":2
}}
```

Defines the Delta protocol requirements and therefore reader/writer compatibility.

## 8. `commitInfo`

Contains information about the commit, such as:

```text
commitInfo
│
├── timestamp
├── operation
├── operationParameters
├── readVersion
├── isolationLevel
└── other commit information
```

This information is used in commit/history inspection such as:

```sql
DESCRIBE HISTORY customers;
```

## 9. `txn`

May contain application-level transaction information:

```json
{"txn":{
  "appId":"my-streaming-job",
  "version":123
}}
```

This is particularly relevant to transaction tracking/idempotency and streaming workloads.

# 10. Typical JSON file — tree view

```text
00000000000000000005.json
│
├── protocol
│   ├── minReaderVersion
│   └── minWriterVersion
│
├── metaData
│   ├── id
│   ├── format
│   │   └── provider
│   ├── schemaString
│   ├── partitionColumns
│   ├── configuration
│   └── createdTime
│
├── add
│   ├── path
│   ├── partitionValues
│   ├── size
│   ├── modificationTime
│   ├── dataChange
│   └── stats
│
├── add
│   └── ...
│
├── remove
│   ├── path
│   ├── deletionTimestamp
│   ├── dataChange
│   └── ...
│
├── commitInfo
│   ├── timestamp
│   ├── operation
│   ├── operationParameters
│   ├── readVersion
│   └── isolationLevel
│
└── txn
    ├── appId
    └── version
```

> **Note:** This is a logical tree representation. The actual file is typically newline-delimited JSON, not one nested JSON document.

# 11. How the JSON fits into Delta architecture

```text
                     DELTA TABLE
                          │
             ┌────────────┴─────────────┐
             │                          │
             ↓                          ↓
       _delta_log/                  Data files
             │                          │
       ┌─────┴──────┐                   ├── A.parquet
       │            │                   ├── B.parquet
       ↓            ↓                   └── C.parquet
     JSON       Checkpoint
     files        files
       │
       │ add/remove actions
       ↓
   Active file set
       │
       ↓
 Logical table state
       │
       ↓
   Spark reads
```

### The three things to remember

**JSON transaction log**

> **What changed?**

`ADD`, `REMOVE`, metadata, protocol, commit information, etc.

**Checkpoint**

> **What was the transaction-log/table state at this point?**

A compact snapshot of transaction-log state.

**Parquet data files**

> **What is the actual data?**

The physical table rows.

## 12. Core mental model

> **Delta stores physical data in Parquet files and maintains a transaction log that describes which files are added or removed and what the table's metadata/protocol state is. By processing the transaction log, Delta derives the logical set of active Parquet files for a particular table version.**

**Key distinction:**

```text
Physical Parquet files ≠ Logical table state

Parquet
  → actual data

Transaction log
  → describes table state/history

Delta
  → uses the transaction log to determine which
    Parquet files form the logical table
```

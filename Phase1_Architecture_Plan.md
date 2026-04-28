# DAS 839 – NoSQL Systems · End-Semester Project
## Phase 1: Proposed Architecture & Design Plan

> **Multi-Pipeline ETL and Reporting Framework for Web Server Log Analytics**

---

5. **Identical logical ETL** – all pipelines implement the same parse, transform, batch, and aggregate stages
6. **Identical runtime boundary** – runtime starts when input reading begins and ends after PostgreSQL writes complete

| Item | Detail |
|---|---|
| Dataset | NASA Kennedy Space Center HTTP logs – Jul 1995 (1,891,714 records) + Aug 1995 (1,569,898 records) = **~3.46 million records** |
Each line in the raw log follows an **NCSA-style common web server log format** with CLF-like core fields:
| Reporting DB | PostgreSQL (or MySQL) |
| Input format | Raw `.gz` compressed CLF (Common Log Format) ASCII files |

---
| `bytes_transferred` | Last field | Integer; `-` → 0 |

## 2. Log Record Format

Each line in the raw log follows **NCSA Combined Log Format**:
| Group | Field |
```
host - - [DD/Mon/YYYY:HH:MM:SS -TZOFF] "METHOD /path HTTP/version" status_code bytes
```

**Example:**
```
199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
```

### 2.1 Extracted Structured Fields (mandatory for every pipeline)

| Field | Source | Notes |
|---|---|---|
| `host` | Before first space | IP or hostname |
| `timestamp` | Inside `[...]` | Full timestamp string |
  Assign batch_id based on record ordinal number
  Q2 is computed globally (not per batch) and stored with batch_id = 0
| `log_hour` | Derived from timestamp | Integer 0–23 |
| `http_method` | First token of quoted request | GET, POST, HEAD, … |
| **Hive** | Load raw data into staging table, use `NTILE(n)` or `CEIL(ROW_NUMBER()/batch_size)` to assign `batch_id` in a view; Q2 is computed globally and stored with `batch_id = 0`, while other batch-aware aggregates may use `batch_id`. |
| `protocol_version` | Third token of quoted request | HTTP/1.0, HTTP/1.1 |
| `status_code` | After closing `"` | Integer |
| `bytes_transferred` | Last field | Integer; `-` → 0 |
| `is_malformed` | Parse result flag | Boolean / count |

---

## 3. Overall System Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        INPUT LAYER                                   │
Q2 is computed globally rather than per batch. In the reporting database, it is stored with `batch_id = 0` to distinguish it from batch-scoped aggregates.
│   NASA_access_log_Jul95.gz  +  NASA_access_log_Aug95.gz              │
│   (raw, unmodified .gz files read directly by each pipeline)         │
└──────────────────────────┬───────────────────────────────────────────┘
                           │  (decompression inside pipeline only)
              ┌────────────▼────────────┐
              │   CONTROLLER / CLI      │  ← Python or Shell script
              │  Pipeline selector UI   │  ← User picks: MR / Pig / Hive / MongoDB
              └─────────┬───────────────┘
        ┌───────────────┼───────────────┬──────────────────┐
        ▼               ▼               ▼                  ▼
In the current implementation and report output, `error_rate` is stored and displayed as a percentage in the range 0–100, rounded to 2 decimals.
   ┌─────────┐    ┌──────────┐   ┌──────────┐      ┌──────────────┐
   │MapReduce│    │   Pig    │   │   Hive   │      │   MongoDB    │
   │Pipeline │    │Pipeline  │   │ Pipeline │      │  Pipeline    │
   └────┬────┘    └────┬─────┘   └────┬─────┘      └──────┬───────┘
        │              │              │                    │
        └──────────────┴──────────────┴────────────────────┘
                                  │
                    ┌─────────────▼──────────────┐
                    │   RESULT LOADER (Python)    │
                    │  Reads pipeline output and  │
                    │  writes to PostgreSQL/MySQL │
                    └─────────────┬──────────────┘
                                  │
                    ┌─────────────▼──────────────┐
                    │   REPORTING MODULE          │
Execution time is normalized in `run_metadata.runtime_ms` and associated with query rows through `run_id`, which avoids duplicating timestamps across every query result row.
                    │  Reads from RDBMS, displays │
                    │  results + execution stats  │
                    └────────────────────────────┘
```

### Key Design Principles
1. **Single entry point** – one CLI/script orchestrates everything
2. **Pluggable backends** – swap pipeline without changing query semantics
3. **No pre-processing** – each pipeline receives the raw `.gz` file; decompression happens inside the pipeline
4. **Equivalence guaranteed** – same parsing regex, same cleaning rules, same query logic documented in a shared spec file

---

## 4. Parsing Strategy

### 4.1 Canonical Regex (shared across all pipelines)

```
^(\S+) \S+ \S+ \[(\d{2}/\w{3}/\d{4}):(\d{2}):\d{2}:\d{2} [+-]\d{4}\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+)
```

Group assignments:

| Group | Field |
|---|---|
| 1 | host |
| 2 | date string `DD/Mon/YYYY` |
| 3 | hour |
| 4 | http_method |
| 5 | resource_path |
| 6 | protocol_version |
| 7 | status_code |
| 8 | bytes (`-` → 0) |

### 4.2 Cleaning Rules (identical across all pipelines)

| Rule | Action |
|---|---|
| `bytes == "-"` | Replace with `0` |
| Missing request field (only host+timestamp) | Count as malformed; **do not drop** |
| Quoted request has < 3 tokens | Count as malformed; method/path/proto = `UNKNOWN` |
| Date cannot be parsed | Count as malformed |

### 4.3 Date Parsing

```
DD/Mon/YYYY  →  YYYY-MM-DD
```
Month abbreviation map: `Jan=01, Feb=02, Mar=03, Apr=04, May=05, Jun=06, Jul=07, Aug=08, Sep=09, Oct=10, Nov=11, Dec=12`

---

## 5. ETL Workflow (identical for all pipelines)

```
Step 1 – EXTRACT
  Read raw .gz file line by line (each pipeline handles decompression natively)
  
Step 2 – PARSE
  Apply canonical regex to each line
  If match fails → increment malformed counter, skip record from aggregation
  
Step 3 – TRANSFORM
  • Derive log_date from date string
  • Derive log_hour from hour group (integer)
  • Convert bytes: "-" → 0, else parseInt
  • Normalise status_code to integer
  
Step 4 – BATCH (see Section 6)
  Assign batch_id based on record ordinal number
  
Step 5 – AGGREGATE (execute 3 queries)
  Q1: Daily Traffic Summary
  Q2: Top 20 Requested Resources
  Q3: Hourly Error Analysis
  
Step 6 – LOAD
  Write aggregated results + metadata to PostgreSQL
  
Step 7 – REPORT
  Read from PostgreSQL and display formatted output
```

---

## 6. Batching Approach

### 6.1 Definition
- **Batch size** = number of input log records per batch (configurable via CLI)
- **Batch ID** starts at 1 and increments sequentially
- Final batch may contain fewer records but is still counted as a valid batch
- **Average batch size** = `total_records_processed / number_of_non_empty_batches`

### 6.2 Batching per Pipeline

| Pipeline | Batching Mechanism |
|---|---|
| **MapReduce** | Custom `InputFormat` that splits the `.gz` stream into N-record chunks; each split = one map task = one batch. `batch_id` injected via `Configuration` object. |
| **Pig** | Python UDF or Pig Latin script that groups records into N-record blocks and tags each with `batch_id` before aggregation. |
| **Hive** | Load raw data into staging table, use `NTILE(n)` or `CEIL(ROW_NUMBER()/batch_size)` to assign `batch_id` in a view; then aggregate per batch. |
| **MongoDB** | Python loader streams `.gz` and calls `insertMany()` with array slices of size N; each call = one batch. `batch_id` field stored with each document. |

### 6.3 Batch Metadata Stored in DB

```
pipeline_name, run_id, batch_id, batch_size (configured),
records_in_batch (actual), batch_start_time, batch_end_time
```

---

## 7. Mandatory Query Definitions

### Q1 – Daily Traffic Summary
```sql
SELECT log_date, status_code,
       COUNT(*)      AS request_count,
       SUM(bytes)    AS total_bytes
FROM parsed_logs
GROUP BY log_date, status_code
ORDER BY log_date, status_code;
```

### Q2 – Top 20 Requested Resources
```sql
SELECT resource_path,
       COUNT(*)               AS request_count,
       SUM(bytes)             AS total_bytes,
       COUNT(DISTINCT host)   AS distinct_host_count
FROM parsed_logs
GROUP BY resource_path
ORDER BY request_count DESC
LIMIT 20;
```

### Q3 – Hourly Error Analysis
```sql
SELECT log_date, log_hour,
       SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END)   AS error_request_count,
       COUNT(*)                                                             AS total_request_count,
       ROUND(SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END)
             * 100.0 / COUNT(*), 2)                                        AS error_rate,
       COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599 THEN host END)
                                                                           AS distinct_error_hosts
FROM parsed_logs
GROUP BY log_date, log_hour
ORDER BY log_date, log_hour;
```

---

## 8. Relational Reporting Schema (PostgreSQL)

### 8.1 Run Metadata Table
```sql
CREATE TABLE run_metadata (
    run_id          SERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(20) NOT NULL,      -- 'mapreduce' | 'pig' | 'hive' | 'mongodb'
    run_timestamp   TIMESTAMP NOT NULL,
    batch_size      INT NOT NULL,
    total_records   BIGINT NOT NULL,
    malformed_count BIGINT NOT NULL DEFAULT 0,
    num_batches     INT NOT NULL,
    avg_batch_size  NUMERIC(10,2) NOT NULL,
    runtime_ms      BIGINT NOT NULL            -- ms: read start → DB write end
);
```

### 8.2 Q1 Results Table
```sql
CREATE TABLE q1_daily_traffic (
    id            SERIAL PRIMARY KEY,
    run_id        INT REFERENCES run_metadata(run_id),
    pipeline_name VARCHAR(20) NOT NULL,
    batch_id      INT NOT NULL,
    log_date      DATE NOT NULL,
    status_code   INT NOT NULL,
    request_count BIGINT NOT NULL,
    total_bytes   BIGINT NOT NULL
);
```

### 8.3 Q2 Results Table
```sql
CREATE TABLE q2_top_resources (
    id                  SERIAL PRIMARY KEY,
    run_id              INT REFERENCES run_metadata(run_id),
    pipeline_name       VARCHAR(20) NOT NULL,
    batch_id            INT NOT NULL,
    resource_path       TEXT NOT NULL,
    request_count       BIGINT NOT NULL,
    total_bytes         BIGINT NOT NULL,
    distinct_host_count BIGINT NOT NULL
);
```

### 8.4 Q3 Results Table
```sql
CREATE TABLE q3_hourly_errors (
    id                   SERIAL PRIMARY KEY,
    run_id               INT REFERENCES run_metadata(run_id),
    pipeline_name        VARCHAR(20) NOT NULL,
    batch_id             INT NOT NULL,
    log_date             DATE NOT NULL,
    log_hour             SMALLINT NOT NULL,
    error_request_count  BIGINT NOT NULL,
    total_request_count  BIGINT NOT NULL,
    error_rate           NUMERIC(6,2) NOT NULL,
    distinct_error_hosts BIGINT NOT NULL
);
```

> [!NOTE]
> Separate result tables per query keeps schema clean and avoids nullable columns. `run_id` + `pipeline_name` enable direct SQL cross-pipeline comparison queries.

---

## 9. Ensuring Equivalence Across Pipelines

| Dimension | Mechanism |
|---|---|
| **Same dataset** | All pipelines read the exact same two `.gz` files |
| **Same parsing** | Canonical regex documented in `specs/PARSE_SPEC.md`; each pipeline's parser unit-tested against 10 golden records |
| **Same cleaning rules** | `specs/CLEANING_RULES.md` is the single source of truth |
| **Same query logic** | SQL/pseudo-SQL definitions in `specs/QUERY_SPEC.md`; no pipeline deviates |
| **Same batch size** | Passed as a CLI parameter; all pipelines use the identical value per experiment |
| **Cross-pipeline validator** | `common/validator.py` joins Q1/Q2/Q3 outputs from two runs and asserts row-level equality |

---

## 10. Recommended Implementation Order

### Phase 1 — Pipeline ① : Apache Hive (PRIMARY)
**Why Hive first?**
- Already configured and verified ✅
- Handles `.gz` natively — zero extra wiring
- Q1/Q2/Q3 map almost directly to HiveQL (least code to write)
- `REGEXP_EXTRACT` replaces custom Java regex parser
- Implementation time: ~4–6 hours vs 2–3 days for MapReduce

**Steps:**
1. `create_tables.hql` — external table on raw `.gz` files
2. `parse_view.hql` — `REGEXP_EXTRACT` view producing all 9 structured fields
3. `q1_daily_traffic.hql`, `q2_top_resources.hql`, `q3_hourly_errors.hql`
4. Batching: `CEIL(ROW_NUMBER() OVER () / N)` window function assigns `batch_id`
5. `run_hive.sh` — orchestrates all HQL files
6. `result_loader.py` — reads Hive output → PostgreSQL
7. `reporter.py` — reads PostgreSQL → formatted report

### Phase 1 — Pipeline ② : Apache MapReduce (SECONDARY, if time permits)
**Why second?**
- Best contrast to Hive (procedural vs. declarative)
- Demonstrates low-level batching control (custom `InputFormat`)
- Java + Hadoop already configured ✅

**Steps:**
1. `NASALogParser.java` — canonical regex parser
2. `NASALogInputFormat.java` — N-record splits for `.gz`
3. Three jobs: `DailyTrafficJob`, `TopResourcesJob`, `HourlyErrorJob`
4. Shares the same `result_loader.py` and `reporter.py`

### Phase 2 (complete after the review)

| Pipeline | Key Challenge |
|---|---|
| **Pig** | Python UDF for regex parsing; `GROUP BY` for aggregation |
| **MongoDB** | Stream `.gz` in Python; `insertMany()` batches; Aggregation Pipeline for Q1/Q2/Q3 |

---

## 11. Project Directory Structure

```
End-Semester-Project/
├── data/
│   ├── NASA_access_log_Jul95.gz
│   └── NASA_access_log_Aug95.gz
│
├── specs/
│   ├── PARSE_SPEC.md          ← canonical regex + field definitions
│   ├── CLEANING_RULES.md      ← cleaning & error-handling rules
│   └── QUERY_SPEC.md          ← Q1/Q2/Q3 definitions (single source of truth)
│
├── pipelines/
│   ├── mapreduce/
│   │   ├── src/
│   │   │   ├── NASALogParser.java
│   │   │   ├── NASALogInputFormat.java
│   │   │   ├── DailyTrafficJob.java
│   │   │   ├── TopResourcesJob.java
│   │   │   └── HourlyErrorJob.java
│   │   ├── pom.xml
│   │   └── run_mr.sh
│   │
│   ├── hive/
│   │   ├── create_tables.hql
│   │   ├── parse_view.hql
│   │   ├── q1_daily_traffic.hql
│   │   ├── q2_top_resources.hql
│   │   ├── q3_hourly_errors.hql
│   │   └── run_hive.sh
│   │
│   ├── pig/
│   │   ├── parse_udf.py
│   │   ├── etl.pig
│   │   └── run_pig.sh
│   │
│   └── mongodb/
│       ├── loader.py
│       ├── q1_aggregation.js
│       ├── q2_aggregation.js
│       ├── q3_aggregation.js
│       └── run_mongo.sh
│
├── common/
│   ├── result_loader.py       ← writes pipeline output → PostgreSQL
│   ├── reporter.py            ← reads PostgreSQL → formatted report
│   └── validator.py           ← cross-pipeline equivalence checker
│
├── schema/
│   └── reporting_schema.sql   ← all CREATE TABLE statements
│
├── controller.py              ← main CLI entry point (pipeline selector)
└── README.md
```

---

## 12. Controller CLI Interface

```bash
# Run MapReduce pipeline with batch size 50,000
python3 controller.py --pipeline mapreduce --batch-size 50000 --input data/

# Run Hive pipeline
python3 controller.py --pipeline hive      --batch-size 50000 --input data/

# Run Pig pipeline
python3 controller.py --pipeline pig       --batch-size 50000 --input data/

# Run MongoDB pipeline
python3 controller.py --pipeline mongodb   --batch-size 50000 --input data/
```

**Controller flow:**
1. Validate args
2. Record `run_start` timestamp
3. Invoke selected pipeline's run script
4. Invoke `result_loader.py` on pipeline output
5. Record `run_end` → compute `runtime_ms`
6. Write row to `run_metadata`
7. Invoke `reporter.py`

---

## 13. Runtime Measurement Definition

```
runtime_ms = timestamp_when_result_loader_finishes_DB_write
           - timestamp_when_pipeline_starts_reading_first_gz_byte
```

**Excludes:** dataset download, software setup, report rendering.

---

## 14. Phase 1 Demo Script (show the professor)

| Step | Command / Action |
|---|---|
| 1. Show raw data | `zcat data/NASA_access_log_Jul95.gz \| head -5` |
| 2. Show parser unit test | Run `NASALogParserTest.java` against 10 golden records |
| 3. Launch MR pipeline | `python3 controller.py --pipeline mapreduce --batch-size 50000 --input data/` |
| 4. Show HDFS output | `hdfs dfs -cat /output/q1/part-r-00000 \| head -20` |
| 5. Show PostgreSQL | `psql -c "SELECT * FROM q1_daily_traffic LIMIT 10;"` |
| 6. Show report | `python3 common/reporter.py --run-id 1` |
| 7. Launch Hive pipeline | Repeat 3–6 with `--pipeline hive` |
| 8. Show equivalence | `python3 common/validator.py --run-id-a 1 --run-id-b 2` |

---

## 15. Summary Table for Professor Presentation

| Phase 1 Discussion Point | Our Approach |
|---|---|
| **Overall system design** | Single-controller multi-backend ETL tool; pluggable pipeline backends; common loader and reporter |
| **Parsing strategy** | Canonical NCSA CLF regex documented in `PARSE_SPEC.md`; malformed records counted, not silently dropped |
| **ETL workflow** | Extract → Parse → Transform → Batch → Aggregate (Q1/Q2/Q3) → Load → Report |
| **Batching approach** | N-record batches (CLI parameter); sequential batch IDs from 1; avg = total_records/non_empty_batches |
| **Reporting DB schema** | `run_metadata` + `q1_daily_traffic` + `q2_top_resources` + `q3_hourly_errors` in PostgreSQL |
| **Equivalence plan** | Shared spec files + golden-record unit tests + post-run `validator.py` cross-checks row-level output |
| **Phase 1 pipelines** | **Hive ✅ (primary)** + MapReduce (secondary, if time allows) |
| **Phase 2 pipelines** | Pig + MongoDB |

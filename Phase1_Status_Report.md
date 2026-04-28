# DAS 839 Phase 1 Status Report

## 1. Project Snapshot
- Project: Multi-Pipeline ETL and Reporting Framework for NASA HTTP log analytics
- Current phase: Phase 1
- Working pipeline(s): Hive (fully working), common controller/loader/reporter (working)
- Planned next pipeline for implementation: MapReduce

## 2. Current Working Prototype (Verified)
A full end-to-end run was executed successfully using:
- Command: `python3 controller.py --pipeline hive --batch-size 100000`
- Latest verified run id: 13

Observed metadata for run 13:
- Pipeline: hive
- Batch size: 100000
- Total records: 3461613
- Malformed records: 8
- Number of batches: 35
- Average batch size: 98903.23
- Runtime: 95549 ms

Output artifacts generated under `/tmp/etl_output/run_13/`:
- `malformed/`
- `q1/`
- `q2/`
- `q3/`
- `step1.log`, `step2.log`

Results were loaded to PostgreSQL and displayed by the reporting module.

We verified correctness by checking that aggregated request counts match total valid records and that batch IDs remain contiguous across the run.
Although some sample batches can show zero errors, the global aggregation for the full run confirms non-zero error counts.

## 3. Architecture Implemented in Phase 1
- `controller.py`: single entry-point CLI pipeline selector and orchestrator
- `pipelines/hive/run_hive.sh`: executes Hive ETL + query scripts
- `pipelines/hive/01_create_tables.hql`: extract/parse/transform and batch assignment
- `pipelines/hive/02_queries.hql`: malformed count + Q1/Q2/Q3 aggregations
- `common/result_loader.py`: loads output TSV files to PostgreSQL schema
- `common/reporter.py`: displays metadata and query outputs from PostgreSQL
- `schema/reporting_schema.sql`: reporting DB schema

### 3.1 Implemented vs Planned Pipeline Scope
- Implemented now (Phase 1): Hive pipeline end-to-end with DB load and reporting
- Planned next (Phase 2): MapReduce, Pig, MongoDB with identical semantics and output schema
- Controller UX for all four choices is already present; non-Hive backends remain planned implementations

The interface contract supports four pipeline choices, but only Hive is implemented at present.

The interface contract supports four pipeline choices, but only Hive is implemented at present.

## 4. Parsing Strategy (Canonical)
Input log format:
- `host - - [DD/Mon/YYYY:HH:MM:SS -TZOFF] "METHOD path protocol" status bytes`

Extracted fields:
- `host`, `timestamp` components, `log_date`, `log_hour`, `http_method`, `resource_path`, `protocol_version`, `status_code`, `bytes`

Cleaning rules:
- `bytes` of `-`, empty, or null is converted to `0`
- unparseable records are excluded from aggregations but counted as malformed

## 5. ETL Workflow (Hive)
1. Extract:
- Read raw NASA logs through Hive external table from HDFS path `hdfs:///nasalogs/raw/`

2. Parse/Transform:
- Parse with regex using `REGEXP_EXTRACT`
- Convert date to `YYYY-MM-DD`
- Parse hour and status as integers
- Normalize bytes field to numeric with fallback to zero

3. Batch:
- Assign sequential batch ids using:
- `CEIL(ROW_NUMBER() OVER (ORDER BY line) / ${BATCH_SIZE})`

4. Aggregate:
- Q1 Daily Traffic Summary (batched)
- Q2 Top 20 Requested Resources (global top-20 by design)
- Q3 Hourly Error Analysis (batched)

Q2 is computed globally and not per batch.

Execution time is represented in `run_metadata.runtime_ms` and tied to query results through `run_id`.

5. Load/Report:
- Load output rows + run metadata to PostgreSQL
- Report script prints execution metadata and query outputs

## 6. Batching and Runtime Compliance
Batching:
- Batch size is CLI-configurable and shared by all pipelines
- Batch ids start at 1 and are sequential
- Last partial batch is counted
- Average batch size formula used: `total_records / non_empty_batches`
- Q1 and Q3 are grouped by `batch_id` with their dimension columns
- Q2 is computed globally and stored with `batch_id = 0` by design

Runtime:
- Runtime now measured from pipeline start to completion of PostgreSQL writes
- Report rendering is excluded

## 7. Relational Reporting Schema
Tables implemented:
- `run_metadata`
- `q1_daily_traffic`
- `q2_top_resources`
- `q3_hourly_errors`

All result tables store:
- `pipeline_name`, `run_id`, `batch_id`, query-specific output columns

## 8. Equivalence Plan Across Pipelines (Phase 2)
To maintain fair comparison, all pipelines will follow:
- Same source NASA dataset
- Same parsing and cleaning semantics
- Same query definitions (Q1/Q2/Q3)
- Same batch size per experiment
- Same runtime boundary definition
- Same PostgreSQL output schema

Planned validation:
- Compare query outputs row-wise across run ids for different backends
- Investigate any mismatch as equivalence violation

Canonical query contract for equivalence:
- Q1: grouped by `batch_id, log_date, status_code`
- Q2: global top-20 by `resource_path` (stored with `batch_id = 0`)
- Q3: grouped by `batch_id, log_date, log_hour`

## 9. Phase 1 Deliverables Status
- Phase 1 minimum prototype objective is substantially met via one working pipeline, with the second pipeline planned.
- Architecture defined: completed
- One working pipeline (Hive): completed
- End-to-end ETL + DB load + report: completed
- Runtime and batching metrics: completed
- Phase 1 report alignment with implementation: completed

## 10. Immediate Next Steps
1. Implement MapReduce backend runner and jobs with the same semantics and output format
2. Add pipeline-specific run scripts for Pig and MongoDB
3. Add cross-pipeline comparator utility for Q1/Q2/Q3 equivalence checks
4. Prepare Phase 2 comparative observations (correctness, runtime, implementation effort)

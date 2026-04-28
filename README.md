# End-Semester-Project — Multi-Pipeline ETL & Reporting (DAS 839)

This repository contains the Phase 1 Hive pipeline and orchestration for NASA web server log analytics.

Quick start

1. Run Hive pipeline (example):

```bash
python3 controller.py --pipeline hive --batch-size 100000
```

2. After run completes:
- Results are loaded into PostgreSQL tables: `run_metadata`, `q1_daily_traffic`, `q2_top_resources`, `q3_hourly_errors`.
- CSV exports are created under `output/hive/run_<run_id>/` (00_run_metadata.csv, 01_q1..., 02_q2..., 03_q3...)

Files to note

- `controller.py` — main CLI orchestrator
- `pipelines/hive/` — Hive HQL and run script
- `common/result_loader.py` — loads TSV outputs into PostgreSQL
- `common/export_results.py` — exports PostgreSQL results to CSV files
- `common/reporter.py` — presents run report in terminal
- `schema/reporting_schema.sql` — DB schema for reporting tables

What to commit / what to exclude

- Commit: source code, HQL, scripts, docs, `.tex` and generated PDFs (optional)
- Exclude: raw data files (`data/`), Hive metastore files (`metastore_db/`), `output/` and other large runtime artifacts — see `.gitignore`

Security

- Do NOT commit credentials. Use environment variables or a `.env` file and keep `.env` in `.gitignore`.
- Add a `.env.example` with placeholder variables for documentation.

If you want, I can initialize the local git repo and create these files for you, or provide the exact git commands to run locally or via the GitHub web UI.
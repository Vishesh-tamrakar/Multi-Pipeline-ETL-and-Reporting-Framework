# Hive Pipeline Output

This folder contains CSV exports of all Hive pipeline run results, organized by run ID.

## Folder Structure

```
hive/
├── run_1/
│   ├── 00_run_metadata.csv      # Run metadata (pipeline name, batch size, runtime, etc.)
│   ├── 01_q1_daily_traffic.csv  # Query 1: Daily traffic summary (by batch, date, status)
│   ├── 02_q2_top_resources.csv  # Query 2: Top 20 requested resources (global)
│   └── 03_q3_hourly_errors.csv  # Query 3: Hourly error analysis (by batch, date, hour)
├── run_2/
│   ├── 00_run_metadata.csv
│   ├── 01_q1_daily_traffic.csv
│   ├── 02_q2_top_resources.csv
│   └── 03_q3_hourly_errors.csv
└── ...
```

## File Descriptions

### 00_run_metadata.csv
Contains metadata about the run:
- `run_id`: Unique identifier for this run
- `pipeline_name`: Pipeline used (e.g., "hive")
- `run_timestamp`: When the run started
- `batch_size`: Number of records per batch
- `total_records`: Total records processed
- `malformed_count`: Records that failed parsing
- `num_batches`: Total number of batches
- `avg_batch_size`: Average records per batch
- `runtime_ms`: Total execution time in milliseconds

### 01_q1_daily_traffic.csv
Daily traffic summary grouped by batch, date, and HTTP status code:
- `batch_id`: Batch number (1-N)
- `log_date`: Date (YYYY-MM-DD)
- `status_code`: HTTP status code (200, 404, etc.)
- `request_count`: Number of requests
- `total_bytes`: Total bytes transferred

### 02_q2_top_resources.csv
Top 20 most requested resources (global, not per-batch):
- `batch_id`: Always 0 (global aggregation)
- `resource_path`: URL path requested
- `request_count`: Number of times requested
- `total_bytes`: Total bytes transferred for this resource
- `distinct_host_count`: Number of unique hosts requesting this resource

### 03_q3_hourly_errors.csv
Hourly error analysis for HTTP errors (4xx and 5xx status codes):
- `batch_id`: Batch number (1-N)
- `log_date`: Date (YYYY-MM-DD)
- `log_hour`: Hour of day (0-23)
- `error_request_count`: Number of error requests
- `total_request_count`: Total requests in this hour
- `error_rate`: Error percentage (0.00-100.00)
- `distinct_error_hosts`: Number of unique hosts with errors

## Viewing Results

Open any CSV file in a spreadsheet editor (Excel, LibreOffice Calc) or text editor:

```bash
# View in terminal
cat output/hive/run_1/00_run_metadata.csv

# Open in spreadsheet (if available)
libreoffice output/hive/run_1/01_q1_daily_traffic.csv
```

## Notes

- Each run is stored in a separate folder (run_1/, run_2/, etc.)
- Files are automatically generated after each pipeline execution
- Data is also stored in PostgreSQL and can be queried directly
- All files use standard CSV format with headers

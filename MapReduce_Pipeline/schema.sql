CREATE TABLE IF NOT EXISTS runs_metadata (
    run_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(50),
    batch_id INT,
    batch_size INT,
    avg_batch_size FLOAT,
    runtime_seconds FLOAT,
    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS q1_daily_traffic (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES runs_metadata(run_id),
    log_date DATE,
    status_code INT,
    request_count BIGINT,
    total_bytes BIGINT
);

CREATE TABLE IF NOT EXISTS q2_top_resources (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES runs_metadata(run_id),
    resource_path TEXT,
    request_count BIGINT,
    total_bytes BIGINT,
    distinct_host_count BIGINT
);

CREATE TABLE IF NOT EXISTS q3_hourly_errors (
    id SERIAL PRIMARY KEY,
    run_id INT REFERENCES runs_metadata(run_id),
    log_date DATE,
    log_hour INT,
    error_request_count BIGINT,
    total_request_count BIGINT,
    error_rate FLOAT,
    distinct_error_hosts BIGINT
);

-- DAS 839 End Semester Project
-- Reporting Database Schema (PostgreSQL)

DROP TABLE IF EXISTS q3_hourly_errors;
DROP TABLE IF EXISTS q2_top_resources;
DROP TABLE IF EXISTS q1_daily_traffic;
DROP TABLE IF EXISTS run_metadata;

CREATE TABLE run_metadata (
    run_id          SERIAL PRIMARY KEY,
    pipeline_name   VARCHAR(20)   NOT NULL,
    run_timestamp   TIMESTAMP     NOT NULL DEFAULT NOW(),
    batch_size      INT           NOT NULL,
    total_records   BIGINT        NOT NULL DEFAULT 0,
    malformed_count BIGINT        NOT NULL DEFAULT 0,
    num_batches     INT           NOT NULL DEFAULT 0,
    avg_batch_size  NUMERIC(12,2) NOT NULL DEFAULT 0,
    runtime_ms      BIGINT        NOT NULL DEFAULT 0
);

CREATE TABLE q1_daily_traffic (
    id            SERIAL PRIMARY KEY,
    run_id        INT     REFERENCES run_metadata(run_id),
    pipeline_name VARCHAR(20) NOT NULL,
    batch_id      INT         NOT NULL,
    log_date      VARCHAR(10) NOT NULL,
    status_code   INT         NOT NULL,
    request_count BIGINT      NOT NULL,
    total_bytes   BIGINT      NOT NULL
);

CREATE TABLE q2_top_resources (
    id                  SERIAL PRIMARY KEY,
    run_id              INT     REFERENCES run_metadata(run_id),
    pipeline_name       VARCHAR(20) NOT NULL,
    batch_id            INT         NOT NULL,
    resource_path       TEXT        NOT NULL,
    request_count       BIGINT      NOT NULL,
    total_bytes         BIGINT      NOT NULL,
    distinct_host_count BIGINT      NOT NULL
);

CREATE TABLE q3_hourly_errors (
    id                   SERIAL PRIMARY KEY,
    run_id               INT     REFERENCES run_metadata(run_id),
    pipeline_name        VARCHAR(20)   NOT NULL,
    batch_id             INT           NOT NULL,
    log_date             VARCHAR(10)   NOT NULL,
    log_hour             SMALLINT      NOT NULL,
    error_request_count  BIGINT        NOT NULL,
    total_request_count  BIGINT        NOT NULL,
    error_rate           NUMERIC(6,2)  NOT NULL,
    distinct_error_hosts BIGINT        NOT NULL
);

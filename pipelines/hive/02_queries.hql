-- =============================================================
-- STEP 3a: Count malformed records
-- A record is malformed if the core regex groups cannot be extracted.
-- Output: single integer written to ${OUTPUT_DIR}/malformed/
-- =============================================================
INSERT OVERWRITE LOCAL DIRECTORY '${OUTPUT_DIR}/malformed'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT COUNT(*) AS malformed_count
FROM nasa_raw
WHERE
    REGEXP_EXTRACT(line, '^(\\S+)', 1) = ''
    OR REGEXP_EXTRACT(line, '\\[(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2})', 3) = ''
    OR REGEXP_EXTRACT(line, '" (\\d{3}) ', 1) = '';

-- =============================================================
-- STEP 3b: Q1 - Daily Traffic Summary
-- Output columns: batch_id, log_date, status_code, request_count, total_bytes
-- =============================================================
INSERT OVERWRITE LOCAL DIRECTORY '${OUTPUT_DIR}/q1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    batch_id,
    log_date,
    status_code,
    COUNT(*)   AS request_count,
    SUM(bytes) AS total_bytes
FROM nasa_parsed
GROUP BY batch_id, log_date, status_code
ORDER BY batch_id, log_date, status_code;

-- =============================================================
-- STEP 3c: Q2 - Top 20 Requested Resources (global, all batches)
-- Output columns: batch_id, resource_path, request_count, total_bytes, distinct_host_count
-- =============================================================
INSERT OVERWRITE LOCAL DIRECTORY '${OUTPUT_DIR}/q2'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    0                    AS batch_id,
    resource_path,
    COUNT(*)             AS request_count,
    SUM(bytes)           AS total_bytes,
    COUNT(DISTINCT host) AS distinct_host_count
FROM nasa_parsed
GROUP BY resource_path
ORDER BY request_count DESC
LIMIT 20;

-- =============================================================
-- STEP 3d: Q3 - Hourly Error Analysis
-- Output columns: batch_id, log_date, log_hour, error_request_count,
--                 total_request_count, error_rate, distinct_error_hosts
-- =============================================================
INSERT OVERWRITE LOCAL DIRECTORY '${OUTPUT_DIR}/q3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT
    batch_id,
    log_date,
    log_hour,
    SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END)
                                                        AS error_request_count,
    COUNT(*)                                            AS total_request_count,
    ROUND(
        SUM(CASE WHEN status_code BETWEEN 400 AND 599 THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 2)                          AS error_rate,
    COUNT(DISTINCT CASE WHEN status_code BETWEEN 400 AND 599
                        THEN host END)                  AS distinct_error_hosts
FROM nasa_parsed
GROUP BY batch_id, log_date, log_hour
ORDER BY batch_id, log_date, log_hour;

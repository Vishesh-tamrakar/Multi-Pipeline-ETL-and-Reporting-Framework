-- =============================================================
-- STEP 1: Raw external table pointing to HDFS log files
-- =============================================================
DROP TABLE IF EXISTS nasa_raw;

CREATE EXTERNAL TABLE nasa_raw (
    line STRING
)
STORED AS TEXTFILE
LOCATION 'hdfs:///nasalogs/raw/';

-- =============================================================
-- STEP 2: Parsed + batched table
-- Uses ROW_NUMBER to assign sequential batch_id.
-- Malformed lines (regex returns empty host) are excluded from
-- aggregations but counted separately.
-- =============================================================
DROP TABLE IF EXISTS nasa_parsed;

CREATE TABLE nasa_parsed AS
SELECT
    REGEXP_EXTRACT(line, '^(\\S+)', 1)  AS host,

    -- log_date: convert DD/Mon/YYYY -> YYYY-MM-DD
    CONCAT(
        REGEXP_EXTRACT(line, '\\[(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2})', 3), '-',
        CASE REGEXP_EXTRACT(line, '\\[(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2})', 2)
            WHEN 'Jan' THEN '01' WHEN 'Feb' THEN '02' WHEN 'Mar' THEN '03'
            WHEN 'Apr' THEN '04' WHEN 'May' THEN '05' WHEN 'Jun' THEN '06'
            WHEN 'Jul' THEN '07' WHEN 'Aug' THEN '08' WHEN 'Sep' THEN '09'
            WHEN 'Oct' THEN '10' WHEN 'Nov' THEN '11' WHEN 'Dec' THEN '12'
            ELSE '00'
        END, '-',
        REGEXP_EXTRACT(line, '\\[(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2})', 1)
    ) AS log_date,

    CAST(REGEXP_EXTRACT(line, '\\[(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2})', 4) AS INT)
                                                                    AS log_hour,

    REGEXP_EXTRACT(line, '"(\\S+) \\S+ \\S+"', 1)                  AS http_method,
    REGEXP_EXTRACT(line, '"\\S+ (\\S+) \\S+"', 1)                  AS resource_path,
    REGEXP_EXTRACT(line, '"\\S+ \\S+ (\\S+)"', 1)                  AS protocol_version,

    CAST(REGEXP_EXTRACT(line, '" (\\d{3}) ', 1) AS INT)             AS status_code,

    -- bytes: treat '-', missing, or empty as 0
    CAST(
        IF(
            REGEXP_EXTRACT(line, '" \\d{3} (\\S+)$', 1) = '-'
            OR REGEXP_EXTRACT(line, '" \\d{3} (\\S+)$', 1) = ''
            OR REGEXP_EXTRACT(line, '" \\d{3} (\\S+)$', 1) IS NULL,
            '0',
            REGEXP_EXTRACT(line, '" \\d{3} (\\S+)$', 1)
        )
    AS BIGINT)                                                       AS bytes,

    -- batch_id: sequential, 1-based
    CAST(CEIL(ROW_NUMBER() OVER (ORDER BY line) / ${BATCH_SIZE}) AS INT)
                                                                    AS batch_id
FROM nasa_raw
WHERE
    -- only keep parseable records (non-empty host means regex matched)
    REGEXP_EXTRACT(line, '^(\\S+)', 1) != ''
    AND REGEXP_EXTRACT(line, '\\[(\\d{2})/(\\w{3})/(\\d{4}):(\\d{2})', 3) != ''
    AND REGEXP_EXTRACT(line, '" (\\d{3}) ', 1) != '';

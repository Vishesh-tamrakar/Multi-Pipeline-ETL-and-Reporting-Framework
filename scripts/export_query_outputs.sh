#!/usr/bin/env bash
set -euo pipefail

RUN_ID="${1:-}"
OUT_BASE="${2:-artifacts}"
DB_NAME="${DB_NAME:-etldb}"
DB_USER="${DB_USER:-vishesh}"

if [[ -z "${RUN_ID}" ]]; then
  echo "Usage: bash scripts/export_query_outputs.sh <run_id> [output_base_dir]"
  exit 1
fi

OUT_DIR="${OUT_BASE}/run_${RUN_ID}"
mkdir -p "${OUT_DIR}"

echo "Exporting run ${RUN_ID} outputs to ${OUT_DIR} ..."

psql -d "${DB_NAME}" -U "${DB_USER}" -c "\copy (SELECT run_id,pipeline_name,run_timestamp,batch_size,total_records,malformed_count,num_batches,avg_batch_size,runtime_ms FROM run_metadata WHERE run_id=${RUN_ID}) TO '${PWD}/${OUT_DIR}/run_metadata.csv' CSV HEADER"

psql -d "${DB_NAME}" -U "${DB_USER}" -c "\copy (SELECT run_id,pipeline_name,batch_id,log_date,status_code,request_count,total_bytes FROM q1_daily_traffic WHERE run_id=${RUN_ID} ORDER BY batch_id,log_date,status_code) TO '${PWD}/${OUT_DIR}/q1_daily_traffic.csv' CSV HEADER"

psql -d "${DB_NAME}" -U "${DB_USER}" -c "\copy (SELECT run_id,pipeline_name,batch_id,resource_path,request_count,total_bytes,distinct_host_count FROM q2_top_resources WHERE run_id=${RUN_ID} ORDER BY request_count DESC) TO '${PWD}/${OUT_DIR}/q2_top_resources.csv' CSV HEADER"

psql -d "${DB_NAME}" -U "${DB_USER}" -c "\copy (SELECT run_id,pipeline_name,batch_id,log_date,log_hour,error_request_count,total_request_count,error_rate,distinct_error_hosts FROM q3_hourly_errors WHERE run_id=${RUN_ID} ORDER BY batch_id,log_date,log_hour) TO '${PWD}/${OUT_DIR}/q3_hourly_errors.csv' CSV HEADER"

# Readable short previews
psql -d "${DB_NAME}" -U "${DB_USER}" -c "\copy (SELECT batch_id,log_date,status_code,request_count,total_bytes FROM q1_daily_traffic WHERE run_id=${RUN_ID} ORDER BY batch_id,log_date,status_code LIMIT 50) TO '${PWD}/${OUT_DIR}/q1_preview_top50.csv' CSV HEADER"

psql -d "${DB_NAME}" -U "${DB_USER}" -c "\copy (SELECT resource_path,request_count,total_bytes,distinct_host_count FROM q2_top_resources WHERE run_id=${RUN_ID} ORDER BY request_count DESC LIMIT 20) TO '${PWD}/${OUT_DIR}/q2_preview_top20.csv' CSV HEADER"

psql -d "${DB_NAME}" -U "${DB_USER}" -c "\copy (SELECT batch_id,log_date,log_hour,error_request_count,total_request_count,error_rate,distinct_error_hosts FROM q3_hourly_errors WHERE run_id=${RUN_ID} ORDER BY batch_id,log_date,log_hour LIMIT 50) TO '${PWD}/${OUT_DIR}/q3_preview_top50.csv' CSV HEADER"

echo "Done. Files created in ${OUT_DIR}"
#!/usr/bin/env bash
# =============================================================
# run_hive.sh  –  Hive pipeline runner (Hive 4.x embedded mode)
# Usage: bash run_hive.sh <batch_size> <run_id> <output_base_dir>
# Example: bash run_hive.sh 100000 1 /tmp/etl_output
# =============================================================
set -euo pipefail

BATCH_SIZE="${1:-100000}"
RUN_ID="${2:-1}"
OUTPUT_DIR="${3:-/tmp/etl_output}/run_${RUN_ID}"

HQL_DIR="$(cd "$(dirname "$0")" && pwd)"
HIVE_HOME="${HIVE_HOME:-/home/vishesh/Tutorial/apache-hive-4.2.0-bin}"
BEELINE="${HIVE_HOME}/bin/beeline"

# Use embedded HiveServer2 (no standalone daemon needed)
HS2_URL="jdbc:hive2://"

# Fix jline / TTY issues for non-interactive mode and allocate more RAM
export TERM=dumb
export HADOOP_CLIENT_OPTS="-Xmx3g -Djline.terminal=jline.UnsupportedTerminal -Dorg.jline.terminal.provider=dumb"
export JAVA_TOOL_OPTIONS="-Xmx3g -Djline.terminal=jline.UnsupportedTerminal -Dorg.jline.terminal.provider=dumb"
export HADOOP_HEAPSIZE="${HADOOP_HEAPSIZE:-3072}"


echo "=============================================="
echo " Hive Pipeline Starting"
echo " Batch Size : ${BATCH_SIZE}"
echo " Run ID     : ${RUN_ID}"
echo " Output Dir : ${OUTPUT_DIR}"
echo "=============================================="

# Clean output directory
rm -rf "${OUTPUT_DIR}" && mkdir -p "${OUTPUT_DIR}"

# Helper to run a HQL file via beeline by piping it via stdin
run_hql() {
    local hql_file="$1"
    local log_file="$2"

    {
        # Apply session settings inside SQL so they are honored by HiveServer2.
        echo "set hive.execution.engine=mr;"
        echo "set mapreduce.framework.name=local;"
        echo "set hive.exec.parallel=false;"
        echo "set mapreduce.task.io.sort.mb=64;"
        echo "set mapreduce.map.memory.mb=1536;"
        echo "set mapreduce.reduce.memory.mb=1536;"
        echo "set mapreduce.map.java.opts=-Xmx1024m;"
        echo "set mapreduce.reduce.java.opts=-Xmx1024m;"
        sed -e "s|\${BATCH_SIZE}|${BATCH_SIZE}|g" \
            -e "s|\${OUTPUT_DIR}|${OUTPUT_DIR}|g" \
            "${hql_file}"
    } | "${BEELINE}" \
        -u "${HS2_URL}" \
        --silent=false \
        --hiveconf hive.execution.engine=mr \
        --hiveconf mapreduce.framework.name=local \
        2>&1 | tee "${log_file}"

    local exit_code="${PIPESTATUS[1]}"
    if [ "${exit_code}" -ne 0 ]; then
        echo "[ERROR] HQL script failed: ${hql_file}"
        exit "${exit_code}"
    fi
}

# ------ STEP 1: Create raw + parsed tables ------
echo ""
echo "[$(date '+%H:%M:%S')] Step 1/2 – Creating raw table and parsed/batched table..."
run_hql "${HQL_DIR}/01_create_tables.hql" "${OUTPUT_DIR}/step1.log"
echo "[$(date '+%H:%M:%S')] Step 1 complete."

# ------ STEP 2: Run Q1, Q2, Q3, malformed count ------
echo ""
echo "[$(date '+%H:%M:%S')] Step 2/2 – Running Q1, Q2, Q3 and malformed count..."
run_hql "${HQL_DIR}/02_queries.hql" "${OUTPUT_DIR}/step2.log"
echo "[$(date '+%H:%M:%S')] Step 2 complete."

echo ""
echo "=============================================="
echo " Hive pipeline finished."
echo " Results written to: ${OUTPUT_DIR}/"
echo "   malformed/ – malformed record count"
echo "   q1/        – Daily Traffic Summary"
echo "   q2/        – Top 20 Requested Resources"
echo "   q3/        – Hourly Error Analysis"
echo "=============================================="

#!/usr/bin/env python3
"""
result_loader.py
Reads Hive TSV output files from a run directory and
loads results into the PostgreSQL reporting database.

Usage:
    python3 result_loader.py \
        --pipeline hive \
        --run-id 1 \
        --batch-size 100000 \
        --output-dir /tmp/etl_output/run_1 \
        --db-host localhost \
        --db-name etldb \
        --db-user postgres
"""

import argparse
import glob
import os
import sys
import psycopg2

# ----------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--pipeline',    required=True)
    p.add_argument('--run-id',      type=int, required=True)
    p.add_argument('--batch-size',  type=int, required=True)
    p.add_argument('--output-dir',  required=True)
    p.add_argument('--db-host',     default='')
    p.add_argument('--db-port',     type=int, default=5432)
    p.add_argument('--db-name',     default='etldb')
    p.add_argument('--db-user',     default='vishesh')
    p.add_argument('--db-password', default='')
    return p.parse_args()


def read_tsv_files(directory):
    """Yield rows from all part-* files in a Hive output directory."""
    files = sorted(glob.glob(os.path.join(directory, '*')))
    for fpath in files:
        if os.path.isfile(fpath):
            with open(fpath, 'r', encoding='utf-8', errors='replace') as f:
                for line in f:
                    line = line.rstrip('\n')
                    if line:
                        yield line.split('\t')


def load_malformed(cur, output_dir):
    rows = list(read_tsv_files(os.path.join(output_dir, 'malformed')))
    return int(rows[0][0]) if rows else 0


def count_parsed_records(output_dir):
    """Sum request_count from Q1 output to get total valid records."""
    total = 0
    for row in read_tsv_files(os.path.join(output_dir, 'q1')):
        # row: batch_id, log_date, status_code, request_count, total_bytes
        if len(row) >= 4:
            try:
                total += int(row[3])
            except ValueError:
                pass
    return total


def count_batches(output_dir):
    """Find max batch_id from Q1 output."""
    max_batch = 0
    for row in read_tsv_files(os.path.join(output_dir, 'q1')):
        if len(row) >= 1:
            try:
                b = int(row[0])
                if b > max_batch:
                    max_batch = b
            except ValueError:
                pass
    return max_batch


# ----------------------------------------------------------------
def main():
    args = parse_args()

    kw = dict(dbname=args.db_name, user=args.db_user)
    if args.db_host:
        kw['host'] = args.db_host
        kw['port'] = args.db_port
        if args.db_password:
            kw['password'] = args.db_password
    conn = psycopg2.connect(**kw)
    cur = conn.cursor()

    out = args.output_dir
    pipeline = args.pipeline
    run_id = args.run_id

    # -- Compute metadata ------------------------------------------
    malformed    = load_malformed(cur, out)
    total_valid  = count_parsed_records(out)
    total_records= total_valid + malformed
    num_batches  = count_batches(out)
    avg_batch    = round(total_records / num_batches, 2) if num_batches > 0 else 0

    # -- Update run_metadata ---------------------------------------
    cur.execute("""
        UPDATE run_metadata SET
            total_records   = %s,
            malformed_count = %s,
            num_batches     = %s,
            avg_batch_size  = %s
        WHERE run_id = %s
    """, (total_records, malformed, num_batches, avg_batch,
          run_id))

    # -- Load Q1 ---------------------------------------------------
    print(f"Loading Q1 results (run_id={run_id})...")
    for row in read_tsv_files(os.path.join(out, 'q1')):
        if len(row) < 5:
            continue
        batch_id, log_date, status_code, request_count, total_bytes = \
            int(row[0]), row[1], int(row[2]), int(row[3]), int(row[4])
        cur.execute("""
            INSERT INTO q1_daily_traffic
              (run_id, pipeline_name, batch_id, log_date, status_code,
               request_count, total_bytes)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (run_id, pipeline, batch_id, log_date, status_code,
              request_count, total_bytes))

    # -- Load Q2 ---------------------------------------------------
    print(f"Loading Q2 results (run_id={run_id})...")
    for row in read_tsv_files(os.path.join(out, 'q2')):
        if len(row) < 5:
            continue
        batch_id = int(row[0])
        resource_path, request_count, total_bytes, distinct_host_count = \
            row[1], int(row[2]), int(row[3]), int(row[4])
        cur.execute("""
            INSERT INTO q2_top_resources
              (run_id, pipeline_name, batch_id, resource_path,
               request_count, total_bytes, distinct_host_count)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (run_id, pipeline, batch_id, resource_path,
              request_count, total_bytes, distinct_host_count))

    # -- Load Q3 ---------------------------------------------------
    print(f"Loading Q3 results (run_id={run_id})...")
    for row in read_tsv_files(os.path.join(out, 'q3')):
        if len(row) < 7:
            continue
        batch_id   = int(row[0])
        log_date   = row[1]
        log_hour   = int(row[2])
        err_count  = int(row[3])
        tot_count  = int(row[4])
        err_rate   = float(row[5])
        dist_hosts = int(row[6])
        cur.execute("""
            INSERT INTO q3_hourly_errors
              (run_id, pipeline_name, batch_id, log_date, log_hour,
               error_request_count, total_request_count,
               error_rate, distinct_error_hosts)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (run_id, pipeline, batch_id, log_date, log_hour,
              err_count, tot_count, err_rate, dist_hosts))

    conn.commit()
    cur.close()
    conn.close()
    print("All results loaded into PostgreSQL successfully.")


if __name__ == '__main__':
    main()

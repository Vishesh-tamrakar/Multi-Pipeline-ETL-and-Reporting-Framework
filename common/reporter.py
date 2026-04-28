#!/usr/bin/env python3
"""
reporter.py  –  Reads results from PostgreSQL and displays them.

Usage:
    python3 reporter.py --run-id 1 --db-name etldb --db-user postgres
"""

import argparse
import psycopg2

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--run-id',      type=int, required=True)
    p.add_argument('--db-host',     default='')
    p.add_argument('--db-port',     type=int, default=5432)
    p.add_argument('--db-name',     default='etldb')
    p.add_argument('--db-user',     default='vishesh')
    p.add_argument('--db-password', default='')
    return p.parse_args()

def sep(char='=', width=72):
    print(char * width)

def header(title):
    sep()
    print(f"  {title}")
    sep()

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
    rid = args.run_id

    # ---- Run metadata ----------------------------------------
    cur.execute("""
        SELECT pipeline_name, run_timestamp, batch_size,
               total_records, malformed_count, num_batches,
               avg_batch_size, runtime_ms
        FROM run_metadata WHERE run_id = %s
    """, (rid,))
    row = cur.fetchone()
    if not row:
        print(f"No run found with run_id={rid}")
        return

    (pipeline, ts, batch_size, total_recs, malformed,
     num_batches, avg_batch, runtime_ms) = row

    header(f"EXECUTION METADATA  –  Run ID: {rid}")
    print(f"  Pipeline       : {pipeline.upper()}")
    print(f"  Run Timestamp  : {ts}")
    print(f"  Batch Size     : {batch_size:,}")
    print(f"  Total Records  : {total_recs:,}")
    print(f"  Malformed Recs : {malformed:,}")
    print(f"  Num Batches    : {num_batches}")
    print(f"  Avg Batch Size : {avg_batch:,.2f}")
    print(f"  Runtime        : {runtime_ms:,} ms  ({runtime_ms/1000:.1f} s)")

    # ---- Q1 -----------------------------------------------
    header("QUERY 1 – Daily Traffic Summary  (sample: first 20 rows)")
    cur.execute("""
        SELECT batch_id, log_date, status_code, request_count, total_bytes
        FROM q1_daily_traffic
        WHERE run_id = %s
        ORDER BY batch_id, log_date, status_code
        LIMIT 20
    """, (rid,))
    print(f"  {'batch_id':>8}  {'log_date':<12}  {'status':>6}  "
          f"{'req_count':>12}  {'total_bytes':>14}")
    sep('-')
    for r in cur.fetchall():
        print(f"  {r[0]:>8}  {r[1]:<12}  {r[2]:>6}  "
              f"  {r[3]:>12,}  {r[4]:>14,}")

    # ---- Q2 -----------------------------------------------
    header("QUERY 2 – Top 20 Requested Resources")
    cur.execute("""
        SELECT resource_path, request_count, total_bytes, distinct_host_count
        FROM q2_top_resources
        WHERE run_id = %s
        ORDER BY request_count DESC
        LIMIT 20
    """, (rid,))
    print(f"  {'rank':>4}  {'resource_path':<45}  "
          f"{'req_count':>10}  {'total_bytes':>12}  {'hosts':>6}")
    sep('-')
    for i, r in enumerate(cur.fetchall(), 1):
        path = r[0][:43] + '..' if len(r[0]) > 45 else r[0]
        print(f"  {i:>4}  {path:<45}  {r[1]:>10,}  {r[2]:>12,}  {r[3]:>6,}")

    # ---- Q3 -----------------------------------------------
    header("QUERY 3 – Hourly Error Analysis  (sample: first 20 rows)")
    cur.execute("""
        SELECT batch_id, log_date, log_hour,
               error_request_count, total_request_count,
               error_rate, distinct_error_hosts
        FROM q3_hourly_errors
        WHERE run_id = %s
        ORDER BY batch_id, log_date, log_hour
        LIMIT 20
    """, (rid,))
    print(f"  {'batch':>5}  {'date':<12}  {'hr':>2}  "
          f"{'errors':>8}  {'total':>8}  {'err%':>6}  {'e_hosts':>7}")
    sep('-')
    for r in cur.fetchall():
        print(f"  {r[0]:>5}  {r[1]:<12}  {r[2]:>2}  "
              f"{r[3]:>8,}  {r[4]:>8,}  {r[5]:>6.2f}  {r[6]:>7,}")

    sep()
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
controller.py  –  Main CLI entry point for the ETL framework.

Usage:
    python3 controller.py --pipeline hive --batch-size 100000
    python3 controller.py --batch-size 100000   # interactive pipeline selection

Options:
    --pipeline    hive | mapreduce | pig | mongodb
    --batch-size  Number of records per batch (default: 100000)
    --db-name     PostgreSQL database name (default: etldb)
    --db-user     PostgreSQL user (default: postgres)
    --db-password PostgreSQL password (default: empty)
    --db-host     PostgreSQL host (default: localhost)
    --output-dir  Base directory for pipeline output (default: /tmp/etl_output)
"""

import argparse
import os
import subprocess
import sys
import time
import psycopg2
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ----------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description='Multi-Pipeline ETL Framework – DAS 839'
    )
    p.add_argument('--pipeline',
                   choices=['hive', 'mapreduce', 'pig', 'mongodb'],
                   help='Execution backend to use')
    p.add_argument('--batch-size',  type=int, default=100000,
                   help='Records per batch')
    p.add_argument('--db-host',     default='')          # empty = Unix socket (peer auth)
    p.add_argument('--db-port',     type=int, default=5432)
    p.add_argument('--db-name',     default='etldb')
    p.add_argument('--db-user',     default='vishesh')
    p.add_argument('--db-password', default='')
    p.add_argument('--output-dir',  default='/tmp/etl_output')
    return p.parse_args()


def select_pipeline_interactive():
    options = ['hive', 'mapreduce', 'pig', 'mongodb']
    print("\nSelect one pipeline:")
    for idx, name in enumerate(options, 1):
        print(f"  {idx}. {name}")
    while True:
        choice = input("Enter choice [1-4]: ").strip()
        if choice.isdigit():
            num = int(choice)
            if 1 <= num <= len(options):
                return options[num - 1]
        print("Invalid choice. Please enter 1, 2, 3, or 4.")


def db_connect(args):
    # Use Unix socket when host is empty (peer authentication, no password needed)
    kw = dict(dbname=args.db_name, user=args.db_user)
    if args.db_host:
        kw['host'] = args.db_host
        kw['port'] = args.db_port
        if args.db_password:
            kw['password'] = args.db_password
    return psycopg2.connect(**kw)


def create_run(conn, pipeline, batch_size):
    """Insert a run_metadata row and return the new run_id."""
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO run_metadata
          (pipeline_name, run_timestamp, batch_size,
           total_records, malformed_count, num_batches,
           avg_batch_size, runtime_ms)
        VALUES (%s, %s, %s, 0, 0, 0, 0, 0)
        RETURNING run_id
    """, (pipeline, datetime.now(), batch_size))
    run_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    return run_id


def run_pipeline(args, run_id):
    """Invoke the shell script for the chosen pipeline."""
    pipeline = args.pipeline
    script = os.path.join(BASE_DIR, 'pipelines', pipeline, f'run_{pipeline}.sh')
    output_dir = args.output_dir
    cmd = ['bash', script, str(args.batch_size), str(run_id), output_dir]

    print(f"\n>>> Launching: {' '.join(cmd)}\n")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"\n[ERROR] Pipeline '{pipeline}' exited with code {result.returncode}")
        sys.exit(result.returncode)

    return os.path.join(output_dir, f'run_{run_id}')


def load_results(args, run_id, output_dir):
    """Call result_loader.py to push results into PostgreSQL."""
    loader = os.path.join(BASE_DIR, 'common', 'result_loader.py')
    cmd = [
        sys.executable, loader,
        '--pipeline',    args.pipeline,
        '--run-id',      str(run_id),
        '--batch-size',  str(args.batch_size),
        '--output-dir',  output_dir,
        '--db-host',     args.db_host,
        '--db-port',     str(args.db_port),
        '--db-name',     args.db_name,
        '--db-user',     args.db_user,
        '--db-password', args.db_password,
    ]
    print(f"\n>>> Launching: {' '.join(cmd)}\n")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"\n[ERROR] result_loader.py failed.")
        sys.exit(result.returncode)


def export_csv_results(args, run_id):
    """Call export_results.py to export query results to CSV files."""
    exporter = os.path.join(BASE_DIR, 'common', 'export_results.py')
    cmd = [
        sys.executable, exporter,
        '--run-id',      str(run_id),
        '--db-host',     args.db_host,
        '--db-port',     str(args.db_port),
        '--db-name',     args.db_name,
        '--db-user',     args.db_user,
        '--db-password', args.db_password,
    ]
    print(f"\n>>> Launching: {' '.join(cmd)}\n")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print(f"\n[WARNING] export_results.py had issues, but continuing.")


def show_report(args, run_id):
    reporter = os.path.join(BASE_DIR, 'common', 'reporter.py')
    cmd = [
        sys.executable, reporter,
        '--run-id',      str(run_id),
        '--db-host',     args.db_host,
        '--db-port',     str(args.db_port),
        '--db-name',     args.db_name,
        '--db-user',     args.db_user,
        '--db-password', args.db_password,
    ]
    subprocess.run(cmd)


def update_runtime(conn, run_id, runtime_ms):
    """Persist final runtime (includes pipeline + DB load)."""
    cur = conn.cursor()
    cur.execute("UPDATE run_metadata SET runtime_ms = %s WHERE run_id = %s",
                (runtime_ms, run_id))
    conn.commit()
    cur.close()


# ----------------------------------------------------------------
def main():
    args = parse_args()
    if not args.pipeline:
        args.pipeline = select_pipeline_interactive()

    print("=" * 60)
    print(f"  DAS 839 – Multi-Pipeline ETL Framework")
    print(f"  Pipeline   : {args.pipeline.upper()}")
    print(f"  Batch Size : {args.batch_size:,}")
    print("=" * 60)

    conn = db_connect(args)
    run_id = create_run(conn, args.pipeline, args.batch_size)
    conn.close()
    print(f"\n  Run ID     : {run_id}")

    # ---- RUNTIME starts here (reading input → writing to DB) ----
    t_start = time.time()

    output_dir = run_pipeline(args, run_id)

    load_results(args, run_id, output_dir)

    # ---- RUNTIME ends when DB write is complete ----------------
    t_end = time.time()
    runtime_ms = int((t_end - t_start) * 1000)

    conn = db_connect(args)
    update_runtime(conn, run_id, runtime_ms)
    conn.close()

    print(f"\n  Total runtime: {runtime_ms:,} ms  ({runtime_ms/1000:.1f} s)")

    # ---- Export results to CSV files (excluded from runtime) ----
    print("\n  Exporting results to CSV files...\n")
    export_csv_results(args, run_id)

    # ---- Report (excluded from runtime) ------------------------
    print("\n  Generating report...\n")
    show_report(args, run_id)


if __name__ == '__main__':
    main()

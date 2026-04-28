#!/usr/bin/env python3
"""
export_results.py
Exports PostgreSQL query results to CSV files in the output folder.
Called after each pipeline run to create persistent, readable output files.

Usage:
    python3 export_results.py --run-id 15 --db-name etldb --db-user postgres
"""

import argparse
import os
import csv
import psycopg2

# ----------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description='Export Hive pipeline results to CSV files'
    )
    p.add_argument('--run-id',      type=int, required=True)
    p.add_argument('--db-host',     default='')
    p.add_argument('--db-port',     type=int, default=5432)
    p.add_argument('--db-name',     default='etldb')
    p.add_argument('--db-user',     default='vishesh')
    p.add_argument('--db-password', default='')
    p.add_argument('--output-dir',  default='output')
    return p.parse_args()


def db_connect(args):
    kw = dict(dbname=args.db_name, user=args.db_user)
    if args.db_host:
        kw['host'] = args.db_host
        kw['port'] = args.db_port
        if args.db_password:
            kw['password'] = args.db_password
    return psycopg2.connect(**kw)


def export_query_to_csv(conn, run_id, table_name, output_file):
    """Export a specific query result table to CSV."""
    cur = conn.cursor()
    
    # Get column names
    cur.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    columns = [row[0] for row in cur.fetchall()]
    
    # Fetch all rows for this run_id
    placeholders = ','.join(['%s'] * len(columns))
    cur.execute(f"SELECT {','.join(columns)} FROM {table_name} WHERE run_id = %s ORDER BY batch_id", (run_id,))
    rows = cur.fetchall()
    
    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)  # header
        writer.writerows(rows)
    
    cur.close()
    print(f"✓ Exported {len(rows)} rows to {output_file}")


def export_metadata_to_csv(conn, run_id, output_file):
    """Export run metadata to CSV."""
    cur = conn.cursor()
    
    # Get column names
    cur.execute("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'run_metadata'
        ORDER BY ordinal_position
    """)
    columns = [row[0] for row in cur.fetchall()]
    
    # Fetch metadata for this run
    cur.execute(f"SELECT {','.join(columns)} FROM run_metadata WHERE run_id = %s", (run_id,))
    rows = cur.fetchall()
    
    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(columns)  # header
        writer.writerows(rows)
    
    cur.close()
    print(f"✓ Exported metadata to {output_file}")


# ----------------------------------------------------------------
def main():
    args = parse_args()
    
    # Create output folder structure
    run_folder = os.path.join(args.output_dir, 'hive', f'run_{args.run_id}')
    os.makedirs(run_folder, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"Exporting Results for Run {args.run_id}")
    print(f"Output Folder: {run_folder}")
    print(f"{'='*60}\n")
    
    conn = db_connect(args)
    
    # Export metadata
    metadata_file = os.path.join(run_folder, '00_run_metadata.csv')
    export_metadata_to_csv(conn, args.run_id, metadata_file)
    
    # Export Q1
    q1_file = os.path.join(run_folder, '01_q1_daily_traffic.csv')
    export_query_to_csv(conn, args.run_id, 'q1_daily_traffic', q1_file)
    
    # Export Q2
    q2_file = os.path.join(run_folder, '02_q2_top_resources.csv')
    export_query_to_csv(conn, args.run_id, 'q2_top_resources', q2_file)
    
    # Export Q3
    q3_file = os.path.join(run_folder, '03_q3_hourly_errors.csv')
    export_query_to_csv(conn, args.run_id, 'q3_hourly_errors', q3_file)
    
    conn.close()
    
    print(f"\n{'='*60}")
    print(f"✓ All results exported successfully!")
    print(f"View results: ls -la {run_folder}")
    print(f"{'='*60}\n")


if __name__ == '__main__':
    main()

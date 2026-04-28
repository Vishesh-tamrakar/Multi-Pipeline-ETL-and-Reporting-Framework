#!/usr/bin/env python3
import os
import sys
import gzip
import time
import shutil
import argparse
import subprocess
import psycopg2
from psycopg2.extras import execute_values

def get_args():
    parser = argparse.ArgumentParser(description="MapReduce ETL Orchestrator")
    parser.add_argument("--batch-size", type=int, default=100000, help="Number of records per batch")
    parser.add_argument("--dbhost", default="/tmp", help="PostgreSQL host")
    parser.add_argument("--dbport", default="5433", help="PostgreSQL port")
    parser.add_argument("--dbuser", default="nosql_user", help="PostgreSQL user")
    parser.add_argument("--dbpass", default="nosql_pass", help="PostgreSQL password")
    parser.add_argument("--dbname", default="nosql_db", help="PostgreSQL database")
    parser.add_argument("--data-dir", default="/home/nivedita/nosql_endterm_project/data", help="Data directory")
    return parser.parse_args()

def setup_db(args):
    try:
        conn = psycopg2.connect(
            host=args.dbhost,
            port=args.dbport,
            user=args.dbuser,
            password=args.dbpass,
            dbname=args.dbname
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            with open("schema.sql", "r") as f:
                cur.execute(f.read())
        return conn
    except Exception as e:
        print(f"Warning: Could not connect to PostgreSQL: {e}")
        print("Pipeline will still run, but results will not be saved to DB.")
        return None

def run_mapreduce(input_file, output_dir):
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
        
    streaming_jar = "/home/nivedita/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.4.3.jar"
    mapper_path = os.path.abspath("mapper.py")
    reducer_path = os.path.abspath("reducer.py")
    
    cmd = [
        "hadoop", "jar", streaming_jar,
        "-D", "mapreduce.job.reduces=1",
        "-D", "fs.defaultFS=file:///",
        "-D", "mapreduce.framework.name=local",
        "-input", f"file://{os.path.abspath(input_file)}",
        "-output", f"file://{os.path.abspath(output_dir)}",
        "-mapper", f"{sys.executable} {mapper_path}",
        "-reducer", f"{sys.executable} {reducer_path}"
    ]
    
    print(f"Running MapReduce on {input_file}...")
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0:
        print(f"MapReduce failed:\n{result.stderr}")
        return False
    return True

def process_results(output_dir, conn, run_id):
    q1_data = []
    q2_data = []
    q3_data = []
    
    part_file = os.path.join(output_dir, "part-00000")
    if not os.path.exists(part_file):
        print("No output file found from MapReduce.")
        return
        
    with open(part_file, "r") as f:
        for line in f:
            parts = line.strip().split("\t")
            if not parts: continue
            
            if parts[0] == "Q1":
                # Q1 date status count bytes
                q1_data.append((run_id, parts[1], int(parts[2]), int(parts[3]), int(parts[4])))
            elif parts[0] == "Q2":
                # Q2 resource count bytes distinct_hosts
                q2_data.append((run_id, parts[1], int(parts[2]), int(parts[3]), int(parts[4])))
            elif parts[0] == "Q3":
                # Q3 date hour error_count total_count error_rate distinct_hosts
                q3_data.append((run_id, parts[1], int(parts[2]), int(parts[3]), int(parts[4]), float(parts[5]), int(parts[6])))
    
    # Sort Q2 and keep top 20
    q2_data = sorted(q2_data, key=lambda x: x[2], reverse=True)[:20]
    
    if conn:
        with conn.cursor() as cur:
            if q1_data:
                execute_values(cur, "INSERT INTO q1_daily_traffic (run_id, log_date, status_code, request_count, total_bytes) VALUES %s", q1_data)
            if q2_data:
                execute_values(cur, "INSERT INTO q2_top_resources (run_id, resource_path, request_count, total_bytes, distinct_host_count) VALUES %s", q2_data)
            if q3_data:
                execute_values(cur, "INSERT INTO q3_hourly_errors (run_id, log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts) VALUES %s", q3_data)
                
    return len(q1_data), len(q2_data), len(q3_data)

def main():
    args = get_args()
    conn = setup_db(args)
    
    # Process files
    log_files = [os.path.join(args.data_dir, f) for f in os.listdir(args.data_dir) if f.endswith(".gz")]
    if not log_files:
        print("No .gz files found in data directory.")
        return
        
    start_time = time.time()
    batch_id = 1
    total_records = 0
    
    os.chmod("mapper.py", 0o755)
    os.chmod("reducer.py", 0o755)
    
    for log_file in log_files:
        print(f"Processing {log_file}...")
        with gzip.open(log_file, "rt", encoding="latin-1") as f:
            batch_lines = []
            for line in f:
                batch_lines.append(line)
                if len(batch_lines) >= args.batch_size:
                    # Write batch to temp file
                    temp_input = f"temp_batch_{batch_id}.txt"
                    with open(temp_input, "w") as out_f:
                        out_f.writelines(batch_lines)
                        
                    # Run MR
                    out_dir = f"temp_output_{batch_id}"
                    if run_mapreduce(temp_input, out_dir):
                        # Log run
                        if conn:
                            with conn.cursor() as cur:
                                cur.execute("""
                                    INSERT INTO runs_metadata 
                                    (pipeline_name, batch_id, batch_size, runtime_seconds) 
                                    VALUES (%s, %s, %s, %s) RETURNING run_id
                                """, ("MapReduce", batch_id, len(batch_lines), 0))
                                run_id = cur.fetchone()[0]
                        else:
                            run_id = batch_id
                            
                        process_results(out_dir, conn, run_id)
                        
                    total_records += len(batch_lines)
                    batch_id += 1
                    batch_lines = []
                    
                    # Cleanup
                    if os.path.exists(temp_input): os.remove(temp_input)
                    if os.path.exists(out_dir): shutil.rmtree(out_dir)
                    
            # Process remaining
            if batch_lines:
                temp_input = f"temp_batch_{batch_id}.txt"
                with open(temp_input, "w") as out_f:
                    out_f.writelines(batch_lines)
                out_dir = f"temp_output_{batch_id}"
                if run_mapreduce(temp_input, out_dir):
                    if conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO runs_metadata 
                                (pipeline_name, batch_id, batch_size, runtime_seconds) 
                                VALUES (%s, %s, %s, %s) RETURNING run_id
                            """, ("MapReduce", batch_id, len(batch_lines), 0))
                            run_id = cur.fetchone()[0]
                    else:
                        run_id = batch_id
                    process_results(out_dir, conn, run_id)
                total_records += len(batch_lines)
                batch_id += 1
                if os.path.exists(temp_input): os.remove(temp_input)
                if os.path.exists(out_dir): shutil.rmtree(out_dir)

    end_time = time.time()
    runtime = end_time - start_time
    num_batches = batch_id - 1
    avg_batch_size = total_records / num_batches if num_batches > 0 else 0
    
    if conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE runs_metadata 
                SET avg_batch_size = %s, runtime_seconds = %s 
                WHERE pipeline_name = 'MapReduce'
            """, (avg_batch_size, runtime))
            
    # Reporting output
    report = []
    report.append("=" * 50)
    report.append("MapReduce Pipeline Execution Complete")
    report.append("=" * 50)
    report.append(f"Pipeline: MapReduce")
    report.append(f"Total Records Processed: {total_records}")
    report.append(f"Number of Batches: {num_batches}")
    report.append(f"Average Batch Size: {avg_batch_size:.2f}")
    report.append(f"Total Runtime (seconds): {runtime:.2f}")
    report.append("=" * 50)
    
    report_text = "\n".join(report)
    print(report_text)
    
    with open("reporting_output.txt", "w") as f:
        f.write(report_text)
    print("\nReporting output has been saved to reporting_output.txt")
    
    # Generate reporting_module.txt with SQL outputs
    print("Generating reporting_module.txt with SQL query outputs...")
    psql_cmd = [
        "psql", "-p", str(args.dbport), "-h", args.dbhost, "-d", args.dbname, "-U", args.dbuser,
        "-c", "\\echo === QUERY 1: Daily Traffic Summary ===",
        "-c", "SELECT * FROM q1_daily_traffic LIMIT 20;",
        "-c", "\\echo === QUERY 2: Top Requested Resources ===",
        "-c", "SELECT * FROM q2_top_resources LIMIT 20;",
        "-c", "\\echo === QUERY 3: Hourly Error Analysis ===",
        "-c", "SELECT * FROM q3_hourly_errors LIMIT 20;"
    ]
    try:
        with open("reporting_module.txt", "w") as f:
            subprocess.run(psql_cmd, stdout=f, stderr=subprocess.PIPE, text=True)
        print("Reporting module queries have been saved to reporting_module.txt")
    except Exception as e:
        print(f"Warning: Could not automatically generate reporting_module.txt: {e}")
    
if __name__ == "__main__":
    main()

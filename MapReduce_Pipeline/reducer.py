#!/usr/bin/env python3
import sys

current_key = None
q1_count = 0
q1_bytes = 0

q2_count = 0
q2_bytes = 0
q2_hosts = set()

q3_total_count = 0
q3_error_count = 0
q3_error_hosts = set()

malformed_count = 0

def emit():
    global current_key, q1_count, q1_bytes, q2_count, q2_bytes, q2_hosts
    global q3_total_count, q3_error_count, q3_error_hosts, malformed_count
    
    if current_key:
        if current_key == "MALFORMED":
            print(f"MALFORMED\t{malformed_count}")
        elif current_key.startswith("Q1|"):
            _, log_date, status = current_key.split("|")
            print(f"Q1\t{log_date}\t{status}\t{q1_count}\t{q1_bytes}")
        elif current_key.startswith("Q2|"):
            resource = current_key.split("|", 1)[1]
            print(f"Q2\t{resource}\t{q2_count}\t{q2_bytes}\t{len(q2_hosts)}")
        elif current_key.startswith("Q3|"):
            _, log_date, log_hour = current_key.split("|")
            error_rate = q3_error_count / q3_total_count if q3_total_count > 0 else 0
            print(f"Q3\t{log_date}\t{log_hour}\t{q3_error_count}\t{q3_total_count}\t{error_rate:.4f}\t{len(q3_error_hosts)}")
            
    # Reset
    q1_count = 0
    q1_bytes = 0
    q2_count = 0
    q2_bytes = 0
    q2_hosts.clear()
    q3_total_count = 0
    q3_error_count = 0
    q3_error_hosts.clear()
    malformed_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line: continue
    
    try:
        key, value = line.split('\t', 1)
    except ValueError:
        continue
        
    if key != current_key:
        if current_key is not None:
            emit()
        current_key = key
        
    if key == "MALFORMED":
        malformed_count += 1
    elif key.startswith("Q1|"):
        try:
            cnt_str, bytes_str = value.split("|")
            q1_count += int(cnt_str)
            q1_bytes += int(bytes_str)
        except ValueError:
            pass
    elif key.startswith("Q2|"):
        try:
            cnt_str, bytes_str, host = value.split("|")
            q2_count += int(cnt_str)
            q2_bytes += int(bytes_str)
            q2_hosts.add(host)
        except ValueError:
            pass
    elif key.startswith("Q3|"):
        try:
            total_str, is_error_str, host = value.split("|")
            q3_total_count += int(total_str)
            is_error = int(is_error_str)
            if is_error == 1:
                q3_error_count += 1
                q3_error_hosts.add(host)
        except ValueError:
            pass

# Output the last key
emit()

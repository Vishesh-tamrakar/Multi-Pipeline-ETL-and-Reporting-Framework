#!/usr/bin/env python3
import sys
import re
from datetime import datetime

# Regex for standard Apache/NASA log format
# Example: 199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245
LOG_PATTERN = re.compile(r'^(?P<host>\S+)\s+\S+\s+\S+\s+\[(?P<timestamp>[^\]]+)\]\s+"(?P<request>[^"]*)"\s+(?P<status>\d{3})\s+(?P<bytes>\S+)')

for line in sys.stdin:
    match = LOG_PATTERN.match(line)
    if not match:
        # We can track malformed records by emitting a special key
        print("MALFORMED\t1")
        continue

    d = match.groupdict()
    host = d['host']
    
    # Parse timestamp e.g. 01/Jul/1995:00:00:01 -0400
    ts_str = d['timestamp'].split()[0] # ignore timezone
    try:
        dt = datetime.strptime(ts_str, "%d/%b/%Y:%H:%M:%S")
        log_date = dt.strftime("%Y-%m-%d")
        log_hour = dt.strftime("%H")
    except ValueError:
        print("MALFORMED\t1")
        continue

    status = d['status']
    
    bytes_str = d['bytes']
    if bytes_str == '-':
        bytes_transferred = 0
    else:
        try:
            bytes_transferred = int(bytes_str)
        except ValueError:
            bytes_transferred = 0

    request_str = d['request']
    req_parts = request_str.split()
    if len(req_parts) >= 2:
        method = req_parts[0]
        resource = req_parts[1]
        protocol = req_parts[2] if len(req_parts) >= 3 else ""
    else:
        # Can't parse request well
        method = request_str
        resource = request_str
        protocol = ""

    # Emitting for Q1: Daily Traffic Summary (log_date, status_code)
    # Value: 1 (count) | bytes
    print(f"Q1|{log_date}|{status}\t1|{bytes_transferred}")

    # Emitting for Q2: Top Requested Resources (resource)
    # Value: 1 (count) | bytes | host
    print(f"Q2|{resource}\t1|{bytes_transferred}|{host}")

    # Emitting for Q3: Hourly Error Analysis (log_date, log_hour)
    # Error definition: 400 <= status <= 599
    try:
        status_code = int(status)
    except ValueError:
        status_code = 0
    
    is_error = 1 if 400 <= status_code <= 599 else 0
    # Value: 1 (total) | is_error | host
    print(f"Q3|{log_date}|{log_hour}\t1|{is_error}|{host}")

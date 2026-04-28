#!/bin/bash

export PGDATA=/home/nivedita/nosql_endterm_project/local_pg_data
export PATH=/usr/lib/postgresql/14/bin:$PATH

echo "Initializing local PostgreSQL database cluster..."
if [ ! -d "$PGDATA" ]; then
    initdb -D "$PGDATA"
    # Change port to avoid conflict with system postgres
    echo "port = 5433" >> "$PGDATA/postgresql.conf"
    echo "unix_socket_directories = '/tmp'" >> "$PGDATA/postgresql.conf"
fi

echo "Starting local PostgreSQL server..."
pg_ctl -D "$PGDATA" -l "$PGDATA/server.log" start

echo "Waiting for server to start..."
sleep 3

echo "Creating user and database..."
psql -p 5433 -d postgres -c "CREATE ROLE nosql_user WITH SUPERUSER LOGIN PASSWORD 'nosql_pass';"
psql -p 5433 -d postgres -c "CREATE DATABASE nosql_db OWNER nosql_user;"

echo "PostgreSQL setup complete. Database is running on port 5433."

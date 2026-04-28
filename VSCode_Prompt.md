# Context & Objective
We are building a Multi-Pipeline ETL and Reporting Framework for Web Server Log Analytics (End Semester Project). We have completed the architecture plan and have written the first pipeline using Apache Hive. 

The project has a strict rule: we must process the raw NASA web server logs (`NASA_access_log_Jul95.gz`, etc.) directly from HDFS or local mode, compute 3 specific queries using batch-wise aggregation (batch size of 100,000 using window functions like `ROW_NUMBER()`), and load the final results into a PostgreSQL database.

# Current State
1. **Architecture & Orchestration**: A python `controller.py` script orchestrates the pipeline execution and passes arguments to the backend runner scripts.
2. **PostgreSQL Loading**: A Python script `common/result_loader.py` correctly reads the TSV outputs and loads them into PostgreSQL (`etldb`).
3. **Hive Pipeline (`pipelines/hive/`)**: We have fully written `01_create_tables.hql`, `02_queries.hql`, and `run_hive.sh`. 
4. **Issue**: We are stuck executing `run_hive.sh`. We are using Hive 4.2.0, which heavily relies on the Tez execution engine.

# The Error We Are Stuck On
When we run `python3 controller.py --pipeline hive --batch-size 100000`, the scripts pipe to `beeline` (using an embedded `jdbc:hive2://` connection to avoid standalone HS2 TTY issues).

However, the MapReduce/Tez job crashes with:
```
org.apache.tez.dag.api.TezUncheckedException: Invalid configuration of tez jars, tez.lib.uris is not defined in the configuration
```

**What we have tried:**
1. We tried forcing MapReduce using `--hiveconf hive.execution.engine=mr` and `--hiveconf mapreduce.framework.name=local`. This worked temporarily in local tests but Hive still randomly forces a `TezTask` which crashes.
2. We found Tez jars at `/home/vishesh/Tutorial/pig-0.18.0/lib/h3/*` and added them to `HADOOP_CLASSPATH`, which fixed a previous `ClassNotFoundException: org.apache.tez.mapreduce.hadoop.InputSplitInfo`.

# Your Task
Please read `Phase1_Architecture_Report.pdf` (or `.tex`), `Phase1_Architecture_Plan.md`, and the `pipelines/hive/run_hive.sh` script to understand the current setup.

Then, fix the execution of the Hive pipeline. Specifically, resolve the `tez.lib.uris is not defined` error. 
Since this is a single node environment and we just need to process 3.4M records quickly:
- You should try configuring Tez to run in local mode by passing `--hiveconf tez.local.mode=true` and setting `--hiveconf tez.lib.uris=file:///home/vishesh/Tutorial/pig-0.18.0/lib/h3/` to see if that allows the embedded Tez planner to succeed.
- Or, if there is a foolproof way to make Hive 4 completely bypass Tez and use `mr` local mode for the window function queries, do that.

Continue debugging until `run_hive.sh` executes successfully end-to-end and the data is inserted into PostgreSQL.

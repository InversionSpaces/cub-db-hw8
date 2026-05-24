#!/usr/bin/env python3

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
import requests
from clickhouse_driver import Client

DATA_DIR = Path("data")
CONTAINER_NAME_PREFIX = "nyc-taxi-benchmark"
CONTAINER_RUNTIME = "podman"

DATA_URLS = [
    f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-{m:02d}.parquet"
    for m in range(1, 13)
]

CONTAINER_CPUS = 8.0
CONTAINER_MEM = "16g"

POSTGRES_CONFIG = {
    "image": "postgres:16-alpine",
    "port": 5432,
    "env": {
        "POSTGRES_USER": "taxi",
        "POSTGRES_PASSWORD": "benchmark",
        "POSTGRES_DB": "nyc_taxi",
    },
    "health_check": "pg_isready -U taxi -d nyc_taxi",
    "cmd_args": [
        "-c", "shared_buffers=4GB",
        "-c", "work_mem=256MB",
        "-c", "effective_cache_size=12GB",
        "-c", "max_parallel_workers_per_gather=8",
        "-c", "max_worker_processes=8",
        "-c", "max_parallel_workers=8",
        "-c", "maintenance_work_mem=1GB",
        "-c", "jit=off",
    ],
}

CLICKHOUSE_CONFIG = {
    "image": "clickhouse/clickhouse-server:24.3-alpine",
    "port": 9000,
    "http_port": 8123,
    "env": {
        "CLICKHOUSE_USER": "default",
        "CLICKHOUSE_PASSWORD": "benchmark",
        "CLICKHOUSE_DB": "nyc_taxi",
    },
    "health_check": "wget --spider -q http://localhost:8123/ping",
}

POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS yellow_taxi_2025 (
    id BIGSERIAL,
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMPTZ,
    tpep_dropoff_datetime TIMESTAMPTZ,
    passenger_count FLOAT,
    trip_distance FLOAT,
    RatecodeID FLOAT,
    store_and_fwd_flag VARCHAR(1),
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type BIGINT,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    Airport_fee FLOAT,
    cbd_congestion_fee FLOAT
);
"""

CLICKHOUSE_SCHEMA = """
CREATE TABLE IF NOT EXISTS yellow_taxi_2025 (
    VendorID Int32,
    tpep_pickup_datetime DateTime64(6, 'America/New_York'),
    tpep_dropoff_datetime DateTime64(6, 'America/New_York'),
    passenger_count Float64,
    trip_distance Float64,
    RatecodeID Float64,
    store_and_fwd_flag String,
    PULocationID Int32,
    DOLocationID Int32,
    payment_type Int64,
    fare_amount Float64,
    extra Float64,
    mta_tax Float64,
    tip_amount Float64,
    tolls_amount Float64,
    improvement_surcharge Float64,
    total_amount Float64,
    congestion_surcharge Float64,
    Airport_fee Float64,
    cbd_congestion_fee Float64,

    INDEX idx_distance_amount (trip_distance, total_amount)
    TYPE minmax
    GRANULARITY 4
)
ENGINE = MergeTree()
ORDER BY (PULocationID, toHour(tpep_pickup_datetime));
"""

BENCHMARK_QUERY = """
SELECT
    PULocationID,
    EXTRACT(HOUR FROM tpep_pickup_datetime) as hour,
    AVG(fare_amount) as avg_fare,
    AVG(tip_amount) as avg_tip,
    SUM(total_amount) as revenue,
    COUNT(*) as trips
FROM yellow_taxi_2025
WHERE trip_distance BETWEEN 1 AND 50
  AND total_amount > 0
GROUP BY PULocationID, hour
ORDER BY revenue DESC;
"""


def log_section(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def log_step(message):
    print(f"  --> {message}")


def log_query(query):
    lines = query.strip().split("\n")
    print(f"  Query: {lines[0]}")
    if len(lines) > 1:
        for line in lines[1:]:
            print(f"         {line}")


def log_sql(stmt):
    print(f"  SQL: {stmt}")


def container_run(container_name, image, ports, env, volumes=None, cpus=None, mem=None, cmd_args=None):
    cmd = [
        CONTAINER_RUNTIME, "run", "-d", "--rm",
        "--name", container_name,
        "-p", ports,
    ]
    
    for k, v in env.items():
        cmd.extend(["-e", f"{k}={v}"])
    
    if volumes:
        for v in volumes:
            cmd.extend(["-v", v])
    
    if cpus:
        cmd.extend(["--cpus", str(cpus)])
    
    if mem:
        cmd.extend(["--memory", mem])
    
    cmd.append(image)
    
    if cmd_args:
        cmd.extend(cmd_args)
    
    print(f"  CMD: {CONTAINER_RUNTIME} run -d --rm --name {container_name} -p {ports} --cpus {cpus} --memory {mem} {image}")
    for k, v in env.items():
        print(f"       -e {k}={v}")
    if cmd_args:
        print(f"       {' '.join(cmd_args)}")
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr}")
        return False
    
    container_id = result.stdout.strip()
    print(f"  Container started: {container_name} ({container_id[:12]}...)")
    return True


def container_stop(container_name):
    print(f"  Stopping container: {container_name}")
    result = subprocess.run([CONTAINER_RUNTIME, "stop", container_name], capture_output=True, text=True)
    print(f"  Stop output: {result.stdout.strip()}")
    
    print(f"  Removing container: {container_name}")
    result = subprocess.run([CONTAINER_RUNTIME, "rm", "-f", container_name], capture_output=True, text=True)
    print(f"  Remove output: {result.stdout.strip()}")


def wait_for_health(container_name, health_check, max_retries=30, interval=2):
    print(f"  Waiting for container health check: {health_check}")
    for i in range(max_retries):
        if "pg_isready" in health_check:
            result = subprocess.run(
                [CONTAINER_RUNTIME, "exec", container_name, "sh", "-c", health_check],
                capture_output=True, text=True
            )
        elif "wget" in health_check:
            result = subprocess.run(
                [CONTAINER_RUNTIME, "exec", container_name, "sh", "-c", health_check],
                capture_output=True, text=True
            )
        else:
            result = subprocess.run(
                [CONTAINER_RUNTIME, "inspect", "--format={{.State.Health.Status}}", container_name],
                capture_output=True, text=True
            )
        
        if result.returncode == 0:
            print(f"  Container healthy after {i+1} checks")
            return True
        
        if i % 5 == 0:
            print(f"  Still waiting... (attempt {i+1}/{max_retries})")
        time.sleep(interval)
    
    print(f"  WARNING: Container not healthy after {max_retries} checks")
    return False


def dump_postgres_settings(container_name):
    log_section("PostgreSQL: Effective Settings")
    settings = [
        "shared_buffers",
        "work_mem",
        "maintenance_work_mem",
        "effective_cache_size",
        "max_parallel_workers_per_gather",
        "max_worker_processes",
        "max_parallel_workers",
        "jit",
        "TimeZone",
    ]
    show_sql = "; ".join(f"SHOW {s}" for s in settings) + ";"
    cmd = [
        CONTAINER_RUNTIME, "exec", container_name,
        "psql", "-U", "taxi", "-d", "nyc_taxi", "-A", "-t", "-c", show_sql,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr}")
        return
    values = [line for line in result.stdout.strip().split("\n") if line]
    for name, value in zip(settings, values):
        print(f"  {name:<35s} = {value}")


def create_postgres_schema(container_name):
    log_section("Creating PostgreSQL Schema")

    tz_stmt = "ALTER DATABASE nyc_taxi SET timezone TO 'America/New_York'"
    log_sql(tz_stmt)
    result = subprocess.run(
        [CONTAINER_RUNTIME, "exec", container_name,
         "psql", "-U", "taxi", "-d", "nyc_taxi", "-c", tz_stmt],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print(f"  ERROR: {result.stderr}")
    else:
        print(f"  OK")

    for stmt in POSTGRES_SCHEMA.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            log_sql(stmt)
            cmd = [
                CONTAINER_RUNTIME, "exec", container_name,
                "psql", "-U", "taxi", "-d", "nyc_taxi", "-c", stmt
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                print(f"  ERROR: {result.stderr}")
            else:
                print(f"  OK")


def create_clickhouse_schema(container_name):
    log_section("Creating ClickHouse Schema")
    
    log_query(CLICKHOUSE_SCHEMA)
    client = Client(
        host='localhost',
        port=9000,
        database='nyc_taxi',
        user='default',
        password='benchmark'
    )
    client.execute(CLICKHOUSE_SCHEMA)
    print(f"  OK")


def prepare_csv():
    combined_csv = DATA_DIR / "taxi_all_2025.csv"

    if combined_csv.exists():
        print(f"  Using existing CSV: {combined_csv}")
        return combined_csv

    parquet_files = sorted(DATA_DIR.glob("yellow_tripdata_2025-*.parquet"))
    if not parquet_files:
        print("  ERROR: No parquet files found in data/")
        return None

    print(f"  Converting {len(parquet_files)} parquet files to CSV: {combined_csv}")
    first_file = True
    for pf in parquet_files:
        print(f"    Processing: {pf.name}")
        df = pd.read_parquet(pf)
        df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df.to_csv(combined_csv, index=False, header=first_file, mode='a', na_rep='\\N')
        first_file = False

    return combined_csv


def load_data_postgres(container_name):
    log_section("Loading Data into PostgreSQL")

    csv_path = prepare_csv()
    if csv_path is None:
        return False

    print(f"  Copying CSV to container")
    subprocess.run([CONTAINER_RUNTIME, "cp", str(csv_path), f"{container_name}:/tmp/data.csv"])

    with open(csv_path) as f:
        columns = f.readline().strip()
    copy_cmd = f"COPY yellow_taxi_2025 ({columns}) FROM '/tmp/data.csv' WITH (FORMAT CSV, DELIMITER ',', HEADER, NULL '\\N')"
    log_query(copy_cmd)
    result = subprocess.run(
        [CONTAINER_RUNTIME, "exec", container_name, "sh", "-c",
         f"psql -U taxi -d nyc_taxi -c \"{copy_cmd}\""],
        capture_output=True, text=True
    )

    if result.returncode != 0 or "ERROR" in result.stdout:
        print(f"  ERROR: {result.stderr or result.stdout}")
        return False

    print(f"  Data load complete")

    print(f"  Running ANALYZE...")
    analyze_cmd = "ANALYZE yellow_taxi_2025"
    log_sql(analyze_cmd)
    result = subprocess.run(
        [CONTAINER_RUNTIME, "exec", container_name, "sh", "-c",
         f"psql -U taxi -d nyc_taxi -c \"{analyze_cmd}\""],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  ERROR running ANALYZE: {result.stderr}")
    else:
        print(f"  ANALYZE complete")

    print(f"  Creating index on trip_distance...")
    index_cmd = "CREATE INDEX idx_trip_distance ON yellow_taxi_2025(trip_distance)"
    log_sql(index_cmd)
    result = subprocess.run(
        [CONTAINER_RUNTIME, "exec", container_name, "sh", "-c",
         f"psql -U taxi -d nyc_taxi -c \"{index_cmd}\""],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  ERROR creating index: {result.stderr}")
    else:
        print(f"  Index created")

    return True


def load_data_clickhouse(container_name):
    log_section("Loading Data into ClickHouse")

    csv_path = prepare_csv()
    if csv_path is None:
        return False

    print(f"  Copying CSV to container")
    subprocess.run([CONTAINER_RUNTIME, "cp", str(csv_path), f"{container_name}:/tmp/data.csv"])

    import_cmd = f"clickhouse-client --user default --password benchmark -d nyc_taxi --query 'INSERT INTO yellow_taxi_2025 FORMAT CSVWithNames' < /tmp/data.csv"
    log_query(import_cmd)
    result = subprocess.run(
        [CONTAINER_RUNTIME, "exec", container_name, "sh", "-c", import_cmd],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        print(f"  ERROR: {result.stderr}")
        return False

    print(f"  Data load complete")
    return True


def download_data():
    log_section("Downloading Data Files")
    
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  Data directory: {DATA_DIR.absolute()}")
    
    for url in DATA_URLS:
        filename = Path(url).name
        filepath = DATA_DIR / filename
        
        if filepath.exists():
            print(f"  SKIP: {filename} (already exists, {filepath.stat().st_size / 1024 / 1024:.1f}MB)")
            continue
        
        print(f"\n  Downloading: {filename}")
        print(f"    From: {url}")
        
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            downloaded = 0
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if downloaded % (10 * 1024 * 1024) == 0:
                        print(f"    Downloaded: {downloaded / 1024 / 1024:.1f}MB...")
            
            size_mb = filepath.stat().st_size / 1024 / 1024
            print(f"    Saved: {filepath} ({size_mb:.1f}MB)")
        except Exception as e:
            print(f"    ERROR: {e}")
            return False
    
    return True


def run_postgres_benchmark(container_name):
    log_section("PostgreSQL: EXPLAIN")
    
    explain_query = f"EXPLAIN {BENCHMARK_QUERY}"
    log_query(explain_query)
    
    cmd = [
        CONTAINER_RUNTIME, "exec", container_name,
        "psql", "-U", "taxi", "-d", "nyc_taxi", "-c", explain_query
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"  EXPLAIN ANALYZE Results:")
        for line in result.stdout.strip().split('\n'):
            print(f"    {line}")
    else:
        print(f"  ERROR: {result.stderr}")
    
    log_section("PostgreSQL: Running Query")
    log_query(BENCHMARK_QUERY)
    
    cmd = [
        CONTAINER_RUNTIME, "exec", container_name,
        "psql", "-U", "taxi", "-d", "nyc_taxi",
        "-c", "\\timing on",
        "-c", BENCHMARK_QUERY
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode == 0:
        lines = result.stdout.strip().split('\n')
        for line in lines[:5]:
            print(f"    {line}")
        if len(lines) > 10:
            print(f"    ... ({len(lines)-10} more rows)")
        for line in lines[-5:]:
            print(f"    {line}")
    else:
        print(f"  ERROR: {result.stderr}")


def run_clickhouse_benchmark(container_name):
    log_section("ClickHouse: EXPLAIN ANALYZE")
    
    client = Client(
        host='localhost',
        port=9000,
        database='nyc_taxi',
        user='default',
        password='benchmark'
    )
    
    explain_query = f"EXPLAIN indexes = 1 {BENCHMARK_QUERY}"
    log_query(explain_query)
    
    try:
        result = client.execute(explain_query)
        print(f"  EXPLAIN Results:")
        for row in result:
            print(f"    {row[0]}")
    except Exception as e:
        print(f"  EXPLAIN ERROR: {e}")
    
    log_section("ClickHouse: Running Query")
    log_query(BENCHMARK_QUERY)
    
    start = time.time()
    try:
        result = client.execute(BENCHMARK_QUERY)
        elapsed = time.time() - start
        print(f"  Query completed in: {elapsed:.3f}s")
        print(f"  Rows returned: {len(result)}")
        if result:
            print(f"  First row: {result[0]}")
    except Exception as e:
        print(f"  ERROR: {e}")


def main():
    parser = argparse.ArgumentParser(description='NYC Yellow Taxi 2025 DB Benchmark')
    parser.add_argument('--db', choices=['postgres', 'clickhouse'], required=True)
    parser.add_argument('--only-load', action='store_true')
    parser.add_argument('--container-name', default=None)
    parser.add_argument('--no-cleanup', action='store_true')
    parser.add_argument('--download', action='store_true')
    args = parser.parse_args()
    
    db_type = args.db
    only_load = args.only_load
    custom_name = args.container_name
    no_cleanup = args.no_cleanup
    download = args.download
    
    suffix = custom_name if custom_name else f"{db_type}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    container_name = f"{CONTAINER_NAME_PREFIX}-{suffix}"
    
    log_section(f"Starting {db_type.upper()} Benchmark")
    print(f"  Container name: {container_name}")
    print(f"  Only load: {only_load}")
    print(f"  No cleanup: {no_cleanup}")
    print(f"  Download: {download}")
    
    parquet_files = list(DATA_DIR.glob("yellow_tripdata_2025-*.parquet"))
    if download or not parquet_files:
        log_step("Data files missing or --download specified, downloading...")
        if not download_data():
            sys.exit(1)
        parquet_files = list(DATA_DIR.glob("yellow_tripdata_2025-*.parquet"))
    
    if not parquet_files:
        print(f"  ERROR: No parquet files found in {DATA_DIR}")
        sys.exit(1)
    
    print(f"  Data files ready: {len(parquet_files)} files")
    total_data_size = sum(f.stat().st_size for f in parquet_files)
    print(f"  Total data size: {total_data_size / 1024 / 1024:.1f}MB")
    
    if db_type == "postgres":
        config = POSTGRES_CONFIG
        ports = f"{config['port']}:{config['port']}"
    else:
        config = CLICKHOUSE_CONFIG
        ports = f"{config['port']}:{config['port']},{config['http_port']}:{config['http_port']}"
    
    try:
        log_section("Starting Container")
        print(f"  Image: {config['image']}")
        print(f"  Ports: {ports}")
        print(f"  CPUs: {CONTAINER_CPUS}")
        print(f"  Memory: {CONTAINER_MEM}")

        success = container_run(
            container_name=container_name,
            image=config["image"],
            ports=ports,
            env=config["env"],
            cpus=CONTAINER_CPUS,
            mem=CONTAINER_MEM,
            cmd_args=config.get("cmd_args")
        )

        if not success:
            print("  ERROR: Failed to start container")
            sys.exit(1)

        log_section("Waiting for Database Ready")
        wait_for_health(container_name, config["health_check"])

        if db_type == "postgres":
            dump_postgres_settings(container_name)

        log_section("Creating Schema")
        if db_type == "postgres":
            create_postgres_schema(container_name)
        else:
            create_clickhouse_schema(container_name)

        log_section("Loading Data")
        start = datetime.now()
        if db_type == "postgres":
            load_data_postgres(container_name)
        else:
            load_data_clickhouse(container_name)
        elapsed = datetime.now() - start
        print(f"  Data load completed in: {elapsed}")

        if not only_load:
            log_section("Running Benchmark Queries")

            if db_type == "postgres":
                run_postgres_benchmark(container_name)
            else:
                run_clickhouse_benchmark(container_name)

        if not no_cleanup:
            log_section("Cleanup")
            container_stop(container_name)
        else:
            log_section("Container Kept Running")
            print(f"  Container: {container_name}")
            print(f"  Connect with:")
            if db_type == "postgres":
                print(f"    PGPASSWORD=benchmark psql -h localhost -p 5432 -U taxi -d nyc_taxi")
            else:
                print(f"    clickhouse-client --host localhost --port 9000 -u default --password benchmark")

        print("\n" + "=" * 60)
        print("  Done!")
        print("=" * 60)
    except KeyboardInterrupt:
        print("\n\n  Interrupted! Cleaning up...")
        if not no_cleanup:
            container_stop(container_name)
        sys.exit(130)


if __name__ == '__main__':
    main()

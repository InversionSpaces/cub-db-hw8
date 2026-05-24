# CUB DB, HW8, PostgreSQL vs ClickHouse

A performance comparison of PostgreSQL 16 and ClickHouse 24.3 
running the same analytical aggregation query 
over the full 2025 NYC Yellow Taxi dataset.

## Containers

Both containers are given 8 CPUs and 16 GB of RAM.

## PostgreSQL

The script starts PostgreSQL 16-alpine with these non-default settings:

| Setting                        | Value  |
|--------------------------------|--------|
| shared_buffers                 | 4 GB   |
| work_mem                       | 256 MB |
| effective_cache_size           | 12 GB  |
| max_parallel_workers_per_gather| 8      |
| max_worker_processes           | 8      |
| max_parallel_workers           | 8      |
| maintenance_work_mem           | 1 GB   |
| jit                            | off    |

## ClickHouse

The script starts ClickHouse 24.3-alpine.

ClickHouse runs with its out-of-the-box defaults.

## Database Schemas

### PostgreSQL

```sql
CREATE TABLE IF NOT EXISTS yellow_taxi_2025 (
    id                      BIGSERIAL,
    VendorID                INTEGER,
    tpep_pickup_datetime    TIMESTAMPTZ,
    tpep_dropoff_datetime   TIMESTAMPTZ,
    passenger_count         FLOAT,
    trip_distance           FLOAT,
    RatecodeID              FLOAT,
    store_and_fwd_flag      VARCHAR(1),
    PULocationID            INTEGER,
    DOLocationID            INTEGER,
    payment_type            BIGINT,
    fare_amount             FLOAT,
    extra                   FLOAT,
    mta_tax                 FLOAT,
    tip_amount              FLOAT,
    tolls_amount            FLOAT,
    improvement_surcharge   FLOAT,
    total_amount            FLOAT,
    congestion_surcharge    FLOAT,
    Airport_fee             FLOAT,
    cbd_congestion_fee      FLOAT
);

-- Post-load
ANALYZE yellow_taxi_2025;
CREATE INDEX idx_trip_distance ON yellow_taxi_2025(trip_distance);
```

### ClickHouse

```sql
CREATE TABLE IF NOT EXISTS yellow_taxi_2025 (
    VendorID                Int32,
    tpep_pickup_datetime    DateTime64(6, 'America/New_York'),
    tpep_dropoff_datetime   DateTime64(6, 'America/New_York'),
    passenger_count         Float64,
    trip_distance           Float64,
    RatecodeID              Float64,
    store_and_fwd_flag      String,
    PULocationID            Int32,
    DOLocationID            Int32,
    payment_type            Int64,
    fare_amount             Float64,
    extra                   Float64,
    mta_tax                 Float64,
    tip_amount              Float64,
    tolls_amount            Float64,
    improvement_surcharge   Float64,
    total_amount            Float64,
    congestion_surcharge    Float64,
    Airport_fee             Float64,
    cbd_congestion_fee      Float64,

    INDEX idx_distance_amount (trip_distance, total_amount)
    TYPE minmax
    GRANULARITY 4
)
ENGINE = MergeTree()
ORDER BY (PULocationID, toHour(tpep_pickup_datetime));
```

## Benchmark Query

An OLAP query which computes aggregates by hour
filtering out outliers by distance and refunds,
then sorts by revenue:

```sql
SELECT
    PULocationID,
    EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour,
    AVG(fare_amount)  AS avg_fare,
    AVG(tip_amount)   AS avg_tip,
    SUM(total_amount) AS revenue,
    COUNT(*)          AS trips
FROM yellow_taxi_2025
WHERE trip_distance BETWEEN 1 AND 50
  AND total_amount > 0
GROUP BY PULocationID, hour
ORDER BY revenue DESC;
```

## Results

### PostgreSQL - EXPLAIN

```
Sort  (cost=8845545.08..8871488.22 rows=10377254 width=68)
  Sort Key: (sum(total_amount)) DESC
  ->  GroupAggregate  (cost=1999639.68..7210604.37 rows=10377254 width=68)
        Group Key: pulocationid, (EXTRACT(hour FROM tpep_pickup_datetime))
        ->  Gather Merge  (cost=1999639.68..6481766.06 rows=36482424 width=60)
              Workers Planned: 7
              ->  Sort  (cost=1998639.56..2011669.00 rows=5211775 width=60)
                    Sort Key: pulocationid, (EXTRACT(hour FROM tpep_pickup_datetime))
                    ->  Parallel Seq Scan on yellow_taxi_2025  (cost=0.00..1221227.94 rows=5211775 width=60)
                          Filter: ((trip_distance >= '1'::double precision)
                                   AND (trip_distance <= '50'::double precision)
                                   AND (total_amount > '0'::double precision))
```

The planner chooses a **parallel sequential scan**.
Index is not used as it is not selective enough,
most rides are between 1 and 50 miles and have positive total amount.

### PostgreSQL - Execution

```
 pulocationid | hour |      avg_fare       |        avg_tip         |      revenue       | trips
--------------+------+---------------------+------------------------+--------------------+--------
          132 |   16 |   69.60525827795814 |     10.829896971280366 | 13024983.119996078 | 141611
          132 |   17 |   66.66899201158434 |     10.399785845792722 | 11489317.129998878 | 129813
          ... (6151 more rows)
          245 |   16 |              -21.72 |                      0 |               3.85 |      1
          253 |    6 |                -1.5 |                      0 |                1.5 |      1
(6155 rows)

Time: 12630.141 ms (00:12.630)
```

**~12.6 seconds.**

### ClickHouse - EXPLAIN

```
Expression (Project names)
  Sorting (Sorting for ORDER BY)
    Expression ((Before ORDER BY + Projection))
      Aggregating
        Expression (Before GROUP BY)
          Expression
            ReadFromMergeTree (nyc_taxi.yellow_taxi_2025)
            Indexes:
              PrimaryKey
                Condition: true
                Parts: 20/20
                Granules: 5966/5966
              Skip
                Name: idx_distance_amount
                Description: minmax GRANULARITY 4
                Parts: 20/20
                Granules: 5966/5966
```

Indexes are also skipped, again because the filter is not selective enough.

### ClickHouse - Execution

```
Query completed in: 0.374s
Rows returned: 6155
First row: (132, 16, 69.60525835012024, 10.829897034191307, 13024983.109779596, 141611)
```

**~0.37 seconds.**

## Interpretation

| Metric              | PostgreSQL | ClickHouse | Speedup  |
|---------------------|------------|------------|----------|
| Execution time      | 12.63 s    | 0.37 s     | **~34x** |
| Rows returned       | 6 155      | 6 155      | -        |
| Top group           | 132, h16   | 132, h16   | -        |

ClickHouse is ~34x faster on this query. Probably due to:

1. **Columnar storage.** ClickHouse stores each column in its own file on disk. 
   The query touches only 5 of 20 columns, so it reads much less data.
   PostgreSQL's row-oriented storage requires reading all columns for all rows that pass the filter.

2. **Sort key alignment.** ClickHouse's `ORDER BY (PULocationID, toHour(...))` exactly matches the `GROUP BY` clause. 
   Rows in the same group are stored contiguously, so the aggregation is a streaming fold.

3. **Vectorized execution.** ClickHouse processes data in columnar batches (vectors) rather than row-by-row, which keeps CPU pipelines full and amortizes per-row overhead. 
   PostgreSQL's executor is row-at-a-time, which is simpler but slower for bulk analytics.

## Running the Benchmark

Run the benchmark (each command launches a container, loads data, runs the query, and tears down):
```
uv run python benchmark.py --db postgres
uv run python benchmark.py --db clickhouse
```

Notes:
- It uses podman, can be changed to docker in source code.
- It will download the dataset if it is missing.

# HDFS + Parquet baseline

Comparison point for the PostgreSQL + ClickHouse lab: same data, same questions, different storage.
Standalone: it needs only Docker. It reads the same `../data/UGLC_point.csv` as the SQL lab (and downloads it if missing).

```
./setup.sh            # download CSV, start HDFS + Spark, clean in Spark, write Parquet, check
./test.sh             # V1-V4, B1-B5, C1/C2 -> ../results/hdfs-<date-time>/summary.md
LAYOUT=decade ./test.sh   # same, on the decade-partitioned copy
```

## Design

| | Main lab | This baseline |
|---|---|---|
| Source of data | raw CSV -> cleaned in SQL | raw CSV -> cleaned in Spark (same rules: sentinels to NULL, 109 swapped dates, 9 infinite points) |
| Storage | PostgreSQL / ClickHouse | HDFS (1 namenode, 1 datanode, replication 1), Parquet + snappy |
| Query engine | the database | Spark SQL 3.5, local mode, driver memory `SPARK_MEM` (default 3g) |
| Method | each query twice, report the 2nd | same, in one Spark session; session start-up reported separately |

Three Parquet copies are written: `landslides_flat` (sorted by id, 32 MB row groups, read tests),
`landslides_decade` (partitioned by decade, optional read tests), `landslides_rw` (copy for C1/C2).

## Things to say in the report

- 1.06 M rows (~340 MB CSV) is far below the scale where HDFS pays off; a one-node cluster only measures the format and the engine.
- No indexes: V1 relies on Parquet min/max statistics (data sorted by id); V4 has no spatial index (V4bbox shows the workaround).
- No UPDATE: C2 is a full rewrite plus a non-atomic rename. No sync lag, because there is no second store.
- Tuning choices made here (shuffle partitions = 8, row group 32 MB, flat vs decade layout) change the numbers; keep them in the method section.
- The setup checks the cleaned data against facts the dataset is known to have (1,060,503 rows, 109 swapped dates, 9 infinite points).
- Row counts of V2/V3 can differ from the SQL lab if its `./test.sh` added TEST reports; the baseline has its own C1/C2 copy.

## Not tested

Written without being able to run Docker. Likely first-run snags: image tags (`apache/hadoop:3.4.1`, `apache/spark:3.5.3-python3`),
datanode registration time, and the Spark container's access to `/data`. Send the output of
`docker compose logs` or `results/hdfs-*/1_spark.log` and I'll fix it.

# TPC-DS query benchmark with sparkMeasure

Builds an image running upstream **TPCDS_PySpark** (Luca Canali, CERN) against
data already generated into S3, with **sparkMeasure** collecting real Spark
metrics rather than wall-clock time alone.

This is a separate image from `k8s-tpcds-gen`, built from the published
packages. Nothing here comes from the previously supplied script archive, whose
copy had sparkMeasure removed and therefore reported only elapsed time.

## What it measures

Each query runs into a `noop` sink — the full plan executes and nothing is
written, so the measurement is of query execution and not of the object store.
For every execution sparkMeasure reports executor run time, executor CPU time,
JVM GC time, bytes and records read, shuffle read/write volumes, spill, and
peak execution memory. The tool then prints three tables: every execution, the
**median per query**, and the totals across the whole run.

Median per query is the number to compare between configurations; the first
execution of a query includes cold reads and JIT warm-up, so a run with
`REPEAT=1` measures something different from one with `REPEAT=3`. Keep the
setting fixed across runs you intend to compare.

## 1. Collect the dependencies (on a machine with internet)

The wheels must match the base image's Python, so read that version off the
image first:

```
podman run --rm --entrypoint python3 <base image> \
  -c 'import sys; print("%d.%d" % sys.version_info[:2])'
```

```
PYVER=3.11 ./fetch-deps.sh
```

That fills `wheels/` (pip, pandas and its dependencies, sparkmeasure,
TPCDS_PySpark) and `jars/` (`spark-measure_2.12-0.28.jar`). Transfer this whole
directory into the isolated segment.

`spark-measure_2.12-0.28` is built against Scala 2.12.18 and Spark 3.5.8, which
matches these images exactly. The TPCDS_PySpark wheel ships its own copy built
for **Scala 2.13** — incompatible, and the Dockerfile deletes it.

## 2. Build (no network needed)

```
podman build -f Dockerfile \
  --build-arg BASE_IMAGE=negistry.ehd-zr.cbr.ru/ehd/k8s/nova/spark-tpcds-gen:2026.2.1_spark3.5.8_iceberg1.10_cb-ca \
  -t negistry.ehd-zr.cbr.ru/ehd/k8s/nova/spark-tpcds-bench:2026.2.1_spark3.5.8_iceberg1.10_cb-ca \
  .
```

The build context is this directory, not the repository root.

Python packages are installed into `/opt/tpcds-python` and reached through
`PYTHONPATH`, so the base image's own pandas/numpy are left untouched — other
tooling in a corporate image may depend on them.

Check the result before pushing:

```
podman run --rm --entrypoint python3 <built image> -c \
  'import pandas, sparkmeasure, tpcds_pyspark, importlib.resources as r; \
   print(pandas.__version__, len(list(r.files("tpcds_pyspark").joinpath("Queries").iterdir())))'
```

It should print the pandas version and `119`.

```
podman push negistry.ehd-zr.cbr.ru/ehd/k8s/nova/spark-tpcds-bench:2026.2.1_spark3.5.8_iceberg1.10_cb-ca
```

## 3. Smoke test first

```
IMAGE=negistry.ehd-zr.cbr.ru/ehd/k8s/nova/spark-tpcds-bench:2026.2.1_spark3.5.8_iceberg1.10_cb-ca \
K8S_MASTER=k8s://https://<api-server> \
S3_ENDPOINT=https://<minio> \
DATA_PATH=s3a://spark-k8s/tpcds_1 \
QUERIES=q1,q3,q5 \
RESULTS_PATH=s3a://spark-k8s/results/smoke \
./run-tpcds-bench.sh
```

Three queries against the SF1 dataset proves the whole path: the metrics
listener loads, the tables map, and results reach S3. If the sparkMeasure jar
were missing or built for the wrong Scala version, this is where it fails.

## 4. Full run

```
IMAGE=... K8S_MASTER=... S3_ENDPOINT=... \
DATA_PATH=s3a://spark-k8s/tpcds_1000 \
RESULTS_PATH=s3a://spark-k8s/results/sf1000-$(date +%F-%H%M) \
./run-tpcds-bench.sh
```

Defaults: all 119 queries, `NUM_RUNS=1`, `REPEAT=1` — one pass, which is what
tells you how long a pass takes at SF1000 before committing to more. Upstream's
own defaults are `-n 2 -r 3`, i.e. 714 executions; raise `REPEAT` to 3 for a
publishable median once the duration is known.

Useful knobs, all environment variables:

| | |
|---|---|
| `QUERIES` | `all`, or a list such as `q1,q3,q5` |
| `QUERIES_EXCLUDE` | queries to skip — use it for one that fails or runs away |
| `NUM_RUNS`, `REPEAT` | outer passes and repeats per query |
| `EXECUTORS`, `EXECUTOR_CORES`, `EXECUTOR_MEMORY`, `EXECUTOR_OVERHEAD` | default 16 × 4 cores, 6g + 2g — the shape used for generation, so results are comparable |
| `SHUFFLE_PARTITIONS` | default 2048, with AQE on top |
| `LOG_LEVEL` | default `WARN`; a full run at `INFO` produces an unreadable log |
| `EVENTLOG_ENABLED` | default `true`; set `false` if the event log size is a problem — 119 queries produce a large one |

## 5. Results

`RESULTS_PATH` produces three folders via Spark:

- `<path>` — one row per execution
- `<path>.grouped` — median per query
- `<path>.aggregated` — totals over the run

Without it the results exist only in the driver log, which disappears with the
pod:

```
kubectl logs -n spark-workload <driver-pod> > tpcds-bench-$(date +%F-%H%M).log
```

The run also leaves a Spark event log, which is the independent cross-check on
sparkMeasure's numbers.

## Notes

The workload maps each table as a temporary view, so no metastore is needed.
The `--run_using_metastore` and `--create_metastore_tables*` options exist but
require `spark.sql.catalogImplementation=hive` and a configured metastore; they
are worth it only for testing the cost-based optimizer, which needs table
statistics.

`spark.sql.execution.arrow.pyspark.enabled` is off by default and should stay
off: with it on, writing the results converts through Arrow and would need
pyarrow on the executors, which this image does not install.

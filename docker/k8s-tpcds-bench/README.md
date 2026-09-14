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
docker run --rm --entrypoint python3 <base image> \
  -c 'import sys; print("%d.%d" % sys.version_info[:2])'
```

```
PYVER=3.11 ./fetch-deps.sh
```

`PYVER` must be on the same line as the command — a separate assignment is not
exported to the script.

That fills `wheels/` (pip, pandas and its dependencies, sparkmeasure,
TPCDS_PySpark) and `jars/` (`spark-measure_2.12-0.28.jar`).

`spark-measure_2.12-0.28` is built against Scala 2.12.18 and Spark 3.5.8, which
matches these images exactly. The TPCDS_PySpark wheel ships its own copy built
for **Scala 2.13** — incompatible, and the Dockerfile deletes it.

## 2. Pack and transfer

```
./pack-context.sh
```

That produces `tpcds-bench-context-<date>.tar.gz` and a `.sha256` beside it. The
archive carries the wheels, the jar, the Dockerfile and the scripts — everything
the isolated segment needs — plus a `SHA256SUMS` inside it and a `MANIFEST.txt`
recording which Python ABI the wheels were built for.

gzip rather than xz or zstd: the target CentOS 8 host has neither, and at a few
tens of megabytes the weaker compression costs nothing worth having.

Copy both files across, then verify before unpacking — a truncated SFTP transfer
otherwise surfaces as a confusing build error much later:

```
sha256sum -c tpcds-bench-context-<date>.tar.gz.sha256
tar -xzf tpcds-bench-context-<date>.tar.gz
cd tpcds-bench-context
```

## 3. Build (no network needed)

First make sure the daemon can reach the registry for the base image — the
corporate CA has to be trusted by the **daemon**, not just by the shell:

```
sudo mkdir -p /etc/docker/certs.d/negistry.ehd-zr.cbr.ru
sudo cp <corporate-ca>.crt /etc/docker/certs.d/negistry.ehd-zr.cbr.ru/ca.crt
docker login negistry.ehd-zr.cbr.ru
```

Then, from the unpacked `tpcds-bench-context` directory:

```
BASE_IMAGE=negistry.ehd-zr.cbr.ru/ehd/k8s/nova/spark-tpcds-gen:2026.2.1_spark3.5.8_iceberg1.10_cb-ca \
TARGET_IMAGE=negistry.ehd-zr.cbr.ru/ehd/k8s/nova/spark-tpcds-bench:2026.2.1_spark3.5.8_iceberg1.10_cb-ca \
./build-image.sh
```

`build-image.sh` re-checks `SHA256SUMS`, pulls the base image, and — the part
worth having — compares the base image's Python against the ABI the wheels were
built for. A mismatch there builds cleanly and then fails on import inside the
driver pod, hours later; the script stops before the build instead and tells you
which `PYVER` to re-fetch with. After building it verifies that pandas imports,
that all 119 query files are present, that the sparkMeasure jar is in place, and
that `tpcds_pyspark` imports with pyspark on the path.

That last check puts the pyspark zips on `PYTHONPATH` itself. In these images
pyspark lives in `$SPARK_HOME/python/lib/*.zip`, which only `spark-submit` adds
to the path — so a plain `python3 -c 'import pyspark'` inside the image fails
even though the driver imports it without trouble.

```
docker push negistry.ehd-zr.cbr.ru/ehd/k8s/nova/spark-tpcds-bench:2026.2.1_spark3.5.8_iceberg1.10_cb-ca
```

### If you would rather run the build by hand

```
docker build -f Dockerfile \
  --build-arg BASE_IMAGE=<base image> \
  -t <target image> .
```

The build context is that directory, not the repository root.

### Notes on the build

Whether BuildKit is on or off makes no difference: nothing in the Dockerfile
needs it, and it carries no `# syntax=` directive — that directive would send
BuildKit to Docker Hub for its frontend image, which this segment cannot reach.

If `docker` needs `sudo` on this host, prefix the commands with it (and pass the
variables after `sudo`, or use `sudo -E`), or add yourself to the `docker` group
(`sudo usermod -aG docker $USER`, then log in again). Membership in that group is
equivalent to root.

Python packages are installed into `/opt/tpcds-python` and reached through
`PYTHONPATH`, so the base image's own pandas/numpy are left untouched — other
tooling in a corporate image may depend on them.

## 4. Smoke test first

```
cp smoke.env.example smoke.env
$EDITOR smoke.env
./smoke-test.sh
```

Three queries against the SF1 dataset prove the whole path end to end. The
script checks the cluster before submitting (kubectl reachable, namespace,
service account, MinIO secret, token file, no leftover driver pod), names the
driver pod so the log can be fetched afterwards, saves it as
`smoke-<timestamp>.log`, and reads the metrics back out:

```
=== Results ===
  QUERY       ELAPSED   RUN TIME        CPU         GC    TASKS
  q1           12.34s     45.67s     33.21s      1.02s      3.7
  q3            5.20s     18.90s     14.30s      0.40s      3.6
  q5            9.75s     31.40s     25.00s      0.80s      3.2

  sparkMeasure: real metrics on 3 of 3 executions
  results written to s3a://spark-k8s/results/smoke

VERDICT: PASSED
```

It fails the run, not just warns, when executor run time equals elapsed time on
every query. That is the signature of metrics that are not being collected —
run time is the sum over tasks and elapsed time is wall clock, so on a real
measurement they differ by roughly the number of active tasks. A workload that
finishes cleanly while reporting nothing but wall-clock time is exactly what the
earlier stripped-down copy did, and it is the failure this image exists to
avoid.

It also fails on a Scala or Python loading error in the log
(`NoSuchMethodError`, `ClassNotFoundException`, `ModuleNotFoundError`), on
fewer query executions than requested, and on `RESULTS_PATH` being set without
the results actually being written.

`smoke.env` holds your cluster values so they are typed once; it is gitignored,
and anything already exported in your shell overrides it. `KEEP_POD=true` leaves
the driver pod behind for inspection.

## 5. Full run

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

### Tuning

`run-tpcds-bench.sh` takes `TUNING_CONF=<file>`, a list of `key=value` settings
appended after its own `--conf` flags — spark-submit keeps the last value for a
repeated key, so the file can override anything the script sets.

```
TUNING_CONF=tuning-sf1000.conf ... ./run-tpcds-bench.sh
```

`tuning-sf1000.conf` holds the candidates for SF1000 on this cluster, with the
default value named next to each. The S3 read settings are the ones with
evidence behind them: the smoke run spent 75-84% of task time off-CPU.

Apply it against a baseline run, not instead of one, and change one group at a
time — a whole file applied at once tells you the total and nothing about which
setting earned it.

## 6. Results

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

# TPC-DS data generation on Spark-on-Kubernetes (Deckhouse) → S3

Builds an image on top of an existing Spark 3.5.8 image with:

* `dsdgen` (Databricks' fork of the TPC-DS kit) baked in at `/opt/tpcds-kit/tools`,
  present on every driver/executor pod since they all run from the same image.
* the `spark-sql-perf` assembly jar.
* the S3A connector (`hadoop-aws` + `aws-java-sdk-bundle`) for writing to S3.

## 1. Build the spark-sql-perf jar

```
build/sbt assembly
```

Produces `target/scala-2.12/spark-sql-perf-assembly-<version>.jar`.
`build.sbt` is pinned to Scala 2.12.18 to match the Scala version the
`apache/spark:3.5.8` image itself is built with — Scala 2.12 and 2.13 are
not binary-compatible, so building against 2.13 fails at runtime with
`NoSuchMethodError`.

## 2. Build and push the image

```
docker build -f docker/k8s-tpcds-gen/Dockerfile \
  --build-arg SPARK_BASE_IMAGE=<your-registry>/spark:3.5.8 \
  -t <your-registry>/spark-tpcds-gen:3.5.8 .

docker push <your-registry>/spark-tpcds-gen:3.5.8
```

Adjust `SPARK_BASE_IMAGE` to whatever Spark 3.5.8 image your Deckhouse cluster
already uses. If that image already ships `hadoop-aws`/`aws-java-sdk-bundle`,
drop that `RUN curl ...` block from the Dockerfile.

## 2b. Rebase onto another Spark image (optional)

To put the same artifacts on a different Spark image — one with your own CA
bundle, Iceberg, S3 setup — use `Dockerfile.rebase`, which copies dsdgen and the
assembly jar out of the image built above instead of rebuilding them:

```
podman build -f docker/k8s-tpcds-gen/Dockerfile.rebase \
  --build-arg TPCDS_SRC_IMAGE=<registry>/spark-tpcds-gen:3.5.8 \
  --build-arg SPARK_BASE_IMAGE=<registry>/spark:<tag> \
  -t <registry>/spark-tpcds-gen:<tag> .
```

It compiles and downloads nothing and ignores the build context, so it runs in a
network-isolated environment. It also skips the S3A jars on purpose — a Spark
image that already talks to S3 has its own, and a second copy at a different
version breaks the classpath.

## 3. Run data generation

Edit the environment variables at the top of `generate-tpcds-data.sh`
(image, Kubernetes API server, namespace/service account, S3 endpoint and
bucket, scale factor), make sure the `spark` service account/namespace has
whatever RBAC Deckhouse's Spark-on-Kubernetes setup requires, provide AWS
credentials to the driver/executor pods (Kubernetes Secret, or the
`spark.hadoop.fs.s3a.access.key`/`secret.key` `--conf` lines), then run:

```
./generate-tpcds-data.sh
```

`-s` is the TPC-DS scale factor in GB; `-n` controls how many dsdgen
partitions/tasks are used to parallelize generation.

## 4. Analyze a run

`analyze-gen-run.sh` turns a driver log into per-table timings, separating the
time spent generating and writing rows from the time spent committing — on
object storage the commit is often the larger half, and the driver logs it as a
single silent gap:

```
./analyze-gen-run.sh driver.log
./analyze-gen-run.sh -p <driver-pod> -n spark-workload
```

Driver pods are short-lived, so keep the log to compare runs later:
`kubectl logs -n <ns> <driver-pod> > run-sf1-$(date +%F-%H%M).log`

`analyze-gen-output.sh` reports the shape of the result — files, partition
directories, and average file size per table — and flags leftover `_temporary`
from an interrupted run. It reads `<bytes> <key>` lines on stdin so it works
with any S3 client (see the header for `aws` and `mc` forms):

```
S3_ENDPOINT=https://minio.example ./analyze-gen-output.sh -a s3://spark-k8s/tpcds_1/
```

Average file size is the number to watch. A date-partitioned fact table spreads
a small scale factor across ~1800 partitions, so at SF1 each file is around half
a megabyte and the write is dominated by per-object overhead rather than by
bytes; the same partition count at SF1000 produces healthy files. If that is
where the time goes at a small scale factor, `-p false` disables partitioning —
at the cost of a layout no longer representative for partition-pruning queries.

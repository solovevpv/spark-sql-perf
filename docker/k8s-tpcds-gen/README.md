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

Produces `target/scala-2.13/spark-sql-perf-assembly-<version>.jar`.

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

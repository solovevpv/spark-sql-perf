#!/usr/bin/env bash
#
# Runs TPC-DS data generation (spark-sql-perf's GenTPCDSData) on a
# Spark-on-Kubernetes (Deckhouse) cluster, writing output to S3.
#
# Requires: the image built from docker/k8s-tpcds-gen/Dockerfile pushed to a
# registry the cluster can pull from, and s3-credentials-secret providing
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY as env vars to driver+executors
# (see the commented-out --conf lines below for an alternative).

set -euo pipefail

: "${IMAGE:=<your-registry>/spark-tpcds-gen:3.5.8}"
: "${K8S_MASTER:=k8s://https://<k8s-api-server>:6443}"
: "${K8S_NAMESPACE:=spark}"
: "${SERVICE_ACCOUNT:=spark}"
: "${S3_ENDPOINT:=https://s3.example.com}"
: "${S3_BUCKET:=s3a://my-bucket/tpcds}"
: "${SCALE_FACTOR:=1000}"
: "${NUM_PARTITIONS:=200}"

# Either export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY into the driver and
# executor pods (e.g. via a Kubernetes Secret + envFrom on the pod template),
# or uncomment the two spark.hadoop.fs.s3a.access.key/secret.key --conf lines
# below and pass them explicitly instead.

spark-submit \
  --master "${K8S_MASTER}" \
  --deploy-mode cluster \
  --name tpcds-gen-sf${SCALE_FACTOR} \
  --class com.databricks.spark.sql.perf.tpcds.GenTPCDSData \
  --conf spark.kubernetes.namespace="${K8S_NAMESPACE}" \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName="${SERVICE_ACCOUNT}" \
  --conf spark.kubernetes.container.image="${IMAGE}" \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  --conf spark.executor.instances=8 \
  --conf spark.executor.cores=4 \
  --conf spark.executor.memory=8g \
  --conf spark.driver.memory=4g \
  --conf spark.hadoop.fs.s3a.endpoint="${S3_ENDPOINT}" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  local:///opt/spark-sql-perf/spark-sql-perf-assembly.jar \
  -d "${DSDGEN_DIR:-/opt/tpcds-kit/tools}" \
  -s "${SCALE_FACTOR}" \
  -l "${S3_BUCKET}" \
  -f parquet \
  -n "${NUM_PARTITIONS}" \
  -o true

#!/usr/bin/env bash
#
# Runs the TPCDS_PySpark query workload on Spark-on-Kubernetes against data
# already generated into S3.
#
# The workload reads each table as a temporary view, so it needs no metastore.
# Results are printed to the driver log and, with RESULTS_PATH set, also written
# through Spark to S3 — worth doing, because the driver pod and anything it
# wrote to its own filesystem disappear once the run ends.
#
# QUERIES/NUM_RUNS/REPEAT default to a short smoke run. The tool's own defaults
# (all 119 queries, 2 runs, 3 repeats = 714 executions) are a full benchmark and
# take correspondingly long at a large scale factor.

set -euo pipefail

: "${IMAGE:?set IMAGE to the image built from docker/k8s-tpcds-run/Dockerfile}"
: "${K8S_MASTER:?set K8S_MASTER, e.g. k8s://https://api.example.internal}"
: "${DATA_PATH:?set DATA_PATH, e.g. s3a://spark-k8s/tpcds_1000}"
: "${S3_ENDPOINT:?set S3_ENDPOINT, e.g. https://minio.example.internal}"
: "${K8S_NAMESPACE:=spark-workload}"
: "${SERVICE_ACCOUNT:=spark-submit-sa}"
: "${OAUTH_TOKEN_FILE:=./spark-tok.jwt}"
: "${YUNIKORN_QUEUE:=root.default}"
: "${EXECUTORS:=16}"
: "${EXECUTOR_CORES:=4}"
: "${EXECUTOR_MEMORY:=6g}"
: "${EXECUTOR_OVERHEAD:=2g}"
: "${DRIVER_MEMORY:=8g}"
: "${SHUFFLE_PARTITIONS:=2048}"
: "${QUERIES:=q1,q3,q5}"
: "${NUM_RUNS:=1}"
: "${REPEAT:=1}"
: "${RESULTS_PATH:=}"
: "${APP:=local:///opt/spark/work-dir/tpcds_pyspark_run.py}"

RESULT_ARGS=()
if [ -n "$RESULTS_PATH" ]; then
  RESULT_ARGS=(-c "$RESULTS_PATH")
fi

spark-submit \
  --master "${K8S_MASTER}" \
  --deploy-mode cluster \
  --name tpcds-queries \
  --conf spark.kubernetes.container.image="${IMAGE}" \
  --conf spark.kubernetes.namespace="${K8S_NAMESPACE}" \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName="${SERVICE_ACCOUNT}" \
  --conf spark.kubernetes.authenticate.submission.oauthTokenFile="${OAUTH_TOKEN_FILE}" \
  --conf spark.driver.memory="${DRIVER_MEMORY}" \
  --conf spark.driver.cores=2 \
  --conf spark.executor.memory="${EXECUTOR_MEMORY}" \
  --conf spark.executor.memoryOverhead="${EXECUTOR_OVERHEAD}" \
  --conf spark.executor.cores="${EXECUTOR_CORES}" \
  --conf spark.executor.instances="${EXECUTORS}" \
  --conf spark.sql.shuffle.partitions="${SHUFFLE_PARTITIONS}" \
  --conf spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID=minio-secret:AWS_ACCESS_KEY_ID \
  --conf spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY=minio-secret:AWS_SECRET_ACCESS_KEY \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID=minio-secret:AWS_ACCESS_KEY_ID \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY=minio-secret:AWS_SECRET_ACCESS_KEY \
  --conf spark.kubernetes.scheduler.name=yunikorn \
  --conf spark.kubernetes.driver.annotation.yunikorn.apache.org/queue="${YUNIKORN_QUEUE}" \
  --conf spark.kubernetes.executor.annotation.yunikorn.apache.org/queue="${YUNIKORN_QUEUE}" \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir="${EVENTLOG_DIR:-s3a://spark-k8s/logs}" \
  --conf spark.hadoop.fs.s3a.endpoint="${S3_ENDPOINT}" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  "${APP}" \
  -d "${DATA_PATH}" \
  -q "${QUERIES}" \
  -n "${NUM_RUNS}" \
  -r "${REPEAT}" \
  "${RESULT_ARGS[@]}"

#!/usr/bin/env bash
#
# Runs the full TPC-DS query benchmark on Spark-on-Kubernetes against data
# already generated into S3, with sparkMeasure collecting real Spark metrics.
#
# Each query is executed into a noop sink, so the whole plan runs and nothing is
# written — the measurement is of query execution, not of the object store.
# Per query sparkMeasure reports executor run time, CPU time, JVM GC time, bytes
# and rows read, shuffle volumes, and average active tasks; the tool then takes
# the median across repeats.
#
# Results go to the driver log and, with RESULTS_PATH set, through Spark to S3.
# Set it: the driver pod and its filesystem are gone once the run ends.
#
# QUERIES/NUM_RUNS/REPEAT default to all 118 queries once. Upstream defaults are
# 2 runs x 3 repeats = 714 executions; raise REPEAT once a single pass has shown
# how long one takes at your scale factor.

set -euo pipefail

: "${IMAGE:?set IMAGE to the image built from the Dockerfile next to this script}"
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
: "${QUERIES:=all}"
: "${NUM_RUNS:=1}"
: "${REPEAT:=1}"
: "${SLEEP_TIME:=1}"
: "${DATA_FORMAT:=parquet}"
: "${RESULTS_PATH:=}"
: "${LOG_LEVEL:=WARN}"
: "${EVENTLOG_ENABLED:=true}"
: "${MINIO_SECRET:=minio-secret}"
: "${APP_NAME:=tpcds-bench}"
: "${DRIVER_CORES:=2}"
: "${DRIVER_POD_NAME:=}"
: "${TUNING_CONF:=}"
: "${APP:=local:///opt/tpcds-python/tpcds_pyspark/tpcds_pyspark_run.py}"

# Optional args are built as arrays so an unset value leaves nothing behind on
# the command line rather than an empty string the tool would try to parse.
RESULT_ARGS=()
if [ -n "$RESULTS_PATH" ]; then
  RESULT_ARGS=(-c "$RESULTS_PATH")
fi

EXCLUDE_ARGS=()
if [ -n "${QUERIES_EXCLUDE:-}" ]; then
  EXCLUDE_ARGS=(-x "$QUERIES_EXCLUDE")
fi

# An optional file of key=value settings, appended after the built-in --conf
# flags. spark-submit keeps the last value for a repeated key, so a tuning file
# can override anything set above it without this script being edited.
TUNING_ARGS=()
if [ -n "$TUNING_CONF" ]; then
  [ -r "$TUNING_CONF" ] || { echo "cannot read $TUNING_CONF" >&2; exit 1; }
  while IFS= read -r line || [ -n "$line" ]; do
    line=${line%$'\r'}
    line=${line#"${line%%[![:space:]]*}"}
    case "$line" in ''|'#'*) continue ;; esac
    case "$line" in *=*) ;; *) echo "not key=value in $TUNING_CONF: $line" >&2; exit 1 ;; esac
    TUNING_ARGS+=(--conf "$line")
  done < "$TUNING_CONF"
  echo "Applying $((${#TUNING_ARGS[@]} / 2)) settings from $TUNING_CONF" >&2
fi

# Naming the driver pod up front is what lets a caller fetch its log afterwards
# without scraping the submit output. Spark requires the name to be unused.
POD_ARGS=()
if [ -n "$DRIVER_POD_NAME" ]; then
  POD_ARGS=(--conf spark.kubernetes.driver.pod.name="$DRIVER_POD_NAME")
fi

# Comments cannot go inside the backslash-continued command below: they would
# terminate it and silently drop the application and its arguments.
spark-submit \
  --master "${K8S_MASTER}" \
  --deploy-mode cluster \
  --name "${APP_NAME}" \
  --conf spark.kubernetes.container.image="${IMAGE}" \
  --conf spark.kubernetes.namespace="${K8S_NAMESPACE}" \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName="${SERVICE_ACCOUNT}" \
  --conf spark.kubernetes.authenticate.submission.oauthTokenFile="${OAUTH_TOKEN_FILE}" \
  --conf spark.driver.memory="${DRIVER_MEMORY}" \
  --conf spark.driver.cores="${DRIVER_CORES}" \
  --conf spark.executor.memory="${EXECUTOR_MEMORY}" \
  --conf spark.executor.memoryOverhead="${EXECUTOR_OVERHEAD}" \
  --conf spark.executor.cores="${EXECUTOR_CORES}" \
  --conf spark.executor.instances="${EXECUTORS}" \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.sql.shuffle.partitions="${SHUFFLE_PARTITIONS}" \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.log.level="${LOG_LEVEL}" \
  --conf spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID="${MINIO_SECRET}":AWS_ACCESS_KEY_ID \
  --conf spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY="${MINIO_SECRET}":AWS_SECRET_ACCESS_KEY \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID="${MINIO_SECRET}":AWS_ACCESS_KEY_ID \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY="${MINIO_SECRET}":AWS_SECRET_ACCESS_KEY \
  --conf spark.kubernetes.scheduler.name=yunikorn \
  --conf spark.kubernetes.driver.annotation.yunikorn.apache.org/queue="${YUNIKORN_QUEUE}" \
  --conf spark.kubernetes.executor.annotation.yunikorn.apache.org/queue="${YUNIKORN_QUEUE}" \
  --conf spark.eventLog.enabled="${EVENTLOG_ENABLED}" \
  --conf spark.eventLog.dir="${EVENTLOG_DIR:-s3a://spark-k8s/logs}" \
  --conf spark.hadoop.fs.s3a.endpoint="${S3_ENDPOINT}" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  "${TUNING_ARGS[@]}" \
  "${POD_ARGS[@]}" \
  "${APP}" \
  -d "${DATA_PATH}" \
  --data_format "${DATA_FORMAT}" \
  -q "${QUERIES}" \
  -n "${NUM_RUNS}" \
  -r "${REPEAT}" \
  -s "${SLEEP_TIME}" \
  "${EXCLUDE_ARGS[@]}" \
  "${RESULT_ARGS[@]}"

#!/usr/bin/env bash
#
# End-to-end smoke test of the benchmark image: checks the cluster can run it,
# runs three queries against a small dataset, keeps the driver log, and reads
# the metrics back out of it.
#
#   cp smoke.env.example smoke.env     # fill in your cluster once
#   ./smoke-test.sh
#
# Settings come from smoke.env (or $SMOKE_ENV), and any variable already set in
# the environment wins over the file.
#
# What it is really testing is whether sparkMeasure is collecting anything. A
# run can finish cleanly and still report nothing but wall-clock time — that is
# exactly what the earlier stripped-down copy of this workload did — so the
# metrics are read back and shown rather than assumed.

set -euo pipefail
cd "$(dirname "$0")"

CONF=${SMOKE_ENV:-./smoke.env}
if [ -f "$CONF" ]; then
  echo "Settings: $CONF"
  # Parsed rather than sourced or eval-ed: values here contain :// and <>, and
  # eval would treat those as shell syntax. Anything already exported keeps its
  # value, so the file only fills the gaps.
  while IFS= read -r line || [ -n "$line" ]; do
    line=${line%$'\r'}
    case "$line" in ''|'#'*) continue ;; esac
    case "$line" in *=*) ;; *) continue ;; esac
    name=${line%%=*}
    value=${line#*=}
    case "$name" in *[!A-Za-z0-9_]*) continue ;; esac
    case "$value" in
      \"*\") value=${value#\"}; value=${value%\"} ;;
      \'*\') value=${value#\'}; value=${value%\'} ;;
    esac
    [ -n "${!name:-}" ] || export "$name=$value"
  done < "$CONF"
else
  echo "No $CONF found; using the environment only."
fi

: "${IMAGE:?not set. Copy smoke.env.example to smoke.env and fill it in}"
: "${K8S_MASTER:?not set (k8s://https://<api-server>)}"
: "${S3_ENDPOINT:?not set (https://<minio>)}"
: "${DATA_PATH:?not set (s3a://<bucket>/<generated dataset>)}"
for v in IMAGE K8S_MASTER S3_ENDPOINT DATA_PATH; do
  case "${!v}" in
    *"<"*|*">"*) echo "$v still holds a placeholder: ${!v}" >&2
                 echo "fill it in in $CONF" >&2; exit 1 ;;
  esac
done

: "${K8S_NAMESPACE:=spark-workload}"
: "${SERVICE_ACCOUNT:=spark-submit-sa}"
: "${OAUTH_TOKEN_FILE:=./spark-tok.jwt}"
: "${MINIO_SECRET:=minio-secret}"
: "${QUERIES:=q1,q3,q5}"
: "${RESULTS_PATH:=}"
: "${KEEP_POD:=false}"

STAMP=$(date +%Y%m%d-%H%M%S)
POD=${DRIVER_POD_NAME:-tpcds-smoke-${STAMP}}
LOG="smoke-${STAMP}.log"
SUBMIT_LOG="smoke-${STAMP}.submit.log"

fail() { echo; echo "FAILED: $*" >&2; exit 1; }

echo
echo "=== Preflight ==="
for c in kubectl spark-submit; do
  command -v "$c" >/dev/null || fail "$c is not on PATH"
done
echo "  kubectl, spark-submit: found"

[ -s "$OAUTH_TOKEN_FILE" ] || fail "token file $OAUTH_TOKEN_FILE is missing or empty"
echo "  token file: $OAUTH_TOKEN_FILE"

kubectl get namespace "$K8S_NAMESPACE" >/dev/null 2>&1 \
  || fail "namespace $K8S_NAMESPACE not found, or kubectl cannot reach the cluster"
kubectl -n "$K8S_NAMESPACE" get serviceaccount "$SERVICE_ACCOUNT" >/dev/null 2>&1 \
  || fail "service account $SERVICE_ACCOUNT not found in $K8S_NAMESPACE"
kubectl -n "$K8S_NAMESPACE" get secret "$MINIO_SECRET" >/dev/null 2>&1 \
  || fail "secret $MINIO_SECRET not found in $K8S_NAMESPACE"
echo "  namespace, service account, secret: present"

# A pod left over from an earlier smoke run makes Spark refuse the name.
if kubectl -n "$K8S_NAMESPACE" get pod "$POD" >/dev/null 2>&1; then
  fail "pod $POD already exists; delete it or set DRIVER_POD_NAME"
fi

echo
echo "=== Running ${QUERIES} against ${DATA_PATH} ==="
echo "  driver pod: $POD"
echo "  image:      $IMAGE"
echo

set +e
DRIVER_POD_NAME="$POD" \
QUERIES="$QUERIES" NUM_RUNS=1 REPEAT=1 \
./run-tpcds-bench.sh 2>&1 | tee "$SUBMIT_LOG"
SUBMIT_RC=${PIPESTATUS[0]}
set -e

echo
echo "=== Collecting the driver log ==="
if kubectl -n "$K8S_NAMESPACE" logs "$POD" > "$LOG" 2>/dev/null; then
  echo "  saved $LOG ($(wc -l < "$LOG") lines)"
else
  echo "  could not read logs from $POD; falling back to the submit output" >&2
  cp "$SUBMIT_LOG" "$LOG"
fi

if [ "$KEEP_POD" != "true" ]; then
  kubectl -n "$K8S_NAMESPACE" delete pod "$POD" --wait=false >/dev/null 2>&1 || true
fi

echo
echo "=== Results ==="
QUERY_COUNT=$(echo "$QUERIES" | tr ',' '\n' | grep -c .)
awk -v expected="$QUERY_COUNT" -v submit_rc="$SUBMIT_RC" -v results="$RESULTS_PATH" '
# Read the value after the "=" rather than counting fields: these labels have
# different word counts and miscounting would silently report every metric as 0.
function afterEq(   i) {
  for (i = 1; i < NF; i++) if ($i == "=") return $(i+1) + 0
  return 0
}

/^Run [0-9]+ - query / { for (i = 1; i < NF; i++) if ($i == "query") q = $(i+1) }
/^Job finished/        { n++; name[n] = q }
/^\.\.\.Elapsed Time =/            { elapsed[n] = afterEq() }
/^\.\.\.Executors Run Time =/      { runtime[n] = afterEq() }
/^\.\.\.Executors CPU Time =/      { cpu[n]     = afterEq() }
/^\.\.\.Executors JVM GC Time =/   { gc[n]      = afterEq() }
/^\.\.\.Average Active Tasks =/    { tasks[n]   = afterEq() }

/NoSuchMethodError|ClassNotFoundException|NoClassDefFoundError/ { jvm_err = $0 }
/ModuleNotFoundError|ImportError/                              { py_err  = $0 }
/Saved the collected metrics to a cluster filesystem/          { saved = 1 }

END {
  if (n == 0) {
    print "  no query executions found in the log"
    print ""
    print "VERDICT: FAILED -- the workload did not run; read the log above"
    exit 1
  }

  printf "  %-8s %10s %10s %10s %10s %8s\n", "QUERY", "ELAPSED", "RUN TIME", "CPU", "GC", "TASKS"
  for (i = 1; i <= n; i++) {
    printf "  %-8s %9.2fs %9.2fs %9.2fs %9.2fs %8.1f\n",
           name[i], elapsed[i], runtime[i], cpu[i], gc[i], tasks[i]
    if (runtime[i] != elapsed[i]) real++
    if (tasks[i] > 1.05) parallel++
  }

  print ""
  ok = 1
  if (submit_rc != 0) { print "  spark-submit exited " submit_rc; ok = 0 }
  if (n != expected)  { printf "  expected %d executions, found %d\n", expected, n; ok = 0 }
  if (jvm_err != "")  { print "  JVM error: " substr(jvm_err, 1, 160); ok = 0 }
  if (py_err != "")   { print "  Python error: " substr(py_err, 1, 160); ok = 0 }

  # Executor run time is the sum over tasks and elapsed time is wall clock, so
  # on a real measurement they differ. The stripped copy of this workload set
  # them equal, because it had nothing else to put there.
  if (real == 0) {
    print "  run time equals elapsed time on every query -- sparkMeasure is not"
    print "  collecting, which is the whole point of this image. Check that"
    print "  /opt/spark/jars/spark-measure_2.12-*.jar is present and that the log"
    print "  has no ClassNotFoundException for ch.cern.sparkmeasure."
    ok = 0
  } else {
    printf "  sparkMeasure: real metrics on %d of %d executions\n", real, n
  }

  if (parallel == 0)
    print "  note: average active tasks never exceeded 1 -- expected at a tiny scale factor"

  if (results != "") {
    if (saved) print "  results written to " results
    else { print "  RESULTS_PATH was set but the log does not say they were saved"; ok = 0 }
  }

  print ""
  if (ok) { print "VERDICT: PASSED"; exit 0 }
  print "VERDICT: FAILED"
  exit 1
}
' "$LOG"

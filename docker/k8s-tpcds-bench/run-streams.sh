#!/usr/bin/env bash
#
# TPC-DS throughput run: several query streams executing at the same time.
#
#   STREAMS=4 IMAGE=... K8S_MASTER=... S3_ENDPOINT=... \
#   DATA_PATH=s3a://spark-k8s/tpcds_1000 \
#   RESULTS_BASE=s3a://spark-k8s/results/thr-$(date +%F-%H%M) \
#   ./run-streams.sh
#
# The workload itself is single-threaded -- it runs one query at a time -- so a
# stream is a separate Spark application. Each gets its own driver pod, its own
# results path and its own query order, and they share the cluster.
#
# Each stream runs the queries in a different rotation of the list. Running the
# same order in every stream would have them contend for the same table at the
# same moment throughout, which measures queueing rather than throughput. This
# is not the permutation TPC-DS specifies (that comes from dsqgen), so these
# numbers are comparable to your own runs, not to published results.
#
# What the run answers: how much total work the cluster does with N streams
# against one. Per-query times will be worse than the single-stream baseline --
# each stream has a quarter of the cluster -- and that is the expected result,
# not a regression.
#
# The per-stream resources below are sized so that four streams fit the same
# envelope one full run uses. Check the total this script prints against the
# YuniKorn queue quota before starting: if the queue cannot hold every pod, the
# drivers start, the executors stay Pending, and nothing progresses.

set -euo pipefail
cd "$(dirname "$0")"

: "${IMAGE:?set IMAGE}"
: "${K8S_MASTER:?set K8S_MASTER}"
: "${S3_ENDPOINT:?set S3_ENDPOINT}"
: "${DATA_PATH:?set DATA_PATH}"
: "${RESULTS_BASE:?set RESULTS_BASE, e.g. s3a://spark-k8s/results/thr-2026-09-15}"
: "${STREAMS:=4}"
: "${K8S_NAMESPACE:=spark-workload}"

# A quarter of the cluster each, and a small driver -- the driver only holds a
# table of results, so the 8g a single run gives it is wasted four times over.
: "${EXECUTORS:=3}"
: "${EXECUTOR_CORES:=4}"
: "${EXECUTOR_MEMORY:=6g}"
: "${EXECUTOR_OVERHEAD:=2g}"
: "${DRIVER_MEMORY:=3g}"
: "${DRIVER_CORES:=1}"
: "${SHUFFLE_PARTITIONS:=512}"
: "${NUM_RUNS:=1}"
: "${REPEAT:=1}"
: "${TUNING_CONF:=}"
: "${KEEP_PODS:=true}"

# TPCDS.tpcds_queries from tpcds_pyspark 1.0.6 -- 99 TPC-DS queries plus the a/b
# variants. Regenerate with:
#   python3 -c 'from tpcds_pyspark import TPCDS; print(",".join(TPCDS.tpcds_queries))'
QUERY_LIST=${QUERY_LIST:-q1,q2,q3,q4,q5,q5a,q6,q7,q8,q9,q10,q10a,q11,q12,q13,q14a,q14b,q14,q15,q16,q17,q18,q18a,q19,q20,q21,q22,q22a,q23a,q23b,q24,q24a,q24b,q25,q26,q27,q27a,q28,q29,q30,q31,q32,q33,q34,q35,q35a,q36,q36a,q37,q38,q39a,q39b,q40,q41,q42,q43,q44,q45,q46,q47,q48,q49,q50,q51,q51a,q52,q53,q54,q55,q56,q57,q58,q59,q60,q61,q62,q63,q64,q65,q66,q67,q67a,q68,q69,q70,q70a,q71,q72,q73,q74,q75,q76,q77,q77a,q78,q79,q80,q80a,q81,q82,q83,q84,q85,q86,q86a,q87,q88,q89,q90,q91,q92,q93,q94,q95,q96,q97,q98,q99}

IFS=',' read -r -a QUERIES_ARR <<< "$QUERY_LIST"
NQ=${#QUERIES_ARR[@]}
[ "$NQ" -gt 0 ] || { echo "empty QUERY_LIST" >&2; exit 1; }

rotate() {  # rotate <offset> -> comma-separated list starting at that index
  local off=$1 i out=""
  for (( i = 0; i < NQ; i++ )); do
    out="${out},${QUERIES_ARR[$(( (i + off) % NQ ))]}"
  done
  echo "${out#,}"
}

mem_mib() {  # "6g" / "512m" -> MiB
  local v=$1 n=${1%[gGmM]} unit=${1: -1}
  case "$unit" in g|G) echo $(( n * 1024 )) ;; m|M) echo "$n" ;; *) echo "$v" ;; esac
}

PER_STREAM_MIB=$(( EXECUTORS * ($(mem_mib "$EXECUTOR_MEMORY") + $(mem_mib "$EXECUTOR_OVERHEAD")) + $(mem_mib "$DRIVER_MEMORY") + 1024 ))
PER_STREAM_CORES=$(( EXECUTORS * EXECUTOR_CORES + DRIVER_CORES ))

STAMP=$(date +%Y%m%d-%H%M%S)

cat <<EOF

TPC-DS throughput run
  streams        : $STREAMS
  queries each   : $NQ (rotated per stream), ${NUM_RUNS} run(s) x ${REPEAT} repeat(s)
  data           : $DATA_PATH
  results        : $RESULTS_BASE/stream<N>
  per stream     : ${EXECUTORS} x ${EXECUTOR_CORES} cores, ${EXECUTOR_MEMORY}+${EXECUTOR_OVERHEAD}, driver ${DRIVER_MEMORY}/${DRIVER_CORES} core
  cluster demand : $(( PER_STREAM_CORES * STREAMS )) cores, $(( PER_STREAM_MIB * STREAMS / 1024 )) GiB across $(( (EXECUTORS + 1) * STREAMS )) pods
  tuning         : ${TUNING_CONF:-none}

EOF

PIDS=(); PODS=(); STARTS=(); LOGS=()

for (( s = 0; s < STREAMS; s++ )); do
  off=$(( s * NQ / STREAMS ))
  pod="tpcds-thr${s}-${STAMP}"
  log="stream${s}-${STAMP}.log"
  echo "  stream $s: pod $pod, starts at ${QUERIES_ARR[$off]}, log $log"

  DRIVER_POD_NAME="$pod" \
  APP_NAME="tpcds-thr${s}" \
  QUERIES="$(rotate "$off")" \
  RESULTS_PATH="${RESULTS_BASE}/stream${s}" \
  EXECUTORS="$EXECUTORS" EXECUTOR_CORES="$EXECUTOR_CORES" \
  EXECUTOR_MEMORY="$EXECUTOR_MEMORY" EXECUTOR_OVERHEAD="$EXECUTOR_OVERHEAD" \
  DRIVER_MEMORY="$DRIVER_MEMORY" DRIVER_CORES="$DRIVER_CORES" \
  SHUFFLE_PARTITIONS="$SHUFFLE_PARTITIONS" \
  NUM_RUNS="$NUM_RUNS" REPEAT="$REPEAT" TUNING_CONF="$TUNING_CONF" \
    ./run-tpcds-bench.sh > "$log" 2>&1 &

  PIDS+=($!); PODS+=("$pod"); STARTS+=("$(date +%s)"); LOGS+=("$log")
done

echo
echo "All $STREAMS streams submitted. Waiting..."
WINDOW_START=$(date +%s)

RCS=(); ENDS=()
for (( s = 0; s < STREAMS; s++ )); do
  set +e; wait "${PIDS[$s]}"; RCS+=($?); set -e
  ENDS+=("$(date +%s)")
  echo "  stream $s finished (exit ${RCS[$s]})"
done
WINDOW_END=$(date +%s)

echo
echo "Collecting driver logs..."
for (( s = 0; s < STREAMS; s++ )); do
  dl="driver-thr${s}-${STAMP}.log"
  if kubectl -n "$K8S_NAMESPACE" logs "${PODS[$s]}" > "$dl" 2>/dev/null; then
    echo "  $dl"
  else
    echo "  stream $s: driver log unavailable, see ${LOGS[$s]}" >&2
  fi
  [ "$KEEP_PODS" = "true" ] || kubectl -n "$K8S_NAMESPACE" delete pod "${PODS[$s]}" --wait=false >/dev/null 2>&1 || true
done

echo
printf "%-8s %8s %10s %12s %10s\n" "STREAM" "EXIT" "WALL" "EXECUTIONS" "STATUS"
ok=0
for (( s = 0; s < STREAMS; s++ )); do
  wall=$(( ENDS[s] - STARTS[s] ))
  dl="driver-thr${s}-${STAMP}.log"
  # grep -c prints 0 and exits 1 when nothing matches, so the count has to be
  # taken without letting that exit status reach set -e or append a second 0.
  done_n=0
  if [ -f "$dl" ]; then
    done_n=$(grep -c '^Job finished' "$dl" || true)
  fi
  want=$(( NQ * NUM_RUNS * REPEAT ))
  if [ "${RCS[$s]}" -eq 0 ] && [ "$done_n" -eq "$want" ]; then st=ok; ok=$(( ok + 1 )); else st=INCOMPLETE; fi
  printf "%-8s %8s %7dm%02ds %6d/%-5d %10s\n" "$s" "${RCS[$s]}" $(( wall / 60 )) $(( wall % 60 )) "$done_n" "$want" "$st"
done

window=$(( WINDOW_END - WINDOW_START ))
echo
printf "  throughput window : %dm%02ds for %d streams\n" $(( window / 60 )) $(( window % 60 )) "$STREAMS"
printf "  streams completed : %d of %d\n" "$ok" "$STREAMS"
echo
echo "Compare the window against a single-stream run of the same query set:"
echo "  N streams in less than Nx the single-stream time means the cluster had"
echo "  headroom; roughly Nx means one stream already saturated it."

[ "$ok" -eq "$STREAMS" ] || exit 1

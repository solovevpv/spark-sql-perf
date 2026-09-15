#!/usr/bin/env bash
#
# Pulls node and pod metrics for a benchmark window out of the Deckhouse
# Prometheus, so the Spark-side numbers can be read against what the machines
# were actually doing.
#
#   START=2026-09-15T10:44:00Z END=2026-09-15T13:30:00Z ./collect-cluster-metrics.sh
#
# Or let it take the window from a run's results CSV (the one with a timestamp
# column), padded by five minutes on each side:
#
#   FROM_RESULTS=results.csv ./collect-cluster-metrics.sh
#
# Deckhouse runs Prometheus in the d8-monitoring namespace. The service name
# differs between versions, so it is discovered unless PROM_SVC is set. Nothing
# is exposed: a port-forward is opened for the duration and closed after.
#
# Output: one CSV per metric in OUT_DIR, as timestamp,series,value, plus a
# printed summary. Spark's own metrics say where time went inside the job;
# these say whether the machines were saturated while it happened.

set -euo pipefail
cd "$(dirname "$0")"

: "${PROM_NS:=d8-monitoring}"
: "${PROM_SVC:=}"
: "${PROM_PORT:=9090}"
: "${LOCAL_PORT:=19090}"
: "${STEP:=30s}"
: "${OUT_DIR:=cluster-metrics-$(date +%Y%m%d-%H%M%S)}"
: "${SPARK_NS:=spark-workload}"
: "${FROM_RESULTS:=}"

if [ -n "$FROM_RESULTS" ]; then
  [ -r "$FROM_RESULTS" ] || { echo "cannot read $FROM_RESULTS" >&2; exit 1; }
  # The results CSV carries one ISO timestamp per query in column 1.
  read -r START END <<<"$(awk -F, 'NR>1 && $1 ~ /^20/ {
      if (min == "" || $1 < min) min = $1
      if ($1 > max) max = $1
    } END { print min, max }' "$FROM_RESULTS")"
  [ -n "${START:-}" ] || { echo "no timestamps found in $FROM_RESULTS" >&2; exit 1; }
  START=$(date -u -d "$START - 5 minutes" +%Y-%m-%dT%H:%M:%SZ)
  END=$(date -u -d "$END + 10 minutes" +%Y-%m-%dT%H:%M:%SZ)
fi

: "${START:?set START (RFC3339, e.g. 2026-09-15T10:44:00Z) or FROM_RESULTS}"
: "${END:?set END (RFC3339) or FROM_RESULTS}"

command -v kubectl >/dev/null || { echo "kubectl not on PATH" >&2; exit 1; }
command -v curl >/dev/null    || { echo "curl not on PATH" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 not on PATH" >&2; exit 1; }

if [ -z "$PROM_SVC" ]; then
  PROM_SVC=$(kubectl -n "$PROM_NS" get svc -o name 2>/dev/null \
    | sed 's|service/||' | grep -E '^(prometheus|prometheus-main|prometheus-operated)$' | head -1 || true)
  [ -n "$PROM_SVC" ] || {
    echo "could not find a Prometheus service in namespace $PROM_NS." >&2
    echo "List them and pass PROM_SVC=<name>:" >&2
    kubectl -n "$PROM_NS" get svc >&2 || true
    exit 1
  }
fi

echo "Prometheus : svc/$PROM_SVC in $PROM_NS"
echo "Window     : $START .. $END  (step $STEP)"
echo "Output     : $OUT_DIR"
echo

kubectl -n "$PROM_NS" port-forward "svc/$PROM_SVC" "${LOCAL_PORT}:${PROM_PORT}" >/dev/null 2>&1 &
PF_PID=$!
trap 'kill "$PF_PID" 2>/dev/null || true' EXIT

for _ in $(seq 1 30); do
  curl -sf "http://127.0.0.1:${LOCAL_PORT}/-/ready" >/dev/null 2>&1 && break
  kill -0 "$PF_PID" 2>/dev/null || { echo "port-forward died; check RBAC for $PROM_NS" >&2; exit 1; }
  sleep 1
done
curl -sf "http://127.0.0.1:${LOCAL_PORT}/-/ready" >/dev/null 2>&1 \
  || { echo "Prometheus did not answer on 127.0.0.1:${LOCAL_PORT}" >&2; exit 1; }

mkdir -p "$OUT_DIR"

# name|promql. Rates use 2m so a 30s step still sees real variation.
QUERIES=$(cat <<EOF
node_cpu_busy_pct|100 * (1 - avg by (node) (rate(node_cpu_seconds_total{mode="idle"}[2m])))
node_mem_used_pct|100 * (1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)
node_load5_per_core|node_load5 / count by (node) (node_cpu_seconds_total{mode="idle"})
node_disk_read_MBps|sum by (node) (rate(node_disk_read_bytes_total[2m])) / 1e6
node_disk_write_MBps|sum by (node) (rate(node_disk_written_bytes_total[2m])) / 1e6
node_disk_busy_pct|100 * max by (node) (rate(node_disk_io_time_seconds_total[2m]))
node_net_rx_MBps|sum by (node) (rate(node_network_receive_bytes_total{device!~"lo|veth.*|cni.*|flannel.*"}[2m])) / 1e6
node_net_tx_MBps|sum by (node) (rate(node_network_transmit_bytes_total{device!~"lo|veth.*|cni.*|flannel.*"}[2m])) / 1e6
spark_pod_cpu_cores|sum by (pod) (rate(container_cpu_usage_seconds_total{namespace="${SPARK_NS}",container!="",container!="POD"}[2m]))
spark_pod_mem_working_set_GiB|sum by (pod) (container_memory_working_set_bytes{namespace="${SPARK_NS}",container!="",container!="POD"}) / 1073741824
spark_pod_cpu_throttled_pct|100 * sum by (pod) (rate(container_cpu_cfs_throttled_periods_total{namespace="${SPARK_NS}"}[2m])) / clamp_min(sum by (pod) (rate(container_cpu_cfs_periods_total{namespace="${SPARK_NS}"}[2m])), 1)
EOF
)

printf "%-32s %8s %10s %10s %10s\n" "METRIC" "SERIES" "MIN" "AVG" "MAX"
while IFS='|' read -r name q; do
  [ -n "$name" ] || continue
  out="$OUT_DIR/${name}.csv"
  curl -sG "http://127.0.0.1:${LOCAL_PORT}/api/v1/query_range" \
    --data-urlencode "query=${q}" \
    --data-urlencode "start=${START}" \
    --data-urlencode "end=${END}" \
    --data-urlencode "step=${STEP}" > "$OUT_DIR/.raw.json" || { echo "  $name: request failed" >&2; continue; }
  python3 - "$OUT_DIR/.raw.json" "$out" "$name" <<'PY'
import json,sys,csv
raw,out,name=sys.argv[1],sys.argv[2],sys.argv[3]
d=json.load(open(raw))
if d.get('status')!='success':
    print(f"  {name:<30} query error: {d.get('error','?')[:60]}"); sys.exit(0)
res=d['data']['result']
vals=[]
with open(out,'w',newline='') as f:
    w=csv.writer(f); w.writerow(['timestamp','series','value'])
    for s in res:
        m=s['metric']
        label=m.get('node') or m.get('pod') or m.get('instance') or 'all'
        for ts,v in s['values']:
            try: fv=float(v)
            except ValueError: continue
            w.writerow([int(float(ts)),label,v]); vals.append(fv)
if not vals:
    print(f"  {name:<30} no data -- metric may be named differently in this install")
else:
    print(f"  {name:<30} {len(res):>8} {min(vals):>10.1f} {sum(vals)/len(vals):>10.1f} {max(vals):>10.1f}")
PY
done <<< "$QUERIES"

rm -f "$OUT_DIR/.raw.json"
echo
echo "CSVs in $OUT_DIR. Read them against the Spark numbers:"
echo "  node_cpu_busy_pct high + spark CPU share low  -> the cluster is busy on something else"
echo "  node_net_rx high, node_disk_busy low          -> reads come over the network, as expected for s3a"
echo "  spark_pod_cpu_throttled_pct above a few %     -> CPU limits are shaping the run, not the workload"

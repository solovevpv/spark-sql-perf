#!/usr/bin/env bash
#
# Summarizes a TPC-DS generation run from its Spark driver log: run settings,
# per-table wall and commit time, the slowest stages and tasks.
#
# Generation splits into two costs that need telling apart — running dsdgen and
# writing the rows, versus committing the output — and on object storage the
# commit is frequently the larger one, while the driver logs it as a single
# silent gap. This pulls both out.
#
#   ./analyze-gen-run.sh driver.log
#   ./analyze-gen-run.sh -p tpcds-gen-sf1-xxxx-driver -n spark-workload
#
# Pods outlive a run only briefly, so keep the log if you want to compare
# later:  kubectl logs -n <ns> <driver-pod> > run-sf1-$(date +%F-%H%M).log

set -euo pipefail

NAMESPACE=spark-workload
POD=""

while getopts ":p:n:h" opt; do
  case "$opt" in
    p) POD="$OPTARG" ;;
    n) NAMESPACE="$OPTARG" ;;
    h) sed -n '2,16p' "$0"; exit 0 ;;
    *) echo "usage: $0 <driver-log> | $0 -p <driver-pod> [-n <namespace>]" >&2; exit 2 ;;
  esac
done
shift $((OPTIND - 1))

if [ -n "$POD" ]; then
  LOG=$(mktemp)
  trap 'rm -f "$LOG"' EXIT
  kubectl logs -n "$NAMESPACE" "$POD" > "$LOG"
elif [ $# -eq 1 ]; then
  LOG="$1"
  [ -r "$LOG" ] || { echo "cannot read $LOG" >&2; exit 1; }
else
  echo "usage: $0 <driver-log> | $0 -p <driver-pod> [-n <namespace>]" >&2
  exit 2
fi

awk '
function tsec(t,   a) { split(t, a, ":"); return a[1]*3600 + a[2]*60 + a[3] }

# Log timestamps carry no date, so a run crossing midnight is detected by the
# clock going backwards.
function abstime(t,   s) {
  s = tsec(t)
  if (seen_time && s < last_s) day++
  last_s = s; seen_time = 1
  return day * 86400 + s
}

function dur(s,   m) {
  if (s < 0) return "?"
  if (s < 60) return sprintf("%.1fs", s)
  m = int(s / 60)
  return sprintf("%dm%02ds", m, s - m * 60)
}

/^[0-9]+\/[0-9]+\/[0-9]+ [0-9:]+/ {
  t = abstime($2)
  if (!app_start) app_start = t
  app_end = t
}

# Parse GenTPCDSData'"'"'s own flags, which follow the jar on the command line the
# entrypoint echoes. Starting at the jar avoids matching spark-submit'"'"'s and
# tini'"'"'s flags, which reuse the same letters.
/bin\/spark-submit/ && /--class/ && !have_args {
  jar = 0
  for (i = 1; i <= NF; i++) if ($i ~ /\.jar$/) { jar = i; break }
  if (jar) {
    have_args = 1
    for (i = jar; i < NF; i++) {
      if ($i == "-s") sf = $(i+1)
      else if ($i == "-n") nparts = $(i+1)
      else if ($i == "-f") fmt = $(i+1)
      else if ($i == "-l") out = $(i+1)
      else if ($i == "-p") nopart = $(i+1)
    }
  }
}

/SparkContext: Running Spark version/ { sparkver = $NF }
/Submitted application:/            { app = $NF }
# The allocator requests executors in batches, so a single line understates the
# total; "target" carries the number actually being aimed for.
/Going to request .* executors from Kubernetes/ {
  if (match($0, /target: [0-9]+/)) {
    s = substr($0, RSTART, RLENGTH); split(s, a, ": ")
    if (a[2] + 0 > nexec + 0) nexec = a[2]
  } else {
    for (i = 1; i < NF; i++) if ($i == "request" && $(i+1) + 0 > nexec + 0) { nexec = $(i+1); break }
  }
}
# Scala does not order Map keys, so match each field wherever it lands.
/executor resources: Map\(/ {
  if (match($0, /cores, amount: [0-9]+/))  { s = substr($0, RSTART, RLENGTH); split(s, a, ": "); ecores = a[2] }
  if (match($0, /memory, amount: [0-9]+/)) { s = substr($0, RSTART, RLENGTH); split(s, a, ": "); emem = a[2] }
}
/File Output Committer Algorithm version is/ { committer = $NF }
/fs\.s3a\.committer\.name|committer.name=magic/ { magic = 1 }
/SparkContext is stopping with exitCode/ { exitcode = $NF; sub(/\.$/, "", exitcode) }

/Pre-clustering with partitioning columns/ { next_is_partitioned = 1 }

/TPCDSTables: Generating table/ {
  for (i = 1; i < NF; i++) if ($i == "table") { name = $(i+1); break }
  n++
  tname[n] = name
  tstart[n] = abstime($2)
  tpart[n] = next_is_partitioned
  next_is_partitioned = 0
}

/FileFormatWriter: Write Job .* committed\. Elapsed time:/ {
  for (i = 1; i < NF; i++) if ($i == "time:") { ms = $(i+1); break }
  if (n > 0) tcommit[n] += ms / 1000
  commit_total += ms / 1000
}

/DAGScheduler: .*Stage [0-9]+ .*finished in [0-9.]+ s/ {
  for (i = 1; i < NF; i++) if ($i == "in" && $(i+2) == "s") { secs = $(i+1); break }
  st = $6; sub(/[^0-9]/, "", st)
  sn++; sname[sn] = $5 " " st; ssec[sn] = secs + 0
}

/Finished task .* in stage .* in [0-9]+ ms/ {
  for (i = 1; i < NF; i++) {
    if ($i == "stage") stg = $(i+1)
    if ($i == "in" && $(i+1) ~ /^[0-9]+$/ && $(i+2) == "ms") ms = $(i+1)
  }
  tn++; tkstage[tn] = stg; tkms[tn] = ms + 0
}

END {
  if (!n) { print "No TPC-DS table generation found in this log."; exit 1 }

  printf "\nTPC-DS generation run\n"
  printf "  application    : %s\n", app ? app : "?"
  printf "  spark          : %s", sparkver ? sparkver : "?"
  printf "   committer: algorithm v%s%s\n", committer ? committer : "?", magic ? " (magic)" : ""
  printf "  scale factor   : %s GB", sf ? sf : "?"
  printf "   dsdgen partitions (-n): %s   format: %s", nparts ? nparts : "?", fmt ? fmt : "?"
  printf "%s\n", nopart == "false" ? "   partitioning: off" : ""
  printf "  output         : %s\n", out ? out : "?"
  printf "  executors      : %s x %s cores, %s MB\n", nexec ? nexec : "?", ecores ? ecores : "?", emem ? emem : "?"
  printf "  wall time      : %s", dur(app_end - app_start)
  printf "   exit code: %s\n", exitcode == "" ? "?" : exitcode
  printf "  startup        : %s before the first table\n\n", dur(tstart[1] - app_start)

  # A table runs until the next one starts; the last one until the app ends.
  for (i = 1; i <= n; i++) {
    twall[i] = (i < n ? tstart[i+1] : app_end) - tstart[i]
    total_wall += twall[i]
    if (tpart[i]) { part_wall += twall[i]; part_n++ } else { dim_wall += twall[i]; dim_n++ }
  }

  printf "Per-table (slowest first)\n"
  printf "  %-24s %10s %10s %8s  %s\n", "TABLE", "WALL", "COMMIT", "COMMIT%", "PARTITIONED"
  for (i = 1; i <= n; i++) ord[i] = i
  for (i = 1; i <= n; i++)
    for (j = i + 1; j <= n; j++)
      if (twall[ord[j]] > twall[ord[i]]) { tmp = ord[i]; ord[i] = ord[j]; ord[j] = tmp }
  for (k = 1; k <= n; k++) {
    i = ord[k]
    pct = twall[i] > 0 ? 100 * tcommit[i] / twall[i] : 0
    printf "  %-24s %10s %10s %7.0f%%  %s\n", tname[i], dur(twall[i]), dur(tcommit[i]), pct, tpart[i] ? "yes" : ""
  }

  commit_pct = total_wall > 0 ? 100 * commit_total / total_wall : 0
  printf "\nTotals\n"
  printf "  %-22s %s\n", sprintf("%d partitioned tables", part_n), dur(part_wall)
  printf "  %-22s %s\n", sprintf("%d other tables", dim_n), dur(dim_wall)
  printf "  %-22s %s (%.0f%% of table time)\n", "commit total", dur(commit_total), commit_pct

  printf "\nSlowest stages\n"
  for (i = 1; i <= sn; i++) sord[i] = i
  for (i = 1; i <= sn; i++)
    for (j = i + 1; j <= sn; j++)
      if (ssec[sord[j]] > ssec[sord[i]]) { tmp = sord[i]; sord[i] = sord[j]; sord[j] = tmp }
  for (k = 1; k <= sn && k <= 8; k++) printf "  %-22s %8s\n", sname[sord[k]], dur(ssec[sord[k]])

  printf "\nSlowest tasks\n"
  for (i = 1; i <= tn; i++) kord[i] = i
  for (i = 1; i <= tn; i++)
    for (j = i + 1; j <= tn; j++)
      if (tkms[kord[j]] > tkms[kord[i]]) { tmp = kord[i]; kord[i] = kord[j]; kord[j] = tmp }
  for (k = 1; k <= tn && k <= 8; k++)
    printf "  stage %-16s %8s\n", tkstage[kord[k]], dur(tkms[kord[k]] / 1000)
  printf "\n"
}
' "$LOG"

#!/usr/bin/env bash
#
# Summarizes the shape of generated TPC-DS data: per table, how many files, how
# big they are on average, how many partition directories.
#
# Average file size is the number to watch. Date-partitioned fact tables spread
# a small scale factor across ~1800 partitions, so at SF1 each file lands around
# half a megabyte, and writing them is dominated by per-object overhead rather
# than by bytes. The same partition count at SF1000 gives healthy files, so this
# only bites at small scale factors.
#
# Takes a listing on stdin as "<bytes> <key>" per line, so it works with any S3
# client:
#
#   aws s3 ls --recursive s3://spark-k8s/tpcds_1/ --endpoint-url "$S3_ENDPOINT" \
#     | awk '{ $1=""; $2=""; size=$3; $3=""; print size, substr($0,4) }' \
#     | ./analyze-gen-output.sh
#
#   mc ls --recursive --json minio/spark-k8s/tpcds_1/ \
#     | python3 -c 'import sys,json; [print(json.loads(l)["size"], json.loads(l)["key"]) for l in sys.stdin]' \
#     | ./analyze-gen-output.sh
#
# With no S3 client installed — likely in an isolated environment — the Spark
# image itself has one, along with whatever CA it was built with. Credentials
# are passed through the environment so they stay out of the process list:
#
#   export AWS_ACCESS_KEY_ID=$(kubectl get secret -n spark-workload minio-secret \
#     -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 -d)
#   export AWS_SECRET_ACCESS_KEY=$(kubectl get secret -n spark-workload minio-secret \
#     -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 -d)
#
#   sudo -E podman run --rm -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY \
#     --entrypoint java <spark image> -cp '/opt/spark/jars/*' \
#     org.apache.hadoop.fs.FsShell -Dfs.s3a.endpoint=<endpoint> \
#     -Dfs.s3a.path.style.access=true -ls -R s3a://bucket/prefix/ \
#     | awk '$1 ~ /^-/ { print $5, $8 }' | ./analyze-gen-output.sh
#
# With -a it runs the aws form itself, using S3_ENDPOINT and a bucket path:
#
#   S3_ENDPOINT=https://minio.example ./analyze-gen-output.sh -a s3://spark-k8s/tpcds_1/

set -euo pipefail

if [ "${1:-}" = "-a" ]; then
  [ -n "${2:-}" ] || { echo "usage: $0 -a s3://bucket/prefix/" >&2; exit 2; }
  command -v aws >/dev/null || { echo "aws CLI not found; pipe a listing in instead (see header)" >&2; exit 1; }
  aws s3 ls --recursive "$2" ${S3_ENDPOINT:+--endpoint-url "$S3_ENDPOINT"} \
    | awk '{ size = $3; $1 = ""; $2 = ""; $3 = ""; sub(/^ +/, ""); print size, $0 }' \
    | "$0"
  exit
fi

if [ -t 0 ]; then
  echo "usage: $0 < listing   (lines of '<bytes> <key>'; see header for aws/mc forms)" >&2
  echo "       $0 -a s3://bucket/prefix/" >&2
  exit 2
fi

awk '
function human(b) {
  if (b >= 1073741824) return sprintf("%.1f GiB", b / 1073741824)
  if (b >= 1048576)    return sprintf("%.1f MiB", b / 1048576)
  if (b >= 1024)       return sprintf("%.0f KiB", b / 1024)
  return sprintf("%d B", b)
}

{
  size = $1
  key = $2
  for (i = 3; i <= NF; i++) key = key " " $i

  # Keys look like <prefix>/<table>/[<col>=<value>/]<file>, and uncommitted
  # ones like <prefix>/<table>/_temporary/<job>/<task>/<file>. In both the table
  # is the segment before the first marker; without a marker it precedes the file.
  nseg = split(key, seg, "/")
  table = ""; part = ""; is_tmp = 0
  for (i = 2; i <= nseg; i++) {
    if (seg[i] == "_temporary") { table = seg[i-1]; is_tmp = 1; break }
    if (seg[i] ~ /=/)           { table = seg[i-1]; part = seg[i]; break }
  }
  if (table == "") table = seg[nseg-1]
  if (table == "") next

  # Spark leaves _temporary and _SUCCESS behind; count them apart from data.
  if (is_tmp) { tmpfiles[table]++; next }
  if (seg[nseg] ~ /^_/) next

  files[table]++
  bytes[table] += size
  total_files++; total_bytes += size
  if (part != "") { k = table SUBSEP part; if (!(k in seenpart)) { seenpart[k] = 1; parts[table]++ } }
}

END {
  if (!total_files) { print "No data files found in the listing."; exit 1 }

  printf "\nGenerated data layout\n"
  printf "  %-24s %8s %10s %12s %10s\n", "TABLE", "FILES", "PARTS", "SIZE", "AVG FILE"
  for (t in files) ord[++n] = t
  for (i = 1; i <= n; i++)
    for (j = i + 1; j <= n; j++)
      if (bytes[ord[j]] > bytes[ord[i]]) { tmp = ord[i]; ord[i] = ord[j]; ord[j] = tmp }
  for (i = 1; i <= n; i++) {
    t = ord[i]
    printf "  %-24s %8d %10s %12s %10s%s\n", t, files[t],
           parts[t] ? parts[t] : "-", human(bytes[t]), human(bytes[t] / files[t]),
           tmpfiles[t] ? sprintf("  (+%d in _temporary)", tmpfiles[t]) : ""
  }

  printf "\n  %-24s %8d %10s %12s %10s\n", "TOTAL", total_files, "", human(total_bytes),
         human(total_bytes / total_files)

  # Leftover _temporary means a run was killed or failed mid-commit; those
  # bytes still occupy the bucket and no query will ever read them.
  leftover = 0
  for (t in tmpfiles) if (!(t in files)) { if (!leftover) printf "\n  Only _temporary left (interrupted run):\n"; leftover = 1
    printf "    %-22s %8d files\n", t, tmpfiles[t] }

  small = 0
  for (t in files) if (bytes[t] / files[t] < 8388608 && files[t] > 50) small++
  if (small)
    printf "\n  %d table(s) average under 8 MiB per file. At that size object\n" \
           "  overhead dominates the write; see the README on partitioning.\n", small
  printf "\n"
}
'

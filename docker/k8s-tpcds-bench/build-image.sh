#!/usr/bin/env bash
#
# Builds the benchmark image inside the isolated segment, from the unpacked
# context archive. Nothing here reaches the internet except the base image pull
# from the corporate registry.
#
#   BASE_IMAGE=negistry.ehd-zr.cbr.ru/ehd/k8s/nova/spark-tpcds-gen:<tag> \
#   TARGET_IMAGE=negistry.ehd-zr.cbr.ru/ehd/k8s/nova/spark-tpcds-bench:<tag> \
#   ./build-image.sh
#
# Both variables must be set on the same line as the command: a separate
# assignment is not exported to this script.
#
# The check worth having is the Python one. The wheels are binary and built for
# one CPython ABI; installed into an image with a different Python they build
# fine and fail on import inside the driver pod, hours later. This compares the
# two before spending the build.

set -euo pipefail
cd "$(dirname "$0")"

: "${BASE_IMAGE:?not set. Use BASE_IMAGE=<registry>/spark-tpcds-gen:<tag> ./build-image.sh -- on one line}"
: "${TARGET_IMAGE:?not set. Use TARGET_IMAGE=<registry>/spark-tpcds-bench:<tag> ./build-image.sh -- on one line}"
: "${DOCKER:=docker}"

if [ -f SHA256SUMS ]; then
  echo "Verifying the transferred files..."
  sha256sum -c SHA256SUMS
else
  echo "note: no SHA256SUMS in this directory, skipping the integrity check" >&2
fi

echo
echo "Pulling the base image..."
$DOCKER pull "$BASE_IMAGE"

WHEEL_PY=""
ABIS=$(ls wheels/*.whl 2>/dev/null | grep -oE 'cp3[0-9]+' | sort -u || true)
if [ -n "$ABIS" ]; then
  WHEEL_PY="3.${ABIS#cp3}"
  IMAGE_PY=$($DOCKER run --rm --entrypoint python3 "$BASE_IMAGE" \
    -c 'import sys; print("%d.%d" % sys.version_info[:2])')
  echo "Python in base image: ${IMAGE_PY}   wheels built for: ${WHEEL_PY}"
  if [ "$IMAGE_PY" != "$WHEEL_PY" ]; then
    echo >&2
    echo "Mismatch. The binary wheels (numpy, pandas) would install but fail to" >&2
    echo "import at runtime. Re-run fetch-deps.sh with PYVER=${IMAGE_PY} on the" >&2
    echo "machine that has internet, repack, and transfer again." >&2
    exit 1
  fi
fi

echo
echo "Building ${TARGET_IMAGE}..."
$DOCKER build -f Dockerfile --build-arg BASE_IMAGE="$BASE_IMAGE" -t "$TARGET_IMAGE" .

echo
echo "Checking the built image..."
$DOCKER run --rm --entrypoint python3 "$TARGET_IMAGE" -c '
import pandas, sparkmeasure, tpcds_pyspark
from importlib.resources import files
n = len(list(files("tpcds_pyspark").joinpath("Queries").iterdir()))
print("pandas", pandas.__version__, "- queries:", n)
assert n == 119, "expected 119 query files"
'
$DOCKER run --rm --entrypoint sh "$TARGET_IMAGE" -c \
  'ls /opt/spark/jars/spark-measure_2.12-*.jar'

echo
echo "Built ${TARGET_IMAGE}. Push it with:"
echo "  ${DOCKER} push ${TARGET_IMAGE}"

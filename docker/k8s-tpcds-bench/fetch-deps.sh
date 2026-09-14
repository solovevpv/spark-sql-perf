#!/usr/bin/env bash
#
# Collects everything the benchmark image needs into this build context. Run it
# on a machine that has internet; the Dockerfile next to it then builds with no
# network at all.
#
#   PYVER=3.11 ./fetch-deps.sh
#
# PYVER must be the Python of the base image, not of the machine running this —
# the wheels are selected for that interpreter's ABI:
#
#   docker run --rm --entrypoint python3 <base image> \
#     -c 'import sys; print("%d.%d" % sys.version_info[:2])'
#
# Two things are fetched, and the Scala one matters more than it looks. The
# TPCDS_PySpark wheel bundles spark-measure built for Scala 2.13; the Spark
# images here are Scala 2.12 builds, and mixing them gives NoSuchMethodError at
# runtime. So the jar comes from Maven Central for the right Scala version, and
# the Dockerfile deletes the bundled one.

set -euo pipefail
cd "$(dirname "$0")"

: "${PYVER:?not set. Use PYVER=3.11 ./fetch-deps.sh -- on one line, since a separate assignment is not exported to this script. See the header for reading the version off the base image.}"

# The Python package and the jar are released together and their versions track
# each other; keep them in step when bumping.
: "${SPARKMEASURE_VERSION:=0.28}"
: "${SPARKMEASURE_PY_VERSION:=0.28.0}"
: "${TPCDS_PYSPARK_VERSION:=1.0.6}"
: "${SCALA_BINARY_VERSION:=2.12}"
: "${MAVEN_REPO:=https://repo1.maven.org/maven2}"

mkdir -p wheels jars

# pip itself is fetched too: the base image may have no pip, and the Dockerfile
# can bootstrap from the wheel without installing anything first.
python3 -m pip download --dest wheels --no-cache-dir \
  --only-binary=:all: --platform manylinux2014_x86_64 --python-version "${PYVER}" \
  pip pandas \
  "sparkmeasure==${SPARKMEASURE_PY_VERSION}" \
  "TPCDS_PySpark==${TPCDS_PYSPARK_VERSION}"

JAR="spark-measure_${SCALA_BINARY_VERSION}-${SPARKMEASURE_VERSION}.jar"
curl -fSL --retry 4 --retry-delay 2 -o "jars/${JAR}" \
  "${MAVEN_REPO}/ch/cern/sparkmeasure/spark-measure_${SCALA_BINARY_VERSION}/${SPARKMEASURE_VERSION}/${JAR}"

echo
echo "Build context ready:"
ls -1 wheels jars | sed 's/^/  /'
echo
echo "Transfer this whole directory to the isolated segment and build there."

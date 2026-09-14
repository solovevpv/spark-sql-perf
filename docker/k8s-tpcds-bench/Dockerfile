# Builds the TPC-DS query benchmark image: upstream TPCDS_PySpark (Luca Canali)
# instrumented with sparkMeasure, installed from wheels already present in the
# build context. Nothing is downloaded, so this builds in an isolated segment.
#
#   ./fetch-deps.sh                     # on a machine with internet
#   docker build -f Dockerfile \
#     --build-arg BASE_IMAGE=<registry>/spark-tpcds-gen:<tag> \
#     -t <registry>/spark-tpcds-bench:<tag> .
#
# The build context is this directory: it must hold wheels/ and jars/, which
# fetch-deps.sh populates.
#
# Two deliberate choices are worth knowing before changing anything here.
#
# 1. The sparkMeasure jar goes into /opt/spark/jars, not onto a --jars flag.
#    TPCDS_PySpark tries to add its own copy through spark.driver.extraClassPath
#    in the SparkSession builder, which does nothing under spark-submit — the
#    driver JVM is already running by then and its classpath is fixed. Putting
#    the jar in the image's jar directory is what actually makes the metrics
#    listener loadable, and it needs no submit-time flag at all.
#
# 2. The wheel's bundled jar is built for Scala 2.13 and is deleted below.
#    These Spark images are Scala 2.12 builds; the two are not binary
#    compatible and mixing them fails at runtime with NoSuchMethodError.
#
# No `# syntax=` directive on purpose: with BuildKit that line makes the
# builder fetch the docker/dockerfile frontend image from Docker Hub, which
# an isolated segment cannot reach. Nothing here needs BuildKit anyway.

ARG BASE_IMAGE=apache/spark:3.5.8

FROM ${BASE_IMAGE}

USER root

ARG SCALA_BINARY_VERSION=2.12
ARG SPARKMEASURE_VERSION=0.28
# Python packages live in their own directory rather than in the image's
# site-packages: the base image is a corporate build whose own pandas/numpy
# other tooling may depend on, and this way nothing of it is overwritten.
ARG PY_PREFIX=/opt/tpcds-python

COPY jars/spark-measure_${SCALA_BINARY_VERSION}-${SPARKMEASURE_VERSION}.jar /opt/spark/jars/
COPY wheels/ /tmp/wheels/

# If the base image has no pip, bootstrap from the wheel: a pip wheel can be
# executed directly, so pip installs itself without pip being present.
RUN set -eux; \
    if python3 -m pip --version >/dev/null 2>&1; then \
      PIP="python3 -m pip"; \
    else \
      PIP="python3 $(ls /tmp/wheels/pip-*.whl | head -1)/pip"; \
    fi; \
    $PIP install --no-index --no-cache-dir --find-links=/tmp/wheels \
        --target="${PY_PREFIX}" pandas sparkmeasure TPCDS_PySpark; \
    rm -rf /tmp/wheels; \
    rm -f "${PY_PREFIX}"/tpcds_pyspark/spark-measure_*.jar; \
    chmod -R a+rX "${PY_PREFIX}" /opt/spark/jars/spark-measure_*.jar; \
    ls "${PY_PREFIX}/tpcds_pyspark/Queries" | wc -l

# Appended, never replaced: Spark images put the pyspark and py4j zips in
# PYTHONPATH, and dropping those breaks PySpark entirely. The :+ form avoids a
# trailing colon, which would silently put the working directory on sys.path.
ENV PYTHONPATH=/opt/tpcds-python${PYTHONPATH:+:${PYTHONPATH}}

# Spark images conventionally run as UID 185; override if the base differs
# (check with: docker inspect --format='{{.Config.User}}' <base image>).
ARG RUNTIME_USER=185
USER ${RUNTIME_USER}

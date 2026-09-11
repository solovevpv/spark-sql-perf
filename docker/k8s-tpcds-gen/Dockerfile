# syntax=docker/dockerfile:1
#
# Extends an existing Spark 3.5.8 image with everything needed to generate
# TPC-DS data (via spark-sql-perf) on a Spark-on-Kubernetes (Deckhouse) cluster,
# writing the output to an S3 bucket.
#
# Build (from the repo root, after `build/sbt assembly`):
#   docker build -f docker/k8s-tpcds-gen/Dockerfile \
#     --build-arg SPARK_BASE_IMAGE=<your-registry>/spark:3.5.8 \
#     -t <your-registry>/spark-tpcds-gen:3.5.8 .
#
# The image is used unmodified as both the driver and executor image in
# spark-submit's --conf spark.kubernetes.container.image=... — no custom
# ENTRYPOINT is set here, the base Spark image's own entrypoint is kept.

# ARGs used in a FROM line must be declared before the FIRST FROM in the
# file to be visible there — declaring it between stages does not work.
ARG SPARK_BASE_IMAGE=apache/spark:3.5.8

# ---------------------------------------------------------------------------
# Stage 1: build dsdgen, the TPC-DS data generator.
#
# spark-sql-perf requires the Databricks fork of the TPC-DS kit, not the
# vanilla one: it patches dsdgen to stream generated rows to stdout so
# spark-sql-perf can pipe them straight into Spark without intermediate
# files. See https://github.com/databricks/tpcds-kit.
# ---------------------------------------------------------------------------
FROM debian:12-slim AS dsdgen-builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       git gcc libc6-dev make flex bison byacc ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 https://github.com/databricks/tpcds-kit.git /tmp/tpcds-kit \
    && cd /tmp/tpcds-kit/tools \
    && make OS=LINUX

# S3A connector for writing the generated data to an S3 bucket. Fetched here
# rather than in stage 2 because the official apache/spark image does not
# ship curl/wget. Skip/remove this if your base image already has these jars.
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
RUN mkdir -p /tmp/s3a-jars \
    && curl -fsSLo /tmp/s3a-jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl -fsSLo /tmp/s3a-jars/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

# ---------------------------------------------------------------------------
# Stage 2: extend the existing Spark 3.5.8 image
# ---------------------------------------------------------------------------
FROM ${SPARK_BASE_IMAGE}

# Official Spark images run as a non-root user; switch to root to install
# files, then drop back down at the end.
USER root

ARG SPARK_HOME=/opt/spark
COPY --from=dsdgen-builder /tmp/s3a-jars/ ${SPARK_HOME}/jars/

# dsdgen binary + its .dst templates. This MUST live at the same path on
# every pod that runs a dsdgen partition (all of them, since generation is
# parallelized across executors) — baking it into the image that both the
# driver and every executor pod run from satisfies that.
ENV DSDGEN_DIR=/opt/tpcds-kit/tools
COPY --from=dsdgen-builder /tmp/tpcds-kit/tools/ ${DSDGEN_DIR}/
RUN chmod +x ${DSDGEN_DIR}/dsdgen

# spark-sql-perf assembly jar.
# Build it first with: build/sbt assembly
# (produces target/scala-2.12/spark-sql-perf-assembly-<version>.jar —
# scala-2.12 to match the Scala version the base Spark image is built with)
COPY target/scala-2.12/spark-sql-perf-assembly-*.jar /opt/spark-sql-perf/spark-sql-perf-assembly.jar

# Official Spark images run pods as UID 185; adjust if your base image differs.
RUN chown -R 185:185 /opt/tpcds-kit /opt/spark-sql-perf

USER 185

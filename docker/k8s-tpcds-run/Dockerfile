# Adds the TPCDS_PySpark query workload to an existing Spark image, so the same
# image that generated the data can also run the benchmark against it.
#
# Nothing is downloaded: the scripts come from a tarball in the build context,
# which ADD unpacks. That makes this buildable in a network-isolated segment.
#
#   docker build -f docker/k8s-tpcds-run/Dockerfile \
#     --build-arg BASE_IMAGE=<registry>/spark-tpcds-gen:<tag> \
#     -t <registry>/spark-tpcds-run:<tag> .
#
# The build context must hold tpcds_pyspark.tar.gz, whose root contains
# tpcds.py, tpcds_pyspark_run.py and Queries/.
#
# Before building, check that the base image can actually run PySpark and has
# the modules the workload imports — it needs python3, pandas and setuptools
# (for pkg_resources). Nothing here installs them, because an isolated build
# cannot reach an index:
#
#   docker run --rm --entrypoint python3 <base image> \
#     -c 'import pandas, pkg_resources, pyspark; print(pandas.__version__)'
#
# If that fails, put the wheels in the context and add before the USER line:
#   COPY wheels/ /tmp/wheels/
#   RUN python3 -m pip install --no-index --find-links=/tmp/wheels pandas \
#       && rm -rf /tmp/wheels
#
# No `# syntax=` directive on purpose: with BuildKit that line makes the
# builder fetch the docker/dockerfile frontend image from Docker Hub, which
# an isolated segment cannot reach. Nothing here needs BuildKit anyway.

ARG BASE_IMAGE=apache/spark:3.5.8

FROM ${BASE_IMAGE}

USER root

# tpcds.py resolves its SQL directory as os.getcwd()/Queries, so the scripts go
# in the image's working directory rather than a path of our choosing. The
# driver pod runs there, which is what makes the queries findable at runtime.
ARG WORK_DIR=/opt/spark/work-dir
ADD tpcds_pyspark.tar.gz ${WORK_DIR}/

# Readable by whichever UID the base image runs as.
RUN chmod -R a+rX ${WORK_DIR}/Queries ${WORK_DIR}/tpcds.py ${WORK_DIR}/tpcds_pyspark_run.py

# Spark images conventionally run as UID 185; override if the base differs
# (check with: docker inspect --format='{{.Config.User}}' <base image>).
ARG RUNTIME_USER=185
USER ${RUNTIME_USER}

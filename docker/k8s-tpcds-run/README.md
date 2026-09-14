# Running the TPC-DS query workload on Spark-on-Kubernetes

Adds the TPCDS_PySpark workload (`tpcds.py`, `tpcds_pyspark_run.py`, `Queries/`)
to the image that generated the data, so the same image both produces and
queries the dataset.

## 1. Check the base image can run it

The workload is PySpark and imports `pandas` and `pkg_resources`. Nothing in the
build installs them — an isolated build has no index to install from — so
confirm they are already there:

```
podman run --rm --entrypoint python3 <base image> \
  -c 'import pandas, pkg_resources, pyspark; print(pandas.__version__)'
```

If that fails, collect the wheels on a connected machine, put them in the build
context under `wheels/`, and add to the Dockerfile before the `USER` line:

```
COPY wheels/ /tmp/wheels/
RUN python3 -m pip install --no-index --find-links=/tmp/wheels pandas && rm -rf /tmp/wheels
```

## 2. Build

The build context must contain `tpcds_pyspark.tar.gz`, whose root holds
`tpcds.py`, `tpcds_pyspark_run.py` and `Queries/`. `ADD` unpacks it, so the
build needs no network.

```
podman build -f docker/k8s-tpcds-run/Dockerfile \
  --build-arg BASE_IMAGE=<registry>/spark-tpcds-gen:<tag> \
  -t <registry>/spark-tpcds-run:<tag> .
```

The scripts land in `/opt/spark/work-dir` deliberately: `tpcds.py` resolves its
SQL directory as `os.getcwd()/Queries`, and that is the directory the driver pod
runs in. Putting them anywhere else makes the queries unfindable at runtime.

## 3. Run

```
IMAGE=<registry>/spark-tpcds-run:<tag> \
K8S_MASTER=k8s://https://<api-server> \
S3_ENDPOINT=https://<minio> \
DATA_PATH=s3a://spark-k8s/tpcds_1000 \
./run-tpcds-queries.sh
```

Defaults run three queries once — enough to prove the path end to end. For the
real benchmark set `QUERIES=all NUM_RUNS=2 REPEAT=3`, which is 714 query
executions and takes correspondingly long at a large scale factor.

Set `RESULTS_PATH=s3a://spark-k8s/results/<run>` to have Spark write the metrics
to S3. Without it the results exist only in the driver log, which disappears
with the pod:

```
kubectl logs -n spark-workload <driver-pod> > tpcds-queries-$(date +%F-%H%M).log
```

The workload maps each table as a temporary view, so no metastore is needed. Its
`--run_using_metastore` and `--create_metastore_tables` options require
`spark.sql.catalogImplementation=hive` and a configured metastore.

#!/usr/bin/env bash
#
# A/B smoke test: runs Spark's built-in SparkPi on the TPC-DS generator image,
# with the same cluster settings the real generation run uses.
#
# SparkPi needs no dsdgen, no S3 input and no application jar of ours, so a
# failure here isolates the image or the cluster from GenTPCDSData itself.
# In particular, an RPC "Cannot find endpoint: spark://CoarseGrainedScheduler@
# ...-driver-svc..." loop here means the driver pod cannot reach its own
# headless service — an image/network problem, not a problem with the job.
#
# Point IMAGE at the image under test, and rerun with IMAGE set to the cluster's
# known-good Spark image to compare.

set -euo pipefail

: "${IMAGE:?set IMAGE to the container image under test}"
: "${K8S_MASTER:?set K8S_MASTER, e.g. k8s://https://api.example.internal}"
: "${K8S_NAMESPACE:=spark-workload}"
: "${SERVICE_ACCOUNT:=spark-submit-sa}"
: "${OAUTH_TOKEN_FILE:=./spark-tok.jwt}"
: "${S3_ENDPOINT:?set S3_ENDPOINT, e.g. https://minio.example.internal}"
: "${S3_BUCKET:=s3a://spark-k8s}"
: "${YUNIKORN_QUEUE:=root.default}"
: "${EXAMPLES_JAR:=local:///opt/spark/examples/jars/spark-examples_2.12-3.5.8.jar}"
: "${PI_PARTITIONS:=100}"

spark-submit \
  --master "${K8S_MASTER}" \
  --deploy-mode cluster \
  --name spark-pi-image-test \
  --class org.apache.spark.examples.SparkPi \
  --conf spark.kubernetes.container.image="${IMAGE}" \
  --conf spark.kubernetes.namespace="${K8S_NAMESPACE}" \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName="${SERVICE_ACCOUNT}" \
  --conf spark.kubernetes.authenticate.submission.oauthTokenFile="${OAUTH_TOKEN_FILE}" \
  --conf spark.driver.memory=4g \
  --conf spark.driver.cores=2 \
  --conf spark.executor.memory=4g \
  --conf spark.executor.cores=2 \
  --conf spark.executor.instances=2 \
  --conf spark.kubernetes.driver.secretKeyRef.AWS_ACCESS_KEY_ID=minio-secret:AWS_ACCESS_KEY_ID \
  --conf spark.kubernetes.driver.secretKeyRef.AWS_SECRET_ACCESS_KEY=minio-secret:AWS_SECRET_ACCESS_KEY \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_ACCESS_KEY_ID=minio-secret:AWS_ACCESS_KEY_ID \
  --conf spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY=minio-secret:AWS_SECRET_ACCESS_KEY \
  --conf spark.kubernetes.scheduler.name=yunikorn \
  --conf spark.kubernetes.driver.annotation.yunikorn.apache.org/queue="${YUNIKORN_QUEUE}" \
  --conf spark.kubernetes.executor.annotation.yunikorn.apache.org/queue="${YUNIKORN_QUEUE}" \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir="${S3_BUCKET}/logs" \
  --conf spark.kubernetes.file.upload.path="${S3_BUCKET}/spark-staging" \
  --conf spark.hadoop.fs.s3a.endpoint="${S3_ENDPOINT}" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  "${EXAMPLES_JAR}" \
  "${PI_PARTITIONS}"

# spark_minio_dag.py
#
# Upload this file to MinIO:
#   aws --endpoint-url http://localhost:9000 s3 cp \
#     spark_minio_dag.py s3://airflow-dags/dags/
#
# The mc-sync sidecar picks it up within 30 seconds.
# No restart. No docker cp. No hostPath.
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)

# ── SparkApplication spec — script read directly from MinIO via s3a:// ────────
SPARK_APPLICATION_SPEC = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "word-count-minio-{{ ds_nodash }}",
        "namespace": "spark-job",
    },
    "spec": {
        "type": "Python",
        "pythonVersion": "3",
        "mode": "cluster",
        "image": "spark-minio-s3a-custom:3.5.0",
        "imagePullPolicy": "IfNotPresent",
        # Script lives in MinIO — Spark downloads it at runtime via S3A
        "mainApplicationFile": "s3a://spark-jobs/test_spark_job.py",
        "sparkVersion": "3.5.0",
        "sparkConf": {
            "spark.hadoop.fs.s3a.endpoint":
                "http://minio-service.minio.svc.cluster.local:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl":
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.access.key": " minio-admin",
            "spark.hadoop.fs.s3a.secret.key": "minio-secret-password",
            "spark.sql.shuffle.partitions": "2",
            "spark.sql.adaptive.enabled": "true",
            "spark.eventLog.enabled": "false",
            "spark.driver.extraClassPath": "/opt/spark/jars/*",
            "spark.executor.extraClassPath": "/opt/spark/jars/*",
        },
        "driver": {
            "cores": 1,
            "memory": "512m",
            "serviceAccount": "spark-driver-sa",
        },
        "executor": {
            "cores": 1,
            "instances": 1,
            "memory": "512m",
        },
        "restartPolicy": {
            "type": "OnFailure",
            "onFailureRetries": 2,
            "onFailureRetryInterval": 10,
        },
    },
}

# ── DAG ───────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_minio_submit_v3",
    description="Submit Spark job — script and DAG both stored in MinIO",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "minio", "kubernetes"],
) as dag:

    submit = SparkKubernetesOperator(
        task_id="submit_spark_job",
        namespace="spark-job",
        application_file=SPARK_APPLICATION_SPEC,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    monitor = SparkKubernetesSensor(
        task_id="monitor_spark_job",
        namespace="spark-job",
        application_name="word-count-minio-{{ ds_nodash }}",
        kubernetes_conn_id="kubernetes_default",
        attach_log=True,
        poke_interval=30,
        timeout=1800,
    )

    submit >> monitor

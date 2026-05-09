# spark_minio_dag.py
#
# Upload this file to MinIO:
#   aws --endpoint-url http://localhost:9000 s3 cp \
#     spark_minio_dag.py s3://airflow-dags/dags/
#
# The mc-sync sidecar picks it up within 30 seconds.
# No restart. No docker cp. No hostPath.
from __future__ import annotations

from spark_apps.word_count import get_spark_app
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)



# ── DAG ───────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="spark_minio_submit_v2",
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
        application_file= get_spark_app(run_id=8992),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    

    submit 

# dags/spark_minio_dag.py
#
# WHAT CHANGED vs the old DAG:
#   • SparkApplication spec is NO LONGER defined inside this file
#   • It is imported from spark_app_config.py (uploaded to the same MinIO prefix)
#   • DAG only contains scheduling logic — clean separation of concerns
#   • Job name is templated with ds_nodash so every daily run gets a unique CRD
#
# UPLOAD BOTH FILES TO MINIO:
#   aws --endpoint-url http://localhost:9000 s3 cp \
#     spark_app_config.py s3://airflow-dags/dags/
#   aws --endpoint-url http://localhost:9000 s3 cp \
#     spark_minio_dag.py s3://airflow-dags/dags/
#
# UPLOAD SPARK SCRIPT TO MINIO:
#   aws --endpoint-url http://localhost:9000 s3 cp \
#     spark_script.py s3://spark-jobs/
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import (
    SparkKubernetesSensor,
)

# ── Import SparkApplication builder from the companion config module ──────────
# Both this file and spark_app_config.py live in /opt/airflow/dags/
# (synced from s3://airflow-dags/dags/ by the mc-sync sidecar).
from spark_apps.spark_app_config import build_spark_application, SPARK_NAMESPACE

# ── DAG-level constants ───────────────────────────────────────────────────────
DAG_ID          = "spark_minio_pipeline"
SCRIPT_KEY      = "spark_script.py"        # key inside s3://spark-jobs/
JOB_NAME_TPL    = f"data-pro-pipeline-{{{{ ds_nodash }}}}"  # unique per run

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    "email_on_failure": False,
}

with DAG(
    dag_id=DAG_ID,
    description=(
        "Read raw data from s3://data-pro/raw/, "
        "select [price, seller, status], "
        "write to s3://data-pro/process/"
    ),
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark", "minio", "data-pro"],
    params={
        # Allow overriding the script key at trigger time from the Airflow UI
        "script_key": Param(
            default=SCRIPT_KEY,
            type="string",
            description="S3 key of the Spark script inside s3://spark-jobs/",
        ),
    },
) as dag:

    # ── Task 1: Submit SparkApplication ──────────────────────────────────────
    # build_spark_application() is imported from spark_app_config.py.
    # The spec is built here at DAG parse time — clean, no inline JSON blob.
    submit_spark_job = SparkKubernetesOperator(
        task_id="submit_spark_job",
        namespace=SPARK_NAMESPACE,
        application_file=build_spark_application(
            job_name=JOB_NAME_TPL,
            script_key="{{ params.script_key }}",
        ),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )

    # ── Task 2: Poll until COMPLETED or FAILED ────────────────────────────────
    # attach_log=True streams driver logs into the Airflow task log panel.
    monitor_spark_job = SparkKubernetesSensor(
        task_id="monitor_spark_job",
        namespace=SPARK_NAMESPACE,
        application_name=JOB_NAME_TPL,
        kubernetes_conn_id="kubernetes_default",
        attach_log=True,
        poke_interval=30,
        timeout=1800,
    )

    submit_spark_job >> monitor_spark_job

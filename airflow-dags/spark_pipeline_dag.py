# 1. SPARK APP CONFIG OUTSIDE DAG
#    The SparkApplication spec is no longer hardcoded here.
#    It is stored in MinIO at s3://airflow-dags/spark-apps/pipeline.yaml
#    and fetched at runtime by build_spark_application_spec().
#    To change Spark config: edit the YAML → upload to MinIO → trigger DAG.



from airflow import DAG
from airflow.models.param import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from datetime import datetime, timedelta

import yaml 

## Constants 
MINIO_CONN_ID        = "minio_default"       # Airflow connection — see setup below
SPARK_APP_S3_BUCKET  = "airflow-dags"
SPARK_APP_S3_KEY     = "dags/spark_apps/spark-application.yaml"
SPARK_NAMESPACE      = "spark-job"


def build_spark_application_spec(run_date: str) -> dict:
    """
    Fetch the SparkApplication YAML from MinIO and inject run_date.

    This keeps ALL Spark configuration out of the DAG file.
    The DAG only injects the run_date to make the job name unique and partition the output correctly.

    MinIO path: s3://airflow-dags/spark-apps/pipeline.yaml
    """
    hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    obj  = hook.get_key(key=SPARK_APP_S3_KEY, bucket_name=SPARK_APP_S3_BUCKET)   ## Return S3 Object
    raw  = obj.get()["Body"].read().decode("utf-8")  # Read "Body" as bytes and decode to string

    # Replace the RUNDATE placeholder with the actual Airflow logical date
    raw = raw.replace("RUNDATE", run_date)

    spec = yaml.safe_load(raw)   # Convert YAML string to Python dict
    return spec



default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="spark_minio_pipeline_2345",
    description="Read data-pro/raw → select price/seller/status → write data-pro/processed",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "run_date": Param(
            default="{{ ds_nodash }}",
            type="string",
            description="Partition date injected into output path and job name",
        ),
    },
) as dag:
    submit = SparkKubernetesOperator(
        task_id="submit_spark_job",
        namespace=SPARK_NAMESPACE, 
        application_file=build_spark_application_spec("{{ ds_nodash }}"),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
    )


    monitor = SparkKubernetesSensor(
        task_id="monitor_spark_job",
        namespace=SPARK_NAMESPACE,
        application_name="spark-minio-pipeline-{{ ds_nodash }}",
        kubernetes_conn_id="kubernetes_default",
        attach_log=True,
        poke_interval=30,
        timeout=3600,
    )

    submit >> monitor



# spark-app-config/spark_app_config.py
#
# SparkApplication spec defined OUTSIDE the DAG file.
#
# WHY SEPARATE?
#   • DAG file stays clean — only scheduling logic, no infrastructure config
#   • SparkApplication spec can be versioned, tested, and reused independently
#   • Multiple DAGs can import the same spec without copy-pasting
#   • Changing Spark resources (memory, cores, image) never requires touching DAGs
#
# UPLOAD TO MINIO (alongside the DAG):
#   aws --endpoint-url http://localhost:9000 s3 cp \
#     spark_app_config.py s3://airflow-dags/dags/
#
# The DAG imports this module at parse time — both files must be in
# /opt/airflow/dags/ (synced from the same MinIO bucket prefix).

# ── MinIO / S3A connection constants ─────────────────────────────────────────
MINIO_ENDPOINT      = "http://minio-service.minio.svc.cluster.local:9000"
MINIO_ACCESS_KEY    = "minio-admin"
MINIO_SECRET_KEY    = "minio-secret-password"

# ── Bucket layout ─────────────────────────────────────────────────────────────
SCRIPTS_BUCKET      = "spark-jobs"           # where .py scripts are stored
DATA_BUCKET         = "data-pro"             # where input/output data lives
RAW_PREFIX          = "raw"                  # s3a://data-pro/raw/
PROCESSED_PREFIX    = "process"              # s3a://data-pro/process/

# ── Spark image ───────────────────────────────────────────────────────────────
SPARK_IMAGE         = "spark-minio-s3a-custom:3.5.0"
SPARK_NAMESPACE     = "spark-job"
SPARK_SERVICE_ACCOUNT = "spark-driver-sa"

# ── Shared S3A sparkConf — applied to every SparkApplication ─────────────────
# Centralised here so all jobs use identical S3A settings.
# SimpleAWSCredentialsProvider reads fs.s3a.access.key / fs.s3a.secret.key
# directly — no env vars needed.
BASE_SPARK_CONF = {
    "spark.hadoop.fs.s3a.endpoint":              MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.path.style.access":     "true",
    "spark.hadoop.fs.s3a.impl":                  "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider":
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.hadoop.fs.s3a.access.key":            MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key":            MINIO_SECRET_KEY,
    "spark.sql.shuffle.partitions":              "2",
    "spark.sql.adaptive.enabled":               "true",
    "spark.eventLog.enabled":                   "false",
    "spark.driver.extraClassPath":              "/opt/spark/jars/*",
    "spark.executor.extraClassPath":            "/opt/spark/jars/*",
}


def build_spark_application(
    job_name: str,
    script_key: str,
    extra_spark_conf: dict = None,
    driver_memory: str = "512m",
    driver_cores: int = 1,
    executor_memory: str = "512m",
    executor_cores: int = 1,
    executor_instances: int = 1,
) -> dict:
    """
    Build a SparkApplication CRD spec dict.

    Parameters
    ----------
    job_name        : Kubernetes resource name for this SparkApplication.
                      Must be unique per run — pass a templated name from the DAG,
                      e.g. "data-pro-pipeline-{{ ds_nodash }}".
    script_key      : S3 key of the Python script inside SCRIPTS_BUCKET,
                      e.g. "spark_script.py".
    extra_spark_conf: Optional dict of additional sparkConf keys that override
                      or extend BASE_SPARK_CONF for this specific job.
    driver_memory   : Spark driver memory string, e.g. "512m" or "2g".
    driver_cores    : Number of driver cores.
    executor_memory : Spark executor memory string.
    executor_cores  : Number of executor cores.
    executor_instances: Number of executor pods to launch.

    Returns
    -------
    dict  — ready to pass to SparkKubernetesOperator(application_file=...)
    """
    script_uri = f"s3a://{SCRIPTS_BUCKET}/{script_key}"

    # Merge base conf with any job-specific overrides
    spark_conf = {**BASE_SPARK_CONF, **(extra_spark_conf or {})}

    # Pass bucket/prefix info to the script via sparkConf so the script
    # never has hardcoded paths — it reads them from SparkContext conf.
    spark_conf["spark.app.minio.data.bucket"]     = DATA_BUCKET
    spark_conf["spark.app.minio.raw.prefix"]      = RAW_PREFIX
    spark_conf["spark.app.minio.process.prefix"]  = PROCESSED_PREFIX

    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": job_name,
            "namespace": SPARK_NAMESPACE,
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": SPARK_IMAGE,
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": script_uri,
            "sparkVersion": "3.5.0",
            "sparkConf": spark_conf,
            "driver": {
                "cores": driver_cores,
                "memory": driver_memory,
                "serviceAccount": SPARK_SERVICE_ACCOUNT,
            },
            "executor": {
                "cores": executor_cores,
                "instances": executor_instances,
                "memory": executor_memory,
            },
            "restartPolicy": {
                "type": "OnFailure",
                "onFailureRetries": 2,
                "onFailureRetryInterval": 10,
            },
        },
    }

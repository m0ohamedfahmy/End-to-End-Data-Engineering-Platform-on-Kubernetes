# 🚀 Spark · Airflow · MinIO — Cloud-Native Data Pipeline on Kubernetes

> **A fully automated, cloud-native data engineering stack on a local Kind cluster.**  
> One command deploys everything. Scripts and DAGs live in MinIO — no `hostPath`, no `docker cp`, no manual syncing.

[![Spark](https://img.shields.io/badge/Apache%20Spark-3.2.1-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-3.2.1-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![MinIO](https://img.shields.io/badge/MinIO-RELEASE.2024--01--16-C72E49?logo=minio&logoColor=white)](https://min.io)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-1.29-326CE5?logo=kubernetes&logoColor=white)](https://kubernetes.io)

---

## 📋 Table of Contents

- [Architecture](#-architecture)
- [Technology Stack](#-technology-stack)
- [Project Structure](#-project-structure)
- [Quick Start](#-quick-start)
- [setup.sh — Command Reference](#-setupsh--command-reference)
- [Kubernetes Namespaces](#-kubernetes-namespaces)
- [MinIO Buckets](#-minio-buckets)
- [Custom Spark Image](#-custom-spark-image)
- [Data Pipeline](#-data-pipeline)
- [Airflow DAG](#-airflow-dag)
- [Live Update Workflow](#-live-update-workflow)
- [Airflow Connection Setup](#-airflow-connection-setup)
- [Verification Commands](#-verification-commands)
- [Troubleshooting](#-troubleshooting)
- [Bugs Fixed During Development](#-bugs-fixed-during-development)

---

## 🏗 Architecture


### Data flow at a glance

| Flow | Path |
|------|------|
| 📝 Script delivery | `pipeline_job.py` → `s3://spark-jobs/` → Spark downloads via `s3a://` at job start |
| 📅 DAG delivery | `spark_pipeline_dag.py` → `s3://airflow-dags/dags/` → `mc-sync` sidecar mirrors every 10s |
| ⚙️ Spark config | `pipeline.yaml` → `s3://airflow-dags/spark-apps/` → DAG fetches at task runtime |
| 📥 Data input | Raw files in `s3://data-pro/raw/` |
| 📤 Data output | Processed Parquet in `s3://data-pro/processed/{run_date}/` |
| 📋 Task logs | Streamed to `s3://airflow-logs/logs/` in real time — survive pod deletion |

---

## 🛠 Technology Stack

| Component | Version | Role |
|-----------|---------|------|
| 🐳 Kind | 0.22+ | Kubernetes in Docker (local cluster) |
| ☸️ Kubernetes | 1.29 | Container orchestration |
| ⚡ Apache Spark | 3.5.0 | Distributed data processing |
| 🎛 Spark Operator | v2.5.0 | Manages `SparkApplication` CRDs |
| 🌬 Apache Airflow | 3.2.1 | Workflow orchestration |
| 🪣 MinIO | 2024-01-16 | S3-compatible object storage |
| 🔌 hadoop-aws | 3.3.4 | S3A filesystem connector |
| ☕ aws-java-sdk-bundle | 1.12.262 | AWS SDK (paired with hadoop-aws 3.3.4) |
| 🐘 PostgreSQL | 15-alpine | Airflow metadata database |
| 🔄 MinIO Client (mc) | 2024-01-13 | DAG sync sidecar |

---

## 📁 Project Structure

```
spark-minio-pipeline/
│
├── 🚀 setup.sh                              ← One-command full deployment
├── 🐳 Dockerfile                            ← Custom Spark image with S3A JARs
├── 🔑 aws.env                               ← MinIO credentials (not committed)
│
├── 📂 kind/
│   └── kind-cluster.yaml                    ← Kind cluster config
│
├── 📂 namespaces/
│   └── namespaces.yaml                      ← All 4 namespaces
│
├── 📂 rbac/
│   └── spark-rbac.yaml                      ← ServiceAccount, Role, RoleBinding
│
├── 📂 minio/manifests/
│   ├── 01-minio-secret.yaml
│   ├── 02-minio-pv-pvc.yaml
│   ├── 03-minio-deployment.yaml
│   └── 04-minio-service.yaml
│
├── 📂 spark-job/manifests/
│   ├── 01-spark-scripts-pv.yaml
│   ├── 02-minio-spark-secret.yaml           ← MinIO credentials in spark-job ns
│   └── 03-spark-application.yaml            ← Manual test SparkApplication
│   📂 spark-job/scripts
│        └── pipeline_job.py
│
│
│
├── 📂 spark-scripts/
│   └── pipeline_job.py                      ← PySpark job: raw → select → processed
│
├── 📂 airflow-dags/
│   └── spark_pipeline_dag.py                ← Airflow DAG: fetch → submit → monitor
│   └── 📂 airflow-dags/spark-apps
│        └── spark-application.yaml
│
└── 📂 airflow/
    └── airflow_remote_logs.yaml             ← Full Airflow deployment with remote logging
```

---

## ⚡ Quick Start

### 1 — Only Docker is required upfront

```bash
docker info   # must show Docker running
```

Everything else (kind, kubectl, helm, awscli, inotify-tools) is installed automatically.

### 2 — Create `aws.env`

```bash
cat > aws.env << 'EOF'
AWS_ACCESS_KEY_ID=YourName
AWS_SECRET_ACCESS_KEY=YourPass
AWS_DEFAULT_REGION=us-east-1
AWS_DEFAULT_OUTPUT=json
EOF
```

### 3 — Run the installer

```bash
sudo ./setup.sh
```

Takes ~5 minutes. Installs tools → creates cluster → deploys all components → uploads files → submits smoke test.

### 4 — Add Airflow connection

Open **http://localhost:8080** → **Admin → Connections → +**  
*(details in [Airflow Connection Setup](#-airflow-connection-setup))*

### 5 — Trigger the pipeline

```
Airflow UI → DAGs → spark_minio_pipeline_v1 → ▶ Trigger DAG
```

---

## 🎮 setup.sh — Command Reference

```bash
sudo ./setup.sh              # Full deploy (idempotent — safe to re-run)
sudo ./setup.sh --recreate   # Delete cluster and redeploy from scratch
sudo ./setup.sh --teardown   # Destroy everything cleanly
sudo ./setup.sh --sync       # Start live file-sync watchers only
```

### Steps performed by `./setup.sh`

| Step | Action |
|------|--------|
| **0** | Install `kind`, `kubectl`, `helm`, `awscli`, `inotify-tools` if missing |
| **0b** | Load `aws.env` and verify MinIO connectivity |
| **0c** | Create MinIO buckets (`spark-jobs`, `airflow-dags`, `airflow-logs`, `data-pro`) |
| **1** | Create Kind cluster (skip if already exists) |
| **2** | Apply all 4 namespaces |
| **3** | Install Spark Operator via Helm, patch to watch `spark-job` namespace |
| **4** | Apply RBAC (ServiceAccount, Role, RoleBinding) |
| **5** | Deploy MinIO and wait for Ready |
| **6** | Create MinIO credentials Secret in `spark-job` namespace |
| **7** | Build `spark-minio-s3a-custom:3.5.0` and load into Kind |
| **8** | Upload Spark script, DAG, and SparkApp YAML to MinIO |
| **9** | Deploy Airflow, wait for init job and all pods |
| **10** | Submit smoke-test `SparkApplication`, poll until `COMPLETED` |

## 🔄 Live Update Workflow


### Automatic (recommended during development)
### `--sync` mode

Starts two `inotifywait` background watchers that upload on every file save:

```bash
sudo ./setup.sh --sync

```

Press `Ctrl+C` to stop both watchers.

---

## 🗂 Kubernetes Namespaces

| Namespace | Contents |
|-----------|----------|
| `spark-operator` | Spark Operator controller + webhook |
| `minio` | MinIO StatefulSet + services |
| `spark-job` | Spark driver + executor pods |
| `airflow` | Scheduler, webserver, triggerer, PostgreSQL |

---

## 🪣 MinIO Buckets

| Bucket | Contents | Used by |
|--------|----------|---------|
| `spark-jobs` | `pipeline_job.py` | Spark driver (`s3a://`) |
| `airflow-dags` | `dags/` DAG files · `spark-apps/` YAML | Airflow mc-sync + DAG S3Hook |
| `data-pro` | `raw/` input · `processed/` output | PySpark script |
| `airflow-logs` | Task execution logs | Airflow remote logging |

| | Value |
|-|-------|
| 🌐 API | http://localhost:9000 |
| 🖥 Console | http://localhost:9001 |

---

## 🐳 Custom Spark Image

The base `apache/spark:3.5.0` has no S3A connector. We bake it in at build time:

```dockerfile
FROM apache/spark:3.5.0-scala2.12-java11-python3-ubuntu
USER root

RUN mkdir -p /opt/spark/jars

RUN curl -fL -o /opt/spark/jars/hadoop-aws.jar \
    https://repo1.maven.org/.../hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

RUN curl -fL -o /opt/spark/jars/aws-java-sdk-bundle.jar \
    https://repo1.maven.org/.../aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN chown -R 185:185 /opt/spark/jars/

RUN mkdir -p /opt/spark/conf && \
    echo "spark.driver.extraClassPath   /opt/spark/jars/*" >> /opt/spark/conf/spark-defaults.conf && \
    echo "spark.executor.extraClassPath /opt/spark/jars/*" >> /opt/spark/conf/spark-defaults.conf

ENV SPARK_CLASSPATH="/opt/spark/jars/*"
USER 185
```

> ⚠️ **Version pairing:** `hadoop-aws 3.3.4` requires `aws-java-sdk-bundle 1.12.262`.  
> Mismatching versions causes `NoSuchMethodError` at runtime.



---

## 🔍 Verification Commands

```bash


# Spark Operator
kubectl get pods -n spark-operator
kubectl logs deploy/spark-operator-controller -n spark-operator | tail -20

# Minio
kubectl get pods -n minio

# Buckets
curl -s http://localhost:9000/minio/health/ready
aws --endpoint-url http://localhost:9000 s3 ls

# Airflow
kubectl get pods -n airflow
kubectl logs deploy/airflow-scheduler -n airflow -c mc-sync --tail=20
kubectl exec -n airflow deploy/airflow-scheduler -c scheduler -- airflow dags list

# Spark job
kubectl get sparkapplication -n spark-job 

```


---

## 🩺 Troubleshooting

| Symptom | Diagnosis | Fix |
|---------|-----------|-----|
| `SparkApplication` has no STATUS | RBAC forbidden in spark-job ns | `kubectl apply -f rbac/spark-rbac.yaml` + restart operator |
| `ClassNotFoundException: SimpleAWSCredentialsProvider` | Custom image not loaded | `kind load docker-image spark-minio-s3a-custom:3.5.0 --name spark-dev` |
| `UnknownHostException: minio-service` | Wrong service name in endpoint | `kubectl get svc -n minio` and use exact name |
| Broken DAG: `botocore 404` | Old DAG calls MinIO at parse time | Upload fixed DAG from `airflow-dags/spark_pipeline_dag.py` |
| `TypeError: string indices must be integers` | Same as above | Same fix |
| `data-pro/processed/` empty | Input missing or credentials wrong | Check `s3://data-pro/raw/` and Secret values |
| `Could not read served logs` | `minio_default` connection missing | Create connection + `s3 mb s3://airflow-logs` |

---

## 🐛 Bugs Fixed During Development

| # | Component | Bug | Fix |
|---|-----------|-----|-----|
| 1 | Spark Operator | Watched `default` not `spark-job` | `--set controller.namespaces={spark-job}` + patch |
| 2 | Spark Operator | RBAC forbidden in `spark-job` ns | Added `Role` + `RoleBinding` |
| 3 | SparkApplication | `hostPath` volumes silently ignored in v2.5.x | Replaced with `s3a://` — no volumes needed |
| 4 | MinIO DNS | Wrong service name `minio` vs `minio-service` | Confirmed with `kubectl get svc -n minio` |
| 5 | Credentials provider | `EnvironmentVariableCredentialsProvider` not in hadoop-aws 3.3.4 | `SimpleAWSCredentialsProvider` + `fs.s3a.access.key` in `sparkConf` |
| 6 | Airflow logs | `Could not read served logs` after pod deletion | Remote logging to MinIO |


---

<div align="center">

**Built with ❤️ — Spark · Airflow · MinIO · Kubernetes**

*Edit locally → upload to MinIO → cluster picks it up. No restarts. No rebuilds.*

</div>
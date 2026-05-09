#!/usr/bin/env bash
# =============================================================================
#  🚀  setup.sh  —  Spark · Airflow · MinIO on Kind
#  Full deployment from zero to running pipeline in one command.
# =============================================================================
#
#  Usage:
#    sudo ./setup.sh              # full deploy (idempotent — safe to re-run)
#    sudo ./setup.sh --recreate   # delete & recreate the Kind cluster first
#    sudo ./setup.sh --teardown   # destroy everything cleanly
#    sudo ./setup.sh --sync       # start live file-sync watchers only
#
#  Requirements:  Docker must already be installed and running.
#  Everything else (kind, kubectl, helm, awscli, inotify-tools) is installed
#  automatically by this script.
# =============================================================================
set -euo pipefail

# ── Root check ────────────────────────────────────────────────────────────────
if [[ "$EUID" -ne 0 ]]; then
  echo "❌  Please run as root:  sudo ./setup.sh"
  exit 1
fi

# ── Colours & logging helpers ─────────────────────────────────────────────────
RED='\033[0;31m'; GRN='\033[0;32m'; YLW='\033[1;33m'
CYN='\033[0;36m'; BLD='\033[1m'; NC='\033[0m'

info()   { echo -e "${CYN}ℹ️  $*${NC}"; }
ok()     { echo -e "${GRN}✅  $*${NC}"; }
warn()   { echo -e "${YLW}⚠️  $*${NC}"; }
die()    { echo -e "${RED}💥  ERROR: $*${NC}" >&2; exit 1; }
step()   { echo -e "\n${BLD}${GRN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}";
           echo -e "${BLD}${GRN}  🔧  $*${NC}";
           echo -e "${BLD}${GRN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; }

# ── Configuration — edit these to match your environment ─────────────────────
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_NAME="spark-dev"
KIND_CONFIG="${REPO_ROOT}/kind/kind-cluster.yaml"
CONTEXT="kind-${CLUSTER_NAME}"
MINIO_ENDPOINT="http://localhost:9000"
AWS_ENV_FILE="${REPO_ROOT}/aws.env"

SCRIPTS_LOCAL="/home/ninja/spark-kind/spark-job/scripts"
DAGS_LOCAL="/home/ninja/spark-kind/airflow-dags"

# ── Welcome banner ────────────────────────────────────────────────────────────
echo -e "
${GRN}${BLD}
 ╔══════════════════════════════════════════════════════════╗
 ║   🚀  Spark · Airflow · MinIO  on  Kubernetes (Kind)    ║
 ║          Cloud-Native Data Pipeline Installer           ║
 ╚══════════════════════════════════════════════════════════╝
${NC}"

# ─────────────────────────────────────────────────────────────────────────────
#  --teardown  :  destroy everything and exit
# ─────────────────────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--teardown" ]]; then
  step "💣  Teardown — removing all resources"
  kubectl --context "$CONTEXT" delete sparkapplication --all -n spark-job --ignore-not-found || true
  kubectl --context "$CONTEXT" delete -f spark-job/manifests/ --ignore-not-found            || true
  kubectl --context "$CONTEXT" delete -f minio/manifests/     --ignore-not-found            || true
  kubectl --context "$CONTEXT" delete -f rbac/                --ignore-not-found            || true
  kubectl --context "$CONTEXT" delete -f namespaces/          --ignore-not-found            || true
  helm --kube-context "$CONTEXT" uninstall spark-operator \
    -n spark-operator --ignore-not-found 2>/dev/null                                        || true
  kind delete cluster --name "$CLUSTER_NAME" 2>/dev/null                                    || true
  ok "Teardown complete. Cluster '${CLUSTER_NAME}' deleted."
  exit 0
fi

# ─────────────────────────────────────────────────────────────────────────────
#  --recreate  :  delete and recreate the Kind cluster, then continue
# ─────────────────────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--recreate" ]]; then
  warn "Deleting existing cluster '${CLUSTER_NAME}'..."
  kind delete cluster --name "$CLUSTER_NAME" 2>/dev/null || true
  ok "Cluster deleted — will recreate in Step 1."
fi

# ─────────────────────────────────────────────────────────────────────────────
#  --sync  :  start live file-sync watchers only (no cluster setup)
# ─────────────────────────────────────────────────────────────────────────────
if [[ "${1:-}" == "--sync" ]]; then
  step "🔄  Live Sync — inotify watchers for scripts & DAGs"
  command -v inotifywait >/dev/null 2>&1 || apt-get install -y inotify-tools -q
  info "Watching ${SCRIPTS_LOCAL} → s3://spark-jobs/"
  info "Watching ${DAGS_LOCAL}    → s3://airflow-dags/dags/"
  info "Press Ctrl+C to stop."
  # Run both watchers in parallel background jobs
  (while inotifywait -r -e modify,create,delete "${SCRIPTS_LOCAL}" 2>/dev/null; do
    aws --endpoint-url "$MINIO_ENDPOINT" s3 sync "${SCRIPTS_LOCAL}" s3://spark-jobs/ --delete
    ok "Scripts synced → s3://spark-jobs/"
  done) &
  (while inotifywait -r -e modify,create,delete "${DAGS_LOCAL}" 2>/dev/null; do
    aws --endpoint-url "$MINIO_ENDPOINT" s3 sync "${DAGS_LOCAL}/" s3://airflow-dags/dags/ --delete
    ok "DAGs synced → s3://airflow-dags/dags/"
  done) &
  # Wait for both background jobs — Ctrl+C kills them cleanly
  wait
  exit 0
fi

# =============================================================================
#  STEP 0 — Install prerequisites
# =============================================================================
step "Step 0 — Installing Prerequisites"

# Docker check — must already be installed
command -v docker >/dev/null 2>&1 || die "Docker not found. Install Docker first: https://docs.docker.com/get-docker/"
ok "Docker: $(docker --version)"

# kind
if ! command -v kind >/dev/null 2>&1; then
  info "Installing kind..."
  curl -fsSLo /tmp/kind "https://kind.sigs.k8s.io/dl/latest/kind-linux-amd64"
  chmod +x /tmp/kind
  mv /tmp/kind /usr/local/bin/kind
fi
ok "kind: $(kind version)"

# kubectl
if ! command -v kubectl >/dev/null 2>&1; then
  info "Installing kubectl..."
  snap install kubectl --classic
fi
ok "kubectl: $(kubectl version --client --short 2>/dev/null || kubectl version --client)"

# helm
if ! command -v helm >/dev/null 2>&1; then
  info "Installing helm..."
  snap install helm --classic
fi
ok "helm: $(helm version --short)"

# awscli
if ! command -v aws >/dev/null 2>&1; then
  info "Installing awscli..."
  apt-get update -q && apt-get install -y awscli -q
fi
ok "aws: $(aws --version)"

# inotify-tools
if ! command -v inotifywait >/dev/null 2>&1; then
  info "Installing inotify-tools..."
  apt-get install -y inotify-tools -q
fi
ok "inotifywait: installed"

# =============================================================================
#  STEP 0b — Load AWS credentials
# =============================================================================
step "Step 0b — Loading AWS / MinIO Credentials"

[[ -f "$AWS_ENV_FILE" ]] || die "aws.env not found at ${AWS_ENV_FILE}. Create it with AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY."
set -a; source "$AWS_ENV_FILE"; set +a

# Quick connectivity test — MinIO must be running and reachable
info "Testing MinIO connectivity at ${MINIO_ENDPOINT}..."
aws --endpoint-url "$MINIO_ENDPOINT" s3 ls >/dev/null 2>&1 \
  || die "Cannot reach MinIO at ${MINIO_ENDPOINT}. Is MinIO running? Check aws.env credentials."
ok "MinIO reachable ✓"

# =============================================================================
#  STEP 0c — Create MinIO buckets (idempotent)
# =============================================================================
step "Step 0c — MinIO Buckets"

create_bucket() {
  local bucket="$1"
  if aws --endpoint-url "$MINIO_ENDPOINT" s3 ls "s3://${bucket}" >/dev/null 2>&1; then
    warn "Bucket 's3://${bucket}' already exists — skipping."
  else
    aws --endpoint-url "$MINIO_ENDPOINT" s3 mb "s3://${bucket}"
    ok "Created s3://${bucket}"
  fi
}

create_bucket "spark-jobs"
create_bucket "airflow-dags"
create_bucket "airflow-logs"
create_bucket "data-pro"

# =============================================================================
#  STEP 1 — Kind cluster
# =============================================================================
step "Step 1 — Kind Cluster '${CLUSTER_NAME}'"

if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  ok "Cluster '${CLUSTER_NAME}' already exists — skipping creation."
  warn "Run with --recreate to rebuild it from scratch."
else
  info "Creating Kind cluster from ${KIND_CONFIG}..."
  cd "$REPO_ROOT"
  kind create cluster --name "$CLUSTER_NAME" --config "$KIND_CONFIG"
  ok "Cluster '${CLUSTER_NAME}' created."
fi

kubectl config use-context "$CONTEXT"
kubectl --context "$CONTEXT" cluster-info
ok "Active context: ${CONTEXT}"

# =============================================================================
#  STEP 2 — Namespaces
# =============================================================================
step "Step 2 — Namespaces"

kubectl --context "$CONTEXT" apply -f namespaces/namespaces.yaml
kubectl --context "$CONTEXT" wait \
  --for=jsonpath='{.status.phase}'=Active \
  namespace/spark-operator namespace/minio namespace/spark-job namespace/airflow \
  --timeout=30s
ok "Namespaces ready: spark-operator · minio · spark-job · airflow"

# =============================================================================
#  STEP 3 — Spark Operator
# =============================================================================
step "Step 3 — Spark Operator (Helm v2.5.0)"

helm repo add spark-operator https://kubeflow.github.io/spark-operator 2>/dev/null || true
helm repo update spark-operator

helm upgrade --install spark-operator spark-operator/spark-operator \
  --kube-context  "$CONTEXT" \
  --namespace     spark-operator \
  --set webhook.enable=true \
  --set sparkJobNamespace=spark-job \
  --set "controller.namespaces={spark-job}" 

# Create the spark SA only if it doesn't exist
kubectl --context "$CONTEXT" get serviceaccount spark -n spark-job >/dev/null 2>&1 \
  || kubectl --context "$CONTEXT" create serviceaccount spark -n spark-job

# Create the clusterrolebinding only if it doesn't exist
kubectl --context "$CONTEXT" get clusterrolebinding spark-role >/dev/null 2>&1 \
  || kubectl --context "$CONTEXT" create clusterrolebinding spark-role \
       --clusterrole=edit \
       --serviceaccount=spark-job:spark \
       --namespace=spark-job

# Patch the controller to enforce --namespaces=spark-job
# (Helm sometimes ignores this flag — patching ensures it is set)
kubectl --context "$CONTEXT" patch deployment spark-operator-controller \
  -n spark-operator --type='json' -p='[
    {"op": "replace", "path": "/spec/template/spec/containers/0/args",
     "value": [
       "controller", "start",
       "--namespaces=spark-job",
       "--zap-log-level=info",
       "--zap-encoder=console",
       "--controller-threads=10",
       "--enable-ui-service=true"
     ]}
  ]'

kubectl --context "$CONTEXT" rollout restart deploy/spark-operator-controller -n spark-operator
kubectl --context "$CONTEXT" rollout status  deploy/spark-operator-controller \
  -n spark-operator 
ok "Spark Operator ready and watching namespace: spark-job"

# =============================================================================
#  STEP 4 — RBAC
# =============================================================================
step "Step 4 — RBAC"

kubectl --context "$CONTEXT" apply -f rbac/spark-rbac.yaml
ok "RBAC applied (spark-driver-sa · spark-operator-role)"

# =============================================================================
#  STEP 5 — MinIO
# =============================================================================
step "Step 5 — MinIO Deployment"

kubectl --context "$CONTEXT" apply -f minio/manifests/01-minio-secret.yaml
kubectl --context "$CONTEXT" apply -f minio/manifests/02-minio-pv-pvc.yaml
kubectl --context "$CONTEXT" apply -f minio/manifests/03-minio-deployment.yaml
kubectl --context "$CONTEXT" apply -f minio/manifests/04-minio-service.yaml

info "Waiting for MinIO pod to be Ready..."
kubectl --context "$CONTEXT" wait pod \
  --selector app=minio \
  --for=condition=Ready \
  --namespace minio 
ok "MinIO running → API: ${MINIO_ENDPOINT}  Console: http://localhost:9001"

# =============================================================================
#  STEP 6 — Spark job resources
# =============================================================================
step "Step 6 — Spark Job Secrets"

kubectl --context "$CONTEXT" apply -f spark-job/manifests/02-minio-spark-secret.yaml
ok "MinIO credentials secret applied in spark-job namespace"

# =============================================================================
#  STEP 7 — Build & load custom Spark image
# =============================================================================
step "Step 7 — Custom Spark Image (spark-minio-s3a-custom:3.5.0)"

info "Building Docker image with S3A JARs..."
docker build -t spark-minio-s3a-custom:3.5.0 "${REPO_ROOT}"
ok "Image built: spark-minio-s3a-custom:3.5.0"

info "Loading image into Kind cluster '${CLUSTER_NAME}'..."
kind load docker-image spark-minio-s3a-custom:3.5.0 --name "$CLUSTER_NAME"
ok "Image loaded into cluster"

# Verify image is available inside the node
NODE=$(kubectl --context "$CONTEXT" get nodes -o jsonpath='{.items[0].metadata.name}')
docker exec "$NODE" crictl images 2>/dev/null | grep -q "spark-minio-s3a-custom" \
  && ok "Image verified inside node: ${NODE}" \
  || warn "Image not yet visible in crictl — it may still be loading."

# =============================================================================
#  STEP 8 — Upload files to MinIO
# =============================================================================
# step "Step 8 — Upload Scripts, DAGs & Config to MinIO"

# MINIO_CMD="aws --endpoint-url ${MINIO_ENDPOINT}"

# info "Uploading Spark script → s3://spark-jobs/"
# $MINIO_CMD s3 cp "${REPO_ROOT}/spark-scripts/pipeline_job.py" s3://spark-jobs/pipeline_job.py
# ok "pipeline_job.py uploaded"

# info "Uploading SparkApplication config → s3://airflow-dags/spark-apps/"
# $MINIO_CMD s3 cp "${REPO_ROOT}/manifests/spark-application.yaml" \
#   s3://airflow-dags/spark-apps/pipeline.yaml
# ok "pipeline.yaml uploaded"

# info "Uploading Airflow DAG → s3://airflow-dags/dags/"
# $MINIO_CMD s3 cp "${REPO_ROOT}/airflow-dags/spark_pipeline_dag.py" \
#   s3://airflow-dags/dags/spark_pipeline_dag.py
# ok "spark_pipeline_dag.py uploaded"

# =============================================================================
#  STEP 9 — Deploy Airflow
# =============================================================================
step "Step 9 — Apache Airflow"

kubectl --context "$CONTEXT" apply -f airflow/airflow_remote_logs.yaml

ok "Airflow ready → UI: http://localhost:8080  (admin / admin)"

# =============================================================================
#  STEP 10 — Submit test SparkApplication (smoke test)
# =============================================================================
step "Step 10 — Smoke Test: Submit SparkApplication"

kubectl --context "$CONTEXT" \
  delete sparkapplication word-count-minio -n spark-job --ignore-not-found
kubectl --context "$CONTEXT" \
  apply -f spark-job/manifests/03-spark-application.yaml

info "Waiting for SparkApplication to reach RUNNING state..."
for i in $(seq 1 24); do
  STATUS=$(kubectl --context "$CONTEXT" get sparkapplication word-count-minio \
    -n spark-job -o jsonpath='{.status.applicationState.state}' 2>/dev/null || echo "")
  if [[ "$STATUS" == "COMPLETED" ]]; then
    ok "SparkApplication COMPLETED successfully! 🎉"
    break
  elif [[ "$STATUS" == "FAILED" ]]; then
    die "SparkApplication FAILED. Run: kubectl logs word-count-minio-driver -n spark-job"
  fi
  info "Status: ${STATUS:-PENDING} — waiting... (${i}/24)"
  sleep 10
done

# =============================================================================
#  Done — print summary
# =============================================================================
echo -e "
${GRN}${BLD}
 ╔══════════════════════════════════════════════════════════╗
 ║             🎉  Deployment Complete!                     ║
 ╠══════════════════════════════════════════════════════════╣
 ║                                                          ║
 ║  🌬  Airflow UI    →  http://localhost:8080              ║
 ║                        admin / admin                     ║
 ║                                                          ║
 ║  🪣  MinIO API     →  http://localhost:9000              ║
 ║  🖥  MinIO Console →  http://localhost:9001              ║
 ║                        minioadmin / minioadmin123        ║
 ║                                                          ║
 ║  📋  Next steps:                                         ║
 ║  1. Open Airflow UI                                      ║
 ║  2. Admin → Connections → add 'minio_default'            ║
 ║  3. Trigger DAG: spark_minio_pipeline_v1                 ║
 ║                                                          ║
 ║  🔄  Live sync (in a new terminal):                      ║
 ║     sudo ./setup.sh --sync                               ║
 ║                                                          ║
 ║  💣  Teardown:                                           ║
 ║     sudo ./setup.sh --teardown                           ║
 ╚══════════════════════════════════════════════════════════╝
${NC}"
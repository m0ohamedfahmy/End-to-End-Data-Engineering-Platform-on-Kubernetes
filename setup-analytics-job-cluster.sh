#!/usr/bin/env bash


set -euo pipefail

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ── Logging helpers ───────────────────────────────────────────────────────────
info()    { echo -e "${CYAN}  ➜${NC}  $*"; }
success() { echo -e "${GREEN}  ✔${NC}  $*"; }
warn()    { echo -e "${YELLOW}  ⚠${NC}  $*"; }
error()   { echo -e "${RED}  ✖${NC}  $*" >&2; exit 1; }

step() {
    local num=$1; shift
    echo ""
    echo -e "${BOLD}${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BOLD}${BLUE}  STEP ${num}: $*${NC}"
    echo -e "${BOLD}${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

banner() {
    echo -e "${BOLD}${BLUE}"
    echo "  ╔══════════════════════════════════════════════════════╗"
    echo "  ║     Spark Connect + JupyterHub  •  Two-Cluster       ║"
    echo "  ║     Analytics Platform on Kind                       ║"
    echo "  ╚══════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# ── Config ────────────────────────────────────────────────────────────────────
SPARK_DEV_CLUSTER="spark-dev"
ANALYTICS_CLUSTER="analytics"
SPARK_CONNECT_NS="spark-connect"
JUPYTERHUB_NS="jupyterhub"
NOTEBOOK_IMAGE="spark-connect-notebook:latest"
SERVER_IMAGE="spark-connect-s3a:1.0.0"
JUPYTERHUB_VERSION="3.3.8"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Mode ──────────────────────────────────────────────────────────────────────
MODE="${1:-deploy}"

# ── Teardown ──────────────────────────────────────────────────────────────────
if [[ "$MODE" == "--teardown" ]]; then
    banner
    echo -e "${RED}  Tearing down analytics cluster and spark-connect...${NC}\n"
    kubectl config use-context "kind-${ANALYTICS_CLUSTER}" 2>/dev/null && \
        helm uninstall jupyterhub -n "$JUPYTERHUB_NS" 2>/dev/null || true
    kind delete cluster --name "$ANALYTICS_CLUSTER" 2>/dev/null && \
        success "Analytics cluster deleted" || warn "Analytics cluster not found"
    kubectl config use-context "kind-${SPARK_DEV_CLUSTER}" 2>/dev/null && \
        helm uninstall my-spark-connect -n "$SPARK_CONNECT_NS" 2>/dev/null || true
    kubectl delete -f "${SCRIPT_DIR}/rbac/spark-connect-rbac.yaml" --ignore-not-found
    kubectl delete -f "${SCRIPT_DIR}/spark-connect/minio-spark-connect-secret.yaml" --ignore-not-found
    success "Teardown complete."
    exit 0
fi

# ── Status ────────────────────────────────────────────────────────────────────
if [[ "$MODE" == "--status" ]]; then
    echo -e "\n${BOLD}── spark-dev: Spark Connect ──────────────────────${NC}"
    kubectl get pods,svc -n "$SPARK_CONNECT_NS" --context "kind-${SPARK_DEV_CLUSTER}" 2>/dev/null || echo "(not found)"
    echo -e "\n${BOLD}── analytics: JupyterHub ─────────────────────────${NC}"
    kubectl get pods,svc -n "$JUPYTERHUB_NS" --context "kind-${ANALYTICS_CLUSTER}" 2>/dev/null || echo "(not found)"
    echo -e "\n${BOLD}── spark-dev: Executor pods ──────────────────────${NC}"
    kubectl get pods -n spark-job --context "kind-${SPARK_DEV_CLUSTER}" 2>/dev/null | grep -v "^NAME" || echo "(none running)"
    exit 0
fi

# ═════════════════════════════════════════════════════════════════════════════
#  DEPLOY
# ═════════════════════════════════════════════════════════════════════════════
banner

# ── Preflight ─────────────────────────────────────────────────────────────────
kubectl cluster-info --context "kind-${SPARK_DEV_CLUSTER}" &>/dev/null \
    || error "Cluster '${SPARK_DEV_CLUSTER}' not found. Deploy spark-dev first."
success "spark-dev cluster is reachable"
kubectl config use-context "kind-${SPARK_DEV_CLUSTER}"

# ─────────────────────────────────────────────────────────────────────────────
step 1 "Apply namespaces and RBAC on spark-dev"
# ─────────────────────────────────────────────────────────────────────────────
kubectl apply -f "${SCRIPT_DIR}/namespaces/namespaces.yaml"
success "Namespaces applied"

kubectl apply -f "${SCRIPT_DIR}/rbac/spark-connect-rbac.yaml"
success "RBAC applied (spark-connect → spark-job executor permissions)"

kubectl apply -f "${SCRIPT_DIR}/spark-connect/minio-spark-connect-secret.yaml"
success "MinIO secret applied in spark-connect namespace"

# ─────────────────────────────────────────────────────────────────────────────
step 2 "Build Spark Connect server image (aagumin + S3A JARs)"
# ─────────────────────────────────────────────────────────────────────────────
info "Building ${SERVER_IMAGE}..."
docker build \
    -f "${SCRIPT_DIR}/Dockerfile.spark-connect-server" \
    -t "$SERVER_IMAGE" \
    "${SCRIPT_DIR}"
success "Image ${SERVER_IMAGE} built"

info "Loading ${SERVER_IMAGE} into Kind cluster '${SPARK_DEV_CLUSTER}'..."
kind load docker-image "$SERVER_IMAGE" --name "$SPARK_DEV_CLUSTER"
success "Image loaded into ${SPARK_DEV_CLUSTER}"


# ─────────────────────────────────────────────────────────────────────────────
step 3 "Deploy Spark Connect server on spark-dev"
# ─────────────────────────────────────────────────────────────────────────────
helm upgrade --install my-spark-connect \
    "${SCRIPT_DIR}/spark-connect-custom-chart/spark-connect" \
    --namespace "$SPARK_CONNECT_NS" \
    -f "${SCRIPT_DIR}/spark-connect/spark-connect-values.yaml" 

success "Spark Connect deployed"


# ─────────────────────────────────────────────────────────────────────────────
step 4 "Create analytics Kind cluster"
# ─────────────────────────────────────────────────────────────────────────────
if kind get clusters 2>/dev/null | grep -q "^${ANALYTICS_CLUSTER}$"; then
    warn "Analytics cluster already exists — skipping creation"
else
    info "Creating analytics cluster..."
    kind create cluster \
        --config "${SCRIPT_DIR}/kind/kind-cluster-analytics.yaml" \
        --name "$ANALYTICS_CLUSTER"
    success "Analytics cluster created"
fi
kubectl config use-context "kind-${ANALYTICS_CLUSTER}"

# ─────────────────────────────────────────────────────────────────────────────
step 5 "Build and load notebook image into analytics cluster"
# ─────────────────────────────────────────────────────────────────────────────
info "Building ${NOTEBOOK_IMAGE}..."
docker build \
    -f "${SCRIPT_DIR}/jupyterhub/Dockerfile.notebook" \
    -t "$NOTEBOOK_IMAGE" \
    "${SCRIPT_DIR}/jupyterhub/"
success "Notebook image built"

info "Loading ${NOTEBOOK_IMAGE} into '${ANALYTICS_CLUSTER}'..."
kind load docker-image "$NOTEBOOK_IMAGE" --name "$ANALYTICS_CLUSTER"
success "Notebook image loaded"

# ─────────────────────────────────────────────────────────────────────────────
step 6 "Generate JupyterHub secret token"
# ─────────────────────────────────────────────────────────────────────────────
JH_CONFIG="${SCRIPT_DIR}/jupyterhub/jupyterhub-config.yaml"
PLACEHOLDER="REPLACE_WITH_OUTPUT_OF__openssl_rand_-hex_32"
if grep -q "$PLACEHOLDER" "$JH_CONFIG" 2>/dev/null; then
    TOKEN=$(openssl rand -hex 32)
    sed -i "s/${PLACEHOLDER}/${TOKEN}/" "$JH_CONFIG"
    success "proxy.secretToken generated and injected"
else
    info "proxy.secretToken already set — skipping"
fi

# ─────────────────────────────────────────────────────────────────────────────
step 7 "Deploy JupyterHub on analytics cluster"
# ─────────────────────────────────────────────────────────────────────────────
info "Setting SPARK_CONNECT_URL to sc://${SPARK_DEV_WORKER_IP}:32002"
helm upgrade --install jupyterhub jupyterhub/jupyterhub \
    --namespace "$JUPYTERHUB_NS" \
    --create-namespace \
    --version "$JUPYTERHUB_VERSION" \
    -f "$JH_CONFIG" \
    --set prePuller.hook.enabled=false \
    --set prePuller.continuous.enabled=false 
success "JupyterHub deployed on analytics cluster"

echo -e "\n${GREEN}  Deployment complete! Access JupyterHub at http://localhost:8888${NC}"
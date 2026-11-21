#!/usr/bin/env bash
# make k8s-up: local Kubernetes with autoscaling.
#   kind cluster -> metrics-server (for the API's CPU HPA) -> KEDA (for the
#   workers' Kafka-lag scaling) -> Hooklane image loaded -> manifests applied.
# Needs: docker, kind, kubectl (brew install kind kubectl).
set -euo pipefail
cd "$(dirname "$0")/../.."

CLUSTER=hooklane
KEDA_VERSION=v2.21.0
METRICS_SERVER_VERSION=v0.9.1

for tool in kind kubectl docker; do
  command -v "$tool" >/dev/null || { echo "missing $tool (macOS: brew install kind kubectl)"; exit 1; }
done

if ! kind get clusters | grep -qx "$CLUSTER"; then
  kind create cluster --name "$CLUSTER" --wait 120s
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null

echo "==> metrics-server $METRICS_SERVER_VERSION"
kubectl apply -f "https://github.com/kubernetes-sigs/metrics-server/releases/download/$METRICS_SERVER_VERSION/components.yaml" >/dev/null
# kind's kubelets use self-signed certificates.
kubectl -n kube-system patch deployment metrics-server --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]' >/dev/null 2>&1 || true

echo "==> KEDA $KEDA_VERSION"
kubectl apply --server-side -f "https://github.com/kedacore/keda/releases/download/$KEDA_VERSION/keda-${KEDA_VERSION#v}.yaml" >/dev/null
kubectl -n keda rollout status deploy/keda-operator --timeout=180s
kubectl -n keda rollout status deploy/keda-metrics-apiserver --timeout=180s
kubectl -n keda rollout status deploy/keda-admission --timeout=180s

echo "==> images"
docker image inspect hooklane:local >/dev/null 2>&1 || docker build -t hooklane:local .
for img in postgres:18.7-bookworm redpandadata/redpanda:v26.2.7 grafana/k6:1.8.3; do
  docker image inspect "$img" >/dev/null 2>&1 || docker pull -q "$img"
done
# `kind load docker-image` imports with --all-platforms, which fails for
# multi-arch images pulled for a single platform. Import into the node's
# containerd directly instead.
for img in hooklane:local postgres:18.7-bookworm redpandadata/redpanda:v26.2.7 grafana/k6:1.8.3; do
  echo "    loading $img"
  docker save "$img" | docker exec -i "$CLUSTER-control-plane" ctr --namespace=k8s.io images import --snapshotter=overlayfs - >/dev/null
done

echo "==> hooklane"
kubectl apply -f deploy/k8s/base/namespace.yaml >/dev/null
if ! kubectl -n hooklane get secret hooklane-secrets >/dev/null 2>&1; then
  KEY=$( (openssl rand -base64 32 2>/dev/null) || (head -c 32 /dev/urandom | base64) )
  kubectl -n hooklane create secret generic hooklane-secrets --from-literal=SECRET_ENCRYPTION_KEY="$KEY" >/dev/null
fi
kubectl apply -k deploy/k8s/overlays/kind
kubectl -n hooklane wait --for=condition=complete job/migrate --timeout=300s
for d in api relay scheduler worker sink; do kubectl -n hooklane rollout status deploy/$d --timeout=180s; done
kubectl -n hooklane get pods
echo ""
echo "Ready. Next: make k8s-demo   (API: kubectl -n hooklane port-forward svc/api 18080:8080)"

#!/usr/bin/env bash
# ==============================================================
# Build → push → deploy, in one command.
#   1. Builds both Docker images.
#   2. Pushes them to Azure Container Registry.
#   3. Fills in the <ACR_NAME> / image-tag placeholders in the manifests.
#   4. Applies everything to the cluster and waits for the rollout.
#
# Usage:
#   export ACR_NAME=fleetpipelineacr
#   ./scripts/deploy.sh
# ==============================================================

set -euo pipefail

: "${ACR_NAME:?Set ACR_NAME to your Azure Container Registry name}"

REGISTRY="${ACR_NAME}.azurecr.io"
# Tag images with the short git commit hash so every deploy is traceable
# back to the exact code that produced it. "latest" tags make rollbacks
# and debugging much harder — avoid them.
TAG="${TAG:-$(git rev-parse --short HEAD)}"

echo "==> Logging in to ACR"
az acr login --name "$ACR_NAME"

echo "==> Building and pushing ingestion image (tag: $TAG)"
docker build -t "${REGISTRY}/telemetry-ingestion:${TAG}" ingestion/
docker push "${REGISTRY}/telemetry-ingestion:${TAG}"

echo "==> Building and pushing processor image (tag: $TAG)"
docker build -t "${REGISTRY}/telemetry-processor:${TAG}" processor/
docker push "${REGISTRY}/telemetry-processor:${TAG}"

# The manifests in git contain placeholders. We render real values into
# a temporary copy and apply THAT, leaving the originals untouched —
# so a deploy never dirties your git working tree.
echo "==> Rendering manifests"
RENDER_DIR=$(mktemp -d)
cp -R k8s/. "$RENDER_DIR/"
find "$RENDER_DIR" -name '*.yaml' -exec \
  sed -i.bak -e "s|<ACR_NAME>|${ACR_NAME}|g" -e "s|:latest|:${TAG}|g" {} \;
find "$RENDER_DIR" -name '*.bak' -delete

echo "==> Applying Kubernetes manifests"
kubectl apply -f "$RENDER_DIR/namespace.yaml"
kubectl apply -f "$RENDER_DIR/configmap.yaml"
kubectl apply -f "$RENDER_DIR/redis/"
kubectl apply -f "$RENDER_DIR/kafka/"
kubectl apply -f "$RENDER_DIR/postgres/"
kubectl apply -f "$RENDER_DIR/ingestion/"
kubectl apply -f "$RENDER_DIR/processor/"

echo "==> Waiting for rollouts to complete"
kubectl rollout status deployment/ingestion -n telematics-pipeline --timeout=180s
kubectl rollout status deployment/processor -n telematics-pipeline --timeout=180s

rm -rf "$RENDER_DIR"

echo ""
echo "Deploy complete. Ingestion endpoint (give Azure a minute to assign the IP):"
kubectl get svc ingestion -n telematics-pipeline \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
echo ""

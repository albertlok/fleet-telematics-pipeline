#!/usr/bin/env bash
# ==============================================================
# One-time Azure infrastructure setup. Creates:
#   1. A Resource Group        (a folder for all related Azure resources)
#   2. Azure Container Registry (ACR — private Docker image storage)
#   3. An AKS cluster           (managed Kubernetes)
#   4. Azure Database for PostgreSQL Flexible Server (production DB)
#
# Usage:
#   export SUBSCRIPTION_ID=<your-subscription-id>
#   ./scripts/setup-aks.sh
#
# Every name below can be overridden via environment variables, e.g.:
#   LOCATION=westus2 ACR_NAME=myuniqueacr ./scripts/setup-aks.sh
# ==============================================================

# Safety flags: -e exit on first error, -u error on unset variables,
# -o pipefail catch failures inside pipelines.
set -euo pipefail

# ":?" aborts with a message if the variable isn't set.
: "${SUBSCRIPTION_ID:?Set SUBSCRIPTION_ID to your Azure subscription id}"

# "${VAR:-default}" = use $VAR if set, otherwise the default.
RESOURCE_GROUP="${RESOURCE_GROUP:-fleet-pipeline-rg}"
LOCATION="${LOCATION:-eastus}"
AKS_CLUSTER="${AKS_CLUSTER:-fleet-pipeline-aks}"
ACR_NAME="${ACR_NAME:-fleetpipelineacr}"   # must be globally unique, lowercase alphanumeric
PG_SERVER="${PG_SERVER:-fleet-telemetry-pg}"
PG_DB="${PG_DB:-telemetry}"
PG_USER="${PG_USER:-fleet}"
# Generate a random password unless one was provided.
PG_PASSWORD="${PG_PASSWORD:-$(openssl rand -base64 24)}"

echo "==> Setting active subscription"
az account set --subscription "$SUBSCRIPTION_ID"

echo "==> Creating resource group: $RESOURCE_GROUP"
az group create --name "$RESOURCE_GROUP" --location "$LOCATION"

echo "==> Creating Azure Container Registry: $ACR_NAME"
az acr create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$ACR_NAME" \
  --sku Standard \
  --admin-enabled false   # we use Azure AD identities, not admin passwords

echo "==> Creating AKS cluster: $AKS_CLUSTER (this takes ~10 minutes)"
az aks create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$AKS_CLUSTER" \
  --node-count 3 \
  --node-vm-size Standard_D4s_v3 \
  --enable-managed-identity \
  --attach-acr "$ACR_NAME" \
  --network-plugin azure \
  --enable-cluster-autoscaler \
  --min-count 3 \
  --max-count 10 \
  --generate-ssh-keys
# --attach-acr lets the cluster pull images from our registry without
# any extra credentials. --enable-cluster-autoscaler adds/removes VMs
# as the pod autoscalers (HPAs) demand more or less capacity.

echo "==> Downloading cluster credentials for kubectl"
az aks get-credentials \
  --resource-group "$RESOURCE_GROUP" \
  --name "$AKS_CLUSTER" \
  --overwrite-existing

echo "==> Creating PostgreSQL Flexible Server (production database)"
az postgres flexible-server create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$PG_SERVER" \
  --location "$LOCATION" \
  --admin-user "$PG_USER" \
  --admin-password "$PG_PASSWORD" \
  --sku-name Standard_D4s_v3 \
  --tier GeneralPurpose \
  --storage-size 128 \
  --version 16 \
  --public-access None
# --public-access None: the DB is only reachable from inside the Azure
# virtual network — never the public internet.

az postgres flexible-server db create \
  --resource-group "$RESOURCE_GROUP" \
  --server-name "$PG_SERVER" \
  --database-name "$PG_DB"

PG_FQDN=$(az postgres flexible-server show \
  --resource-group "$RESOURCE_GROUP" \
  --name "$PG_SERVER" \
  --query "fullyQualifiedDomainName" -o tsv)

DB_DSN="postgresql://${PG_USER}:${PG_PASSWORD}@${PG_FQDN}:5432/${PG_DB}?sslmode=require"

echo ""
echo "======================================================"
echo " Infrastructure provisioned successfully!"
echo "======================================================"
echo " ACR:     ${ACR_NAME}.azurecr.io"
echo " AKS:     $AKS_CLUSTER"
echo " PG FQDN: $PG_FQDN"
echo ""
echo " IMPORTANT — save the DB DSN somewhere safe (a password manager),"
echo " then create the Kubernetes secret:"
echo ""
echo "   DB_DSN: $DB_DSN"
echo ""
echo "   kubectl create secret generic pipeline-secrets \\"
echo "     -n telematics-pipeline \\"
echo "     --from-literal=DB_DSN='${DB_DSN}' \\"
echo "     --from-literal=WEBHOOK_SECRET='<your-provider-webhook-secret>' \\"
echo "     --from-literal=POSTGRES_PASSWORD='${PG_PASSWORD}'"
echo "======================================================"

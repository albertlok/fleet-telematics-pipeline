#!/usr/bin/env bash
# ==============================================================
# One-time Azure infrastructure setup. Creates:
#   1. A Resource Group
#   2. A shared VNet with subnets for AKS and PostgreSQL
#   3. Azure Container Registry (ACR)
#   4. An AKS cluster  (nodes in the AKS subnet)
#   5. Azure Database for PostgreSQL Flexible Server
#      — joined to the PG subnet via VNet integration
#      — DNS-resolvable from AKS pods via a private DNS zone
#      — no public internet endpoint at all
#
# Usage:
#   export SUBSCRIPTION_ID=<your-subscription-id>
#   ./scripts/setup-aks.sh
#
# AKS and PostgreSQL must be in the same region for shared-VNet
# integration. If LOCATION is restricted for PostgreSQL on your
# subscription, override it: LOCATION=eastus2 ./scripts/setup-aks.sh
# ==============================================================

# Safety flags: -e exit on first error, -u error on unset variables,
# -o pipefail catch failures inside pipelines.
set -euo pipefail

# ":?" aborts with a message if the variable isn't set.
: "${SUBSCRIPTION_ID:?Set SUBSCRIPTION_ID to your Azure subscription id}"

# "${VAR:-default}" = use $VAR if set, otherwise the default.
RESOURCE_GROUP="${RESOURCE_GROUP:-fleet-pipeline-rg}"
LOCATION="${LOCATION:-eastus2}"
AKS_CLUSTER="${AKS_CLUSTER:-fleet-pipeline-aks}"
ACR_NAME="${ACR_NAME:-fleetpipelineacr}"   # globally unique, lowercase alphanumeric
VNET_NAME="${VNET_NAME:-fleet-pipeline-vnet}"
AKS_SUBNET_NAME="${AKS_SUBNET_NAME:-aks-subnet}"
PG_SUBNET_NAME="${PG_SUBNET_NAME:-pg-subnet}"
PG_SERVER="${PG_SERVER:-fleet-telemetry-pg}"
# Private DNS zone name must match the server FQDN suffix that Azure assigns.
PG_DNS_ZONE="${PG_DNS_ZONE:-${PG_SERVER}.private.postgres.database.azure.com}"
PG_DB="${PG_DB:-telemetry}"
PG_USER="${PG_USER:-fleet}"
# Generate a random password unless one was provided.
PG_PASSWORD="${PG_PASSWORD:-$(openssl rand -base64 24)}"

echo "==> Setting active subscription"
az account set --subscription "$SUBSCRIPTION_ID"

echo "==> Registering required resource providers (idempotent, skipped if already registered)"
for ns in \
  Microsoft.Compute \
  Microsoft.ContainerRegistry \
  Microsoft.ContainerService \
  Microsoft.DBforPostgreSQL \
  Microsoft.ManagedIdentity \
  Microsoft.Network \
  Microsoft.OperationalInsights \
  Microsoft.OperationsManagement; do
  state=$(az provider show --namespace "$ns" --query registrationState -o tsv 2>/dev/null || echo "NotRegistered")
  if [[ "$state" != "Registered" ]]; then
    echo "   Registering $ns ..."
    az provider register --namespace "$ns" --wait
  else
    echo "   $ns already registered"
  fi
done

echo "==> Creating resource group: $RESOURCE_GROUP"
az group create --name "$RESOURCE_GROUP" --location "$LOCATION"

# -- VNet & subnets --------------------------------------------------
# AKS with Azure CNI allocates one IP per pod from the node subnet, so
# a /16 (65k addresses) gives plenty of headroom.
# The PostgreSQL subnet must be /28 or larger and delegated exclusively
# to the Flexible Server service — no other resources can use it.
if az network vnet show --resource-group "$RESOURCE_GROUP" --name "$VNET_NAME" &>/dev/null; then
  echo "==> VNet '$VNET_NAME' already exists — skipping"
else
  echo "==> Creating VNet: $VNET_NAME"
  az network vnet create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$VNET_NAME" \
    --location "$LOCATION" \
    --address-prefix 10.0.0.0/8

  az network vnet subnet create \
    --resource-group "$RESOURCE_GROUP" \
    --vnet-name "$VNET_NAME" \
    --name "$AKS_SUBNET_NAME" \
    --address-prefix 10.240.0.0/16

  az network vnet subnet create \
    --resource-group "$RESOURCE_GROUP" \
    --vnet-name "$VNET_NAME" \
    --name "$PG_SUBNET_NAME" \
    --address-prefix 10.241.0.0/24 \
    --delegations Microsoft.DBforPostgreSQL/flexibleServers
fi

AKS_SUBNET_ID=$(az network vnet subnet show \
  --resource-group "$RESOURCE_GROUP" \
  --vnet-name "$VNET_NAME" \
  --name "$AKS_SUBNET_NAME" \
  --query id -o tsv)

PG_SUBNET_ID=$(az network vnet subnet show \
  --resource-group "$RESOURCE_GROUP" \
  --vnet-name "$VNET_NAME" \
  --name "$PG_SUBNET_NAME" \
  --query id -o tsv)

# -- Private DNS zone ------------------------------------------------
# PostgreSQL Flexible Server with VNet integration requires a private
# DNS zone linked to the VNet so pods can resolve the server hostname.
if az network private-dns zone show \
    --resource-group "$RESOURCE_GROUP" --name "$PG_DNS_ZONE" &>/dev/null; then
  echo "==> Private DNS zone '$PG_DNS_ZONE' already exists — skipping"
else
  echo "==> Creating private DNS zone: $PG_DNS_ZONE"
  az network private-dns zone create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$PG_DNS_ZONE"

  az network private-dns link vnet create \
    --resource-group "$RESOURCE_GROUP" \
    --zone-name "$PG_DNS_ZONE" \
    --name "${VNET_NAME}-dns-link" \
    --virtual-network "$VNET_NAME" \
    --registration-enabled false
fi

# -- ACR -------------------------------------------------------------
if az acr show --resource-group "$RESOURCE_GROUP" --name "$ACR_NAME" &>/dev/null; then
  echo "==> ACR '$ACR_NAME' already exists — skipping"
else
  echo "==> Creating Azure Container Registry: $ACR_NAME"
  az acr create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$ACR_NAME" \
    --sku Standard \
    --admin-enabled false   # use Azure AD identities, not admin passwords
fi

# -- AKS cluster -----------------------------------------------------
if az aks show --resource-group "$RESOURCE_GROUP" --name "$AKS_CLUSTER" &>/dev/null; then
  echo "==> AKS cluster '$AKS_CLUSTER' already exists — skipping"
else
  echo "==> Creating AKS cluster: $AKS_CLUSTER (this takes ~10 minutes)"
  az aks create \
    --resource-group "$RESOURCE_GROUP" \
    --name "$AKS_CLUSTER" \
    --node-count 3 \
    --node-vm-size Standard_D2s_v3 \
    --enable-managed-identity \
    --attach-acr "$ACR_NAME" \
    --network-plugin azure \
    --vnet-subnet-id "$AKS_SUBNET_ID" \
    --enable-cluster-autoscaler \
    --min-count 3 \
    --max-count 5 \
    --generate-ssh-keys
  # --vnet-subnet-id: place nodes and pods in our AKS subnet so they
  # share the VNet with PostgreSQL and can reach it via private DNS.
  # --attach-acr: pull images from ACR without extra credentials.
fi

echo "==> Downloading cluster credentials for kubectl"
az aks get-credentials \
  --resource-group "$RESOURCE_GROUP" \
  --name "$AKS_CLUSTER" \
  --overwrite-existing

echo "==> Creating Kubernetes namespace"
kubectl apply -f k8s/namespace.yaml

# -- PostgreSQL Flexible Server --------------------------------------
if az postgres flexible-server show \
    --resource-group "$RESOURCE_GROUP" --name "$PG_SERVER" &>/dev/null; then
  echo "==> PostgreSQL server '$PG_SERVER' already exists — skipping"
else
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
    --subnet "$PG_SUBNET_ID" \
    --private-dns-zone "$PG_DNS_ZONE"
  # --subnet + --private-dns-zone: VNet integration — the server gets a
  # private IP in pg-subnet and is resolvable only from inside the VNet.
  # No public endpoint is created; firewall rules are irrelevant.
fi

if az postgres flexible-server db show \
    --resource-group "$RESOURCE_GROUP" \
    --server-name "$PG_SERVER" \
    --database-name "$PG_DB" &>/dev/null; then
  echo "==> Database '$PG_DB' already exists — skipping"
else
  az postgres flexible-server db create \
    --resource-group "$RESOURCE_GROUP" \
    --server-name "$PG_SERVER" \
    --name "$PG_DB"
fi

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
echo ""
echo " To apply the database schema (runs psql inside the cluster VNet):"
echo ""
echo "   kubectl run psql-migration --image=postgres:16-alpine \\"
echo "     -n telematics-pipeline --restart=Never --rm -i \\"
echo "     -- psql '${DB_DSN}' < sql/001_schema.sql"
echo "======================================================"

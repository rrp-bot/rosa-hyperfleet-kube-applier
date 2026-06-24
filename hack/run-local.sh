#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER_NAME:-kube-applier-dev}"
MC_NAME="${MANAGEMENT_CLUSTER:-dev-local}"
AWS_REGION="${AWS_REGION:-us-east-1}"
LOCALSTACK_ENDPOINT="${LOCALSTACK_ENDPOINT:-http://localhost:4566}"
SPECS_TABLE="${SPECS_TABLE_PREFIX:-mc-${MC_NAME}-specs}"
STATUS_TABLE="${STATUS_TABLE_PREFIX:-mc-${MC_NAME}-status}"
VERBOSITY="${LOG_VERBOSITY:-4}"

echo "Building kube-applier-aws..."
make build

echo ""
echo "Running kube-applier-aws against Kind cluster '${CLUSTER_NAME}' + LocalStack at ${LOCALSTACK_ENDPOINT}"
echo "  Management cluster:    ${MC_NAME}"
echo "  AWS region:            ${AWS_REGION}"
echo "  Specs table prefix:    ${SPECS_TABLE}"
echo "  Status table prefix:   ${STATUS_TABLE}"
echo ""

exec ./kube-applier-aws \
  --kubeconfig="${KUBECONFIG:-$HOME/.kube/config}" \
  --namespace=kube-applier-system \
  --management-cluster="${MC_NAME}" \
  --aws-region="${AWS_REGION}" \
  --aws-endpoint-url="${LOCALSTACK_ENDPOINT}" \
  --specs-table="${SPECS_TABLE}" \
  --status-table="${STATUS_TABLE}" \
  --log-verbosity="${VERBOSITY}"

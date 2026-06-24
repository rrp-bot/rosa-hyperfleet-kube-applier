#!/usr/bin/env bash
# start-localstack.sh — starts a LocalStack container with DynamoDB and
# DynamoDB Streams enabled in detached mode.
# Usage: ./hack/start-localstack.sh
# The container runs detached. Use 'docker stop localstack-kube-applier-aws'
# to stop it.
#
# DynamoDB is available in the free community image (localstack/localstack).
# If LOCALSTACK_AUTH_TOKEN is set the Pro image is used instead, which adds
# extra services — not required for these tests.

set -euo pipefail

CONTAINER_NAME="localstack-kube-applier-aws"
PORT="${LOCALSTACK_PORT:-4566}"

if [[ -n "${LOCALSTACK_AUTH_TOKEN:-}" ]]; then
  IMAGE="localstack/localstack-pro"
  AUTH_ARGS=(-e "LOCALSTACK_AUTH_TOKEN=${LOCALSTACK_AUTH_TOKEN}")
else
  IMAGE="localstack/localstack"
  AUTH_ARGS=()
fi

# Remove any stale container with the same name.
docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true

echo "Starting ${IMAGE} on port ${PORT} (detached) ..."
docker run -d \
  --name "${CONTAINER_NAME}" \
  -p "${PORT}:4566" \
  -e "SERVICES=dynamodb,dynamodbstreams" \
  -e "DEBUG=0" \
  "${AUTH_ARGS[@]}" \
  "${IMAGE}"

echo "LocalStack container '${CONTAINER_NAME}' started."
echo "Set LOCALSTACK_ENDPOINT=http://localhost:${PORT} before running integration tests."
echo "Stop with: docker stop ${CONTAINER_NAME}"

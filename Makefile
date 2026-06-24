BINARY ?= kube-applier-aws
IMAGE  ?= kube-applier-aws
TAG    ?= latest

.PHONY: build desire-tool desirectl test integration-test vet image clean localstack kind-setup run-local

build:
	go build -o $(BINARY) .

desire-tool:
	go build -o desire-tool ./cmd/desire-tool

desirectl:
	go build -o desirectl ./cmd/desirectl

test:
	go test ./... -count=1

# integration-test runs controller-level tests that need both LocalStack and a
# Kind cluster. Set LOCALSTACK_ENDPOINT and KUBECONFIG before running.
#   make localstack
#   make kind-setup
#   LOCALSTACK_ENDPOINT=http://localhost:4566 KUBECONFIG=~/.kube/config make integration-test
integration-test:
	go test ./test/integration/... -v -count=1 -timeout 120s

vet:
	go vet ./...

image:
	docker build -t $(IMAGE):$(TAG) .

localstack:
	./hack/start-localstack.sh

kind-setup:
	./hack/setup-kind.sh

run-local:
	./hack/run-local.sh

clean:
	rm -f $(BINARY) desire-tool desirectl

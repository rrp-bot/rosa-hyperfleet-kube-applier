FROM golang:1.26 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /kube-applier-aws .

FROM registry.access.redhat.com/ubi9/ubi-micro:latest
COPY --from=builder /kube-applier-aws /kube-applier-aws
USER 65532:65532
ENTRYPOINT ["/kube-applier-aws"]

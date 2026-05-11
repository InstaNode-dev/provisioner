# Multi-target Dockerfile for the provisioner.
#
# Two named final stages — pick which one to build with `--target`:
#
#   prod  (default — used by `docker build … -t instant-provisioner:local .`)
#         Distroless. No shell. Minimum attack surface. Whatever ops
#         pushes to the registry as `instant-provisioner:latest` runs
#         from this stage.
#
#   debug (opt-in — `docker build … --target debug -t instant-provisioner:debug-local .`)
#         Same binary, but on top of alpine with /bin/sh, curl, jq,
#         psql, redis-cli, and bind-tools (dig / nslookup). Use this
#         image only for ad-hoc debugging — `kubectl exec` into a
#         provisioner pod running this tag to verify backing-service
#         connectivity or DNS resolution from inside the cluster.
#
# Build context is the repo root (CLAUDE.md convention); the COPY
# paths assume `cd cron && docker build -f provisioner/Dockerfile .`.

FROM golang:1.24-alpine AS builder
WORKDIR /app
# Copy replace-directive modules first (go.mod uses replace ../proto and ../common)
COPY proto/ /proto/
COPY common/ /common/
# Copy provisioner source
COPY provisioner/go.mod provisioner/go.sum ./
RUN go mod download
COPY provisioner/ .
RUN CGO_ENABLED=0 go build -o /provisioner .

# debug — alpine-based image that supports `kubectl exec` for one-off
# debugging. Bundles the most-likely-needed tools so an operator (or
# an agent) can verify reachability without having to install anything
# inside the running pod:
#   curl + jq    — talk to APIs and parse JSON
#   psql         — verify Postgres connectivity
#   redis-cli    — verify Redis connectivity
#   bind-tools   — dig / nslookup for cluster DNS resolution
#   netcat       — raw TCP port checks
#   bash         — nicer than ash for ad-hoc scripts
# Total image size is ~80 MB (vs ~10 MB for distroless prod) — fine
# for a debug-only artifact that never serves customer traffic.
FROM alpine:3.20 AS debug
RUN apk add --no-cache \
        ca-certificates tzdata \
        bash curl jq \
        bind-tools netcat-openbsd \
        postgresql-client redis
COPY --from=builder /provisioner /provisioner
ENTRYPOINT ["/provisioner"]

# prod — distroless static, the default target. Keep this LAST so
# existing build commands (no `--target` flag in CLAUDE.md or in
# infra/k8s scripts) keep producing the prod image unchanged.
FROM gcr.io/distroless/static-debian12 AS prod
COPY --from=builder /provisioner /provisioner
ENTRYPOINT ["/provisioner"]

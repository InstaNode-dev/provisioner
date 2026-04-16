# instant-provisioner

A gRPC service (port 50051) that performs the actual work of creating and destroying real databases, caches, and queues. When `api/` receives `POST /db/new`, it talks to this service. This is where `CREATE DATABASE`, `ACL SETUSER`, and `CREATE USER` actually execute.

---

## Why Is This a Separate Service?

The separation exists for a deliberate security reason: **blast radius**.

`api/` is agent-facing — it handles anonymous traffic, issues JWTs, and talks to the public internet. If it were ever compromised, the attacker should not get admin credentials to the databases where all customer data lives.

`provisioner/` holds admin credentials (`CUSTOMER_POSTGRES_DSN`, `CUSTOMER_REDIS_URL`, `CUSTOMER_MONGO_URL`) and nothing else — no customer-facing secrets, no Razorpay keys, no JWT signing material. Conversely, `api/` never sees the admin DSNs. The boundary between them is gRPC, enforced at the network level: provisioner runs on a ClusterIP service that is not reachable from outside the cluster.

---

## Backends: Three Modes Per Service

Each resource type (postgres, redis, mongodb, queue) has up to three backend implementations:

| Backend | File | When used |
|---|---|---|
| `local.go` | Shared infra in k8s | Default for all tiers up to pro |
| `k8s.go` | Dedicated pod per token | Team / growth tier (isolated pods per token) |
| `neon.go` | Cloud-hosted Postgres (Neon) | When `NEON_API_KEY` is set |

**Local backend** (`local.go`) is what runs today. For Postgres it calls `CREATE DATABASE db_{token}` and `CREATE USER usr_{token}` on the shared `postgres-customers` pod. The token maps 1:1 to a database and user. For Redis it sets an ACL user scoped to a key namespace. For MongoDB it creates a scoped user on a per-token database.

**Multi-cluster support**: when `POSTGRES_CLUSTER_URLS` is set to a comma-separated list of admin DSNs, the `ClusterRouter` distributes provisions across them by available capacity. The cluster index is stored in `providerResourceID` (e.g. `"local:0"`, `"local:1"`) so that `StorageBytes` and `Deprovision` can reconnect to the right cluster.

---

## Hot Pool

`provisioner/internal/pool/` pre-creates resources so that provisioning feels instant. When a new token arrives, it pulls a pre-created database from the pool and returns it immediately, then refills in the background.

**Important caveat**: the pool lives in memory. If the provisioner pod restarts, the pool state is lost — resources may exist on the shared infra but the pod doesn't know about them. Those orphans are cleaned up by the expiry worker. A fresh pod simply starts filling the pool from scratch. The first few provisions after a restart may be slightly slower.

---

## How to Develop

```bash
cd provisioner

# Build
go build ./...

# Run unit tests (no k8s or real databases required)
# server_test.go uses mock backends — safe to run anywhere
go test ./...

# Run against a real local database
CUSTOMER_POSTGRES_DSN="postgres://instant_cust:instant_cust@localhost:5435/instant_customers?sslmode=disable" \
go test ./... -run TestProvision -v
```

The test suite uses mock backends for almost all tests, so `go test ./...` passes without any infrastructure. Only integration tests that explicitly require a DSN need the real services.

---

## Environment Variables

| Variable | Purpose | Default |
|---|---|---|
| `CUSTOMER_POSTGRES_DSN` | Admin DSN for postgres-customers | `postgres://instant_cust:instant_cust@postgres-customers:5432/instant_customers` |
| `CUSTOMER_REDIS_URL` | Admin URL for redis-provision | `redis://redis-provision.instant-data.svc.cluster.local:6379` |
| `CUSTOMER_MONGO_URL` | Admin URL for mongodb | `mongodb://root:root@mongodb.instant-data.svc.cluster.local:27017` |
| `POSTGRES_CLUSTER_URLS` | Comma-separated list of admin DSNs (multi-cluster) | unset |
| `K8S_DEDICATED_BACKEND` | Enable k8s dedicated-pod backend for team / growth tier | `false` |
| `K8S_EXTERNAL_HOST` | External hostname for dedicated k8s services | unset |
| `K8S_STORAGE_CLASS` | Storage class for dedicated PVCs | `local-path` |

---

## Deployment

```bash
# Build Docker image (must be run from repo root — Dockerfile copies provisioner/)
docker build -f provisioner/Dockerfile -t instant-provisioner:local .

# Deploy to k8s
kubectl apply -f infra/k8s/provisioner/deployment.yaml
kubectl apply -f infra/k8s/provisioner/service.yaml

# Check pod status
kubectl get pods -n instant-infra
kubectl logs -n instant-infra deployment/instant-provisioner
```

Namespace: `instant-infra`. Port 50051 is ClusterIP only — not reachable outside the cluster. `api/` reaches it at `instant-provisioner.instant-infra.svc.cluster.local:50051`.

---

## Common Failures

**CrashLoopBackOff on startup**: almost always `CUSTOMER_POSTGRES_DSN` is wrong, or the `postgres-customers` pod in `instant-data` is not yet Ready. Check:
```bash
kubectl get pods -n instant-data
kubectl logs -n instant-infra deployment/instant-provisioner
```

**Provision returns error "connection refused"**: provisioner is up but can't reach the backend. Verify network policies allow `instant-infra` to reach `instant-data`.

**Pool appears empty after restart**: expected — the in-memory pool refills in the background. Provisions still work; they just take a few hundred milliseconds longer until the pool is warm.

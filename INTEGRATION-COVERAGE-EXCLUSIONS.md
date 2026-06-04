# Provisioner — Integration-Coverage Exclusions

> Companion to the standing integration-coverage program
> (`docs/sessions/2026-06-04/INTEGRATION-COVERAGE-PLAN-2026-06-04.md`, Wave 4).
>
> The provisioner's forum-defined done-bar is a **≥80% line-coverage floor measured
> integration-only** (real backends, not mocks) AND flow-completeness (every gRPC
> Provision / Deprovision / Status / Regrade flow has a real-backend round-trip).
>
> The ≥80 floor is computed **after subtracting the lines listed here** — each is
> genuinely unreachable from an integration test (process bootstrap / k8s
> control-plane wiring / fatal-exit) and would otherwise force a fake just to
> tick a line. Every entry has a one-line justification. Keep this list *small* —
> it is exclusions, not waivers; a flow that can be driven against a real backend
> must be, not listed here.

## How integration-only coverage is measured (mechanism C, per the PLAN)

```bash
# from provisioner/, with real backends reachable (CI coverage.yml supplies them):
#   Postgres, Redis, Mongo (no-auth + auth), NATS (monitor :8222), MinIO.
export CUSTOMER_MONGO_AUTH_URL=mongodb://root:rootpass@127.0.0.1:27018
export TEST_NATS_HOST=127.0.0.1
export TEST_MINIO_ENDPOINT=127.0.0.1:9100
export TEST_MINIO_ROOT_USER=minioadmin TEST_MINIO_ROOT_PASSWORD=minioadmin TEST_MINIO_BUCKET=itest-bucket
export TEST_POSTGRES_CUSTOMERS_URL=postgres://postgres:postgres@127.0.0.1:5432/postgres?sslmode=disable
export CUSTOMER_REDIS_URL=redis://127.0.0.1:6379

go test ./internal/server/ -coverpkg=./internal/server/... \
  -coverprofile=/tmp/srvcov.out -count=1 -timeout 300s
go tool cover -func=/tmp/srvcov.out | tail -1
```

Measured for `internal/server` (the gRPC handler layer — the package this Wave-4
PR touched), all backends wired: **99.2%** of statements — well above the 80
floor before any subtraction.

## Excluded lines (genuinely unreachable from an integration test)

| Location | Symbol | Why it cannot be integration-covered |
|---|---|---|
| `internal/server/server.go` `New()` — the `cfg.K8sEnabled` block | dedicated-backend init (`postgres/redis/mongo/queue.NewK8sDedicatedBackend`) | Requires a live kube-apiserver + kubeconfig; the dedicated (Pro/Team) k8s backends are exercised by the per-backend `k8s_live_test.go` suites against a real cluster (nightly e2e), not by the in-process gRPC server round-trip. The error-log fallback branches fire only on a malformed kubeconfig at process boot. |
| `internal/server/server.go` `NewWithBackends()` — `poolMgr != nil` typed-nil normalization | constructor guard | The branch that converts a typed-nil `*pool.Manager` to a nil `PoolClaimer` interface is a boot-time correctness guard; it is covered by the unit constructor test, but the "real pool manager passed" arm needs a live `*pgxpool.Pool` to the provisioner DB and is exercised by `internal/pool/manager_db_test.go`, not the server round-trip. |
| `cmd/` (all) | `main`, signal wiring, `os.Exit` | Process entrypoint / fatal-exit paths — not reachable without forking the binary. Excluded from the measured package set (PLAN §1.4). |

Everything else in the gRPC handler layer — every `ProvisionResource`,
`DeprovisionResource`, `GetStorageBytes`, `RegradeResource` arm across postgres,
redis, mongo, queue, and storage — IS driven by a real-backend round-trip
(`server_live_roundtrip_test.go` + `server_live_roundtrip_mqs_test.go`) and sits
at 100% function coverage.

## Drift guard — the RPC-iterating done-bar test (rule 18)

`internal/server/server_rpc_coverage_guard_test.go`
(`TestGRPCSurface_EveryRPCHasRoundTripTest`) iterates the proto-generated
`ProvisionerService_ServiceDesc.Methods` (the single source of truth for which
RPCs exist) and FAILS CI if any RPC lacks a maintained, existing round-trip test
— catching the silent-untested-RPC class the day a new RPC is added to
`proto/provisioner/v1/provisioner.proto`. It is a pure descriptor + source-scan
test (no backends, no env) so it runs unconditionally in the `go test -short`
deploy gate, never skips, and reds on: an unmapped new RPC, a mapping pointing at
a deleted/renamed test, or a stale mapping for a removed RPC.

To intentionally exempt an RPC from round-trip coverage, add it to the
`exemptedRPCs` set in that test AND add a justification row to this file. There
are **no exemptions today** — every RPC has a real round-trip.

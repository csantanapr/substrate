# Agent Substrate — Architectural Knowledge

This file captures deep architectural knowledge of the Agent Substrate codebase for use by AI assistants in future sessions.

## Component Map

```
ateapi          cmd/ateapi/                  gRPC control plane (actor lifecycle, worker assignment)
atecontroller   cmd/atecontroller/           K8s controller (WorkerPool→Deployment, ActorTemplate golden snapshot)
atelet          cmd/atelet/                  Node-level DaemonSet agent (object storage I/O, OCI image pulling)
ateom-gvisor    cmd/ateom-gvisor/            Per-pod gVisor sandbox manager (runsc, netns, process reaping)
atenet          cmd/atenet/                  Networking: Envoy control plane + ext_proc routing
kubectl-ate     cmd/kubectl-ate/             CLI plugin for actor management
```

## Key Data Stores

- **Redis (Valkey)**: Source of truth for Actor records and Worker assignment. Accessed only by ateapi.
- **GCS / S3 (rustfs locally)**: Durable snapshot storage. Three files per snapshot: `checkpoint.img.zstd`, `pages.img.zstd`, `pages_meta.img.zstd`.
- **K8s informers** (in ateapi): Worker pod state synced into Redis via `WorkerPoolSyncer` watching pods with label `ate.dev/worker-pool`.

## Actor Lifecycle

```
CreateActor  → Redis record STATUS_SUSPENDED, no compute allocated
ResumeActor  → AssignWorkerStep (pick free worker in Redis)
               → CallAteletRestoreStep (dial atelet on worker's node)
               → atelet: download snapshot from GCS + pull OCI images + build OCI bundles
               → ateom: runsc restore (all containers from single pause checkpoint)
               → FinalizeRunningStep → STATUS_RUNNING
SuspendActor → MarkSuspendingStep (generate snapshot URI, STATUS_SUSPENDING)
               → CallAteletSuspendStep → atelet.Checkpoint
               → ateom: runsc checkpoint pause (only root container)
               → atelet: upload checkpoint.img/pages.img/pages_meta.img to GCS
               → FinalizeSuspendedStep: free worker, promote InProgressSnapshot→LastSnapshot, STATUS_SUSPENDED
```

## gVisor Snapshot Mechanics

**Checkpoint** (`cmd/ateom-gvisor/main.go:CheckpointWorkload`, `cmd/ateom-gvisor/runsc.go:cmdCheckpoint`):
- `runsc checkpoint -image-path <dir> pause` — checkpoints ONLY the pause/sandbox root container
- Application containers are then `runsc delete`d — they don't need separate checkpoints
- gVisor captures full Sentry state: process memory, CPU state, file descriptors, GoVFS state (including all file writes made during actor lifetime)
- `eth0` is moved back from interior netns to pod netns after checkpoint

**Restore** (`cmd/ateom-gvisor/main.go:RestoreWorkload`, `cmd/ateom-gvisor/runsc.go:cmdRestore`):
- For EACH container (pause first, then app containers): `runsc create` then `runsc restore -image-path <dir> <name>`
- Same single checkpoint image used for all containers — gVisor design
- `eth0` moved into interior netns, saved IP/routes re-applied before runsc restore

**What's in checkpoint vs. what's re-pulled:**
- `checkpoint.img` = Sentry state including VFS write-deltas (file modifications during actor lifetime)
- OCI rootfs = always re-pulled from container registry by atelet on every restore via `memorypullcache`
- Both are required: rootfs provides base read layer, checkpoint provides Sentry state + write deltas on top

**Memory pull cache** (`internal/memorypullcache/memorypullcache.go`):
- In-memory LRU of 256 entries keyed by image digest
- Only caches if image ≤ 100MB; larger images streamed directly from registry
- Cache is per-atelet-instance (not shared across nodes)
- Cache miss on different node = full registry pull required

## Pod/Worker Architecture

**Worker pod structure** (`internal/controllers/utils.go:createActorDeploymentSpec`):
- Single container: `ateom-gvisor`, privileged, root, mounts `/run/ateom-gvisor` as hostPath
- WorkerPool → Deployment (N replicas). No resource requests — scheduler blind to actual workload resources.
- One actor per pod at a time. `Worker.actor_id` is the exclusive binding in Redis.

**Shared hostPath** `/run/ateom-gvisor`:
- atelet (DaemonSet) and all ateom pods on the same node share this volume
- atelet writes OCI bundles, checkpoint files, runsc binary here
- ateom reads them and writes its UDS socket here

**Actor directory layout** on host:
```
/run/ateom-gvisor/actors/<ns>:<template>:<actorID>/
├── bundles/          OCI bundle dirs (rootfs + config.json) — wiped and rebuilt on each restore
├── runsc-state/      runsc bookkeeping — wiped on each restore
├── checkpoint/       staging area for checkpoint files — wiped then populated
├── pidfiles/         runsc PID files — wiped on each restore
└── runsc-debug-logs/ intentionally left untouched by resetActorDirs
```

## atelet ↔ ateom Communication

atelet dials ateom via Unix Domain Socket:
```
"unix://" + /run/ateom-gvisor/ateoms/<namespace>:<podname>/ateom.sock
```
Connection cached in LRU of 256 (`cmd/atelet/main.go:AteomDialer`).

## ateapi → atelet Routing (AteletDialer)

`cmd/ateapi/internal/controlapi/dialer.go`:
1. Look up worker pod by namespace/name in K8s informer cache → get `pod.Spec.NodeName`
2. Look up atelet DaemonSet pod on that node via `byNode` index
3. Dial atelet at `atelet_pod_ip:8085` (hostPort)

## Worker Registration

`cmd/ateapi/internal/controlapi/syncer.go` (WorkerPoolSyncer):
- Watches all pods with label `ate.dev/worker-pool` via K8s informer
- Creates Worker record in Redis when pod gets a PodIP (`isWorkerEligible`)
- Deletes Worker record when pod is deleted or has DeletionTimestamp

## Resume on Different Node

`AssignWorkerStep` (`cmd/ateapi/internal/controlapi/workflow_resume.go`):
- Searches Redis for worker already bound to this actor (idempotency guard for failed resume retries)
- If not found: calls `findFreeWorker` → random shuffle of workers where `actor_id == ""`
- Updates `actor.AteomPodName/Namespace/IP` to new worker's pod
- Resume proceeds on new node; snapshot downloaded fresh from GCS

## ActorTemplate Golden Snapshot

`internal/controllers/actortemplate_controller.go` — state machine:
1. `PhaseInitial` → create golden actor (STATUS_SUSPENDED in Redis)
2. `PhaseResumeGoldenActor` → cold boot (no snapshot yet), actor runs fresh from OCI image
3. `PhaseWaitGoldenActor` → wait 20 seconds, then SuspendActor → snapshot saved to GCS
4. `PhaseReady` → `ActorTemplate.Status.GoldenSnapshot` URI set

New actors without a personal snapshot restore from this golden snapshot instead of cold-booting.

## Networking / IP Addresses

**Pod IP ≠ stable actor IP.** Each worker pod has its own Kubernetes-assigned IP.

**Interior netns** (`cmd/ateom-gvisor/main.go`):
- ateom creates named netns `ateom:<ns>:<name>` at startup
- Scrapes pod's eth0 IP/routes into `eth0LinkInfo` at startup (held in memory only)
- On actor run/restore: eth0 physically moved into interior netns, saved IP/routes re-applied
- gVisor reads network config from the interior netns → actor sees pod's IP
- On suspend: eth0 moved back to pod netns

**Cross-pod resume → actor gets new IP.** gVisor checkpoint contains TCP state anchored to old IP. Open TCP connections are lost on cross-pod resume. Actor code must handle reconnection.

**Inbound traffic routing** (atenet ext_proc):
- Every request: atenet reads `actor.AteomPodIp` from Redis (updated during AssignWorkerStep)
- Rewrites `:authority` header to `<current-worker-pod-ip>:80`
- Envoy dynamic_forward_proxy dials worker pod IP directly
- Transparent to HTTP clients regardless of which pod the actor migrated to

**No Envoy per worker pod.** Single `atenet-router` Deployment with two co-located containers:
- `envoy` container (port 8080 HTTP, 8443 HTTPS)
- `atenet-router` container (port 18000 xDS, port 50051 ext_proc)
- Envoy talks to atenet over 127.0.0.1 within the same pod
- `atenet-router` Service exposes port 80 → 8080, 443 → 8443 as ClusterIP

## HTTP Routing — Actor Addressing

Actor traffic uses Host header for routing:
```
Host: <actor-id>.actors.resources.substrate.ate.dev
```

Envoy ext_proc intercepts every request, calls `handleRequestHeaders`:
1. Parse actor ID from Host header
2. `ResumeActor(actorID)` — blocks until running (triggers full restore if suspended)
3. Get `actor.AteomPodIp` → rewrite `:authority` to `<ip>:80`
4. Envoy routes to worker pod directly

`singleflight.Group` deduplicates concurrent resume calls for the same actor.

## OCI Bundle Assembly

`cmd/atelet/oci.go:prepareOCIDirectory`:
- Fetches image via `memorypullcache.Fetch` → `remote.Image()` from go-containerregistry
- Flattens all image layers into single tar via `mutate.Extract(img)`
- Extracts tar to `rootfs/` directory
- Generates `config.json` (OCI runtime spec) with hardcoded mounts: `/proc`, `/dev`, `/sys`, `/etc/resolv.conf`
- No user-configurable volume mounts currently (roadmap item)

## Known Gaps / Roadmap Items (docs/roadmap.md)

- **No resource requests on worker pods** — scheduler blind to actor workload CPU/memory needs
- **No persistent volume support** — `/workspace/` writes captured in gVisor checkpoint only (snapshot grows with data)
- **No ConfigMap/PVC volume mounts** — OCI spec has only hardcoded system mounts
- **memorypullcache not disk-backed or cross-node** — images re-pulled on different node
- **TCP connections lost on cross-pod resume** — no connection migration
- **Working volume separate lifecycle** — roadmap: split rootfs+memory snapshot from working disk state
- **No resource quotas for actor workloads** — K8s quota only sees ateom container

## Local Kind Development (No GCP Required)

GCP dependency substitutions:
- GCS → **rustfs** (S3-compatible, in-cluster, `manifests/ate-install/kind/rustfs.yaml`)
- GCR/Artifact Registry → **local kind-registry** Docker container on port 5001
- GCP auth → disabled (`--gcp-auth-for-image-pulls=false`)
- Memorystore Redis → **Valkey** StatefulSet in ate-system
- GKE → **kind** single-node cluster

atelet kind patch (`manifests/ate-install/kind/atelet/kustomization.yaml`):
```
ATE_STORAGE_BACKEND=s3
AWS_ENDPOINT_URL=http://rustfs.ate-system.svc:9000
AWS_ACCESS_KEY_ID=rustfsadmin
--gcp-auth-for-image-pulls=false
--localhost-registry-replacement=kind-registry:5000
```

Setup:
```bash
hack/create-kind-cluster.sh          # kind cluster + local registry + Proxy ARP
hack/install-ate-kind.sh --deploy-ate-system
hack/install-ate-kind.sh --deploy-demo-counter
go install ./cmd/kubectl-ate
kubectl port-forward -n ate-system svc/atenet-router 8000:80
curl -X POST -H "Host: my-counter-1.actors.resources.substrate.ate.dev" http://localhost:8000/
```

`kind` and `ko` are auto-managed via `hack/run-tool.sh` using `go tool` — no manual installs needed.

## Demo Usage Patterns

### Pattern A — Traffic-triggered resume (counter demo)
Actor auto-resumes on first HTTP request. Client only needs Host header:
```bash
curl -H "Host: <actor-id>.actors.resources.substrate.ate.dev" http://<atenet-router>/
```

### Pattern B — Explicit lifecycle + HTTP data plane (sandbox client demo)
```go
// Control plane: gRPC to ateapi
cli.ResumeActor(ctx, &ateapipb.ResumeActorRequest{ActorId: actorID})
defer cli.SuspendActor(ctx, &ateapipb.SuspendActorRequest{ActorId: actorID})

// Data plane: HTTP with Host header
req.Host = actorID + ".actors.resources.substrate.ate.dev"
http.DefaultClient.Do(req)
```

### Pattern C — Self-suspension / zero-idle (agent-secret demo)
Actor calls ateapi on itself after handling a request:
```go
client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{ActorId: myID})
```
Actor ID extracted from Host header: `strings.Split(r.Host, ".")[0]`

## Key File Locations

| Topic | Files |
|---|---|
| gVisor checkpoint/restore | `cmd/ateom-gvisor/main.go`, `cmd/ateom-gvisor/runsc.go` |
| Snapshot upload/download | `cmd/atelet/main.go` (Checkpoint/Restore funcs) |
| OCI bundle assembly | `cmd/atelet/oci.go` |
| Image pull cache | `internal/memorypullcache/memorypullcache.go` |
| Resume workflow | `cmd/ateapi/internal/controlapi/workflow_resume.go` |
| Suspend workflow | `cmd/ateapi/internal/controlapi/workflow_suspend.go` |
| Worker registration | `cmd/ateapi/internal/controlapi/syncer.go` |
| atelet→atenet routing | `cmd/ateapi/internal/controlapi/dialer.go` |
| HTTP routing / ext_proc | `cmd/atenet/internal/app/router/extproc.go` |
| Envoy xDS config | `cmd/atenet/internal/app/router/xds.go` |
| Actor resume dedup | `cmd/atenet/internal/app/router/resumer.go` |
| WorkerPool controller | `internal/controllers/workerpool_controller.go` |
| Deployment spec builder | `internal/controllers/utils.go` |
| ActorTemplate controller | `internal/controllers/actortemplate_controller.go` |
| Filesystem paths | `internal/ateompath/ateompath.go` |
| Kind manifests | `manifests/ate-install/kind/` |
| Counter demo | `demos/counter/` |
| Sandbox demo + client | `demos/sandbox/` |
| Agent-secret demo | `demos/agent-secret/` |

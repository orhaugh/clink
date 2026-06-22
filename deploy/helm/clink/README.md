# clink Helm chart

Deploys a clink streaming cluster on Kubernetes: one JobManager (control plane +
dashboard) and a StatefulSet of TaskManagers. It maps the `docker-compose.yml`
topology onto k8s primitives.

## Status

Statically validated (`helm lint`, `helm template`, `kubeconform -strict` vs the
k8s 1.29 API schemas - 5/5 valid) **and live-verified end-to-end on a `kind`
cluster** via `kind-smoke.sh`: the JobManager and TaskManagers come up Ready,
every TM registers with the JM (its own pod IP + stable ordinal id), a cold
start is clean (0 restarts - the `wait-for-jobmanager` initContainer gates the
JM race), AND a job submitted to the cluster runs across the TaskManagers and
produces its sink output.

```sh
deploy/helm/clink/kind-smoke.sh            # create kind cluster, deploy, submit a job, assert
deploy/helm/clink/kind-smoke.sh --cleanup  # tear the cluster down
```

The smoke submits the baked-in `k8s_smoke_job.so` (a bounded
`from_elements -> map -> filter -> FileSink` pipeline) over HTTP and confirms
the `30,40,50` sink output on a TaskManager - end-to-end proof that a submitted
job executes on the deployed cluster. Multi-JobManager HA is also supported and
kind-verified (see HA below).

## Topology

| clink role | k8s object | why |
|---|---|---|
| JobManager | Deployment (1 replica, `Recreate`) + Service | single leader; the Service DNS name is what the JM advertises (`--advertise-host`) and TMs dial |
| TaskManager | StatefulSet + headless Service | stable ordinal pod names give each TM a stable `--id` across restarts |

Data plane: each TaskManager advertises its **own pod IP** (`--data-host=$(POD_IP)`
via the downward API), reachable cluster-wide on the flat pod network, so no
data port is published on a Service. The headless Service exists to back the
StatefulSet and provide per-pod DNS. `CLINK_DATA_BIND_HOST=0.0.0.0` binds the
data-plane listener on all interfaces (the default `127.0.0.1` is single-host).

Readiness and liveness probes hit `/api/v1/health` on each role's HTTP port
(the JobManager and TaskManager both serve it).

## Prerequisites

- A `clink-runtime` image (built from `docker/Dockerfile.runtime`) pushed to a
  registry your cluster can pull. Override `image.repository` / `image.tag`.
- Helm 3 / Kubernetes 1.25+.

## Install

```sh
helm install my-clink deploy/helm/clink \
  --set image.repository=<registry>/clink-runtime \
  --set image.tag=<version> \
  --set taskmanager.replicas=4 \
  --set taskmanager.slots=4
```

Open the dashboard:

```sh
kubectl port-forward svc/my-clink-jobmanager 8081:8081
# open http://localhost:8081
```

## Key values

| key | default | meaning |
|---|---|---|
| `image.repository` / `image.tag` | `clink-runtime` / `latest` | the runtime image |
| `taskmanager.replicas` | `3` | number of TaskManagers |
| `taskmanager.slots` | `4` | task slots per TaskManager |
| `jobmanager.service.type` | `ClusterIP` | set NodePort/LoadBalancer to expose the dashboard |
| `jobmanager.stateBackend` | `""` | `--state-backend` URI (e.g. a `s3+rocksdb://...` disaggregated backend) |
| `ha.enabled` | `false` | see below |

## HA (multi-JobManager)

`ha.enabled=true` runs `ha.replicas` JobManagers with leader election via
clink's **file coordinator** (always compiled in, no extra dependency): every
JM races an `fcntl` lock on a **shared** `--ha-dir`; the winner binds the
control port and writes `active-leader.json`, standbys wait, and TaskManagers
discover the leader from that shared dir. On leader death a standby acquires the
lock and takes over.

Shared storage is required (`ha.storage`):
- `type: pvc` (production) - a **ReadWriteMany** PVC; needs an RWX StorageClass
  (NFS/CephFS/...). `fcntl` over a network FS carries a stale-lock risk under
  pathological hangs.
- `type: hostPath` (single-node dev / kind) - a node-local path shared by
  co-located pods.

```sh
helm install my-clink deploy/helm/clink --set ha.enabled=true --set ha.replicas=3 \
  --set ha.storage.pvc.storageClassName=<rwx-class>
```

Verified by `kind-ha-smoke.sh` (single-node hostPath): a leader is elected, TMs
register with it, the leader is killed, a standby takes over, and the TMs
re-register with the new leader.

Follow-on: the **etcd** coordinator is the more robust multi-machine election
primitive (no `fcntl`-over-NFS), but needs a `clink_node` built with
`-DCLINK_WITH_ETCD=ON` + `etcd-cpp-apiv3` - which the default runtime image is
not - so it is not wired here. A CRD Operator (lifecycle/upgrade automation)
remains a separate follow-on.

# clink Helm chart

Deploys a clink streaming cluster on Kubernetes: one JobManager (control plane +
dashboard) and a StatefulSet of TaskManagers. It maps the `docker-compose.yml`
topology onto k8s primitives.

## Status

Statically validated (`helm lint`, `helm template`, `kubeconform -strict` vs the
k8s 1.29 API schemas - 5/5 valid) **and live-verified on a `kind` cluster** via
`kind-smoke.sh`: the JobManager and TaskManagers come up Ready, every TM
registers with the JM (its own pod IP + stable ordinal id), and a cold start is
clean (0 restarts - the `wait-for-jobmanager` initContainer gates the JM race).

```sh
deploy/helm/clink/kind-smoke.sh            # create kind cluster, deploy, assert
deploy/helm/clink/kind-smoke.sh --cleanup  # tear the cluster down
```

The smoke validates the deployment surface (the chart). A job-submission
end-to-end on k8s (build a `CLINK_REGISTER_JOB` `.so` matching the image's
`clink_node` commit, submit via the control port, confirm output) is the
remaining follow-on, along with true multi-JM HA (see below).

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

## HA (follow-on)

clink ships an etcd-backed HA coordinator (`clink_node --etcd-endpoints`), and
`ha.enabled=true` wires those args onto the pods. But this chart v1 still
deploys a **single** JobManager and does not stand up etcd or run multiple JM
replicas, so it is not yet true multi-JM HA. Closing that out (multiple JM
replicas racing for leadership, an etcd endpoint or subchart, and the RBAC for
k8s-native leader election) is the documented next increment.

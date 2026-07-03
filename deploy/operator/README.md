# clink operator

A Kubernetes operator that manages clink clusters declaratively through a
`ClinkCluster` custom resource. Instead of running `helm upgrade` and hand-wiring
JobManager and TaskManager workloads, you apply one custom resource and the
operator reconciles the cluster to match, continuously.

It is built with controller-runtime (Go) and ships as a small static image; it
needs no host Go toolchain to build (the image is a multi-stage Docker build).

## What it does

Given a `ClinkCluster`, the controller creates and owns:

- a **JobManager** Deployment (1 replica, or `ha.replicas` when HA is enabled) plus
  a ClusterIP Service (control + http ports),
- a **TaskManager** StatefulSet (`taskManager.replicas`, parallel pod management,
  a `wait-for-jobmanager` initContainer) plus a headless Service,
- a **ServiceAccount** (unless you supply one),
- for HA, a shared **ReadWriteMany PVC** mounted at `ha.mountPath`, used by the
  file-coordinator leader election.

All owned objects carry an owner reference, so deleting the `ClinkCluster`
garbage-collects the whole cluster. The controller then reports status by reading
the JobManager's `/api/v1/tms` endpoint:

```
$ kubectl get clinkcluster
NAME   PHASE     TMS-READY   DESIRED-TMS   AGE
demo   Running   2           2             40s
```

`.status.phase` is `Pending` until every TaskManager has registered, then
`Running`; `.status.taskManagersReady` is the live registered count.

## Install

```sh
kubectl apply -f config/crd/
kubectl apply -f config/manager/manager.yaml   # Namespace + operator Deployment
kubectl apply -f config/rbac/                   # ServiceAccount + ClusterRole(Binding)
```

The manager image is `clink-operator:latest` (build it with
`docker build -t clink-operator:latest deploy/operator`).

## Use

```yaml
apiVersion: clink.dev/v1alpha1
kind: ClinkCluster
metadata:
  name: demo
spec:
  image:
    repository: clink-runtime
    tag: latest
  taskManager:
    replicas: 2
    slots: 4
  # ha:
  #   enabled: true
  #   replicas: 2        # needs a ReadWriteMany StorageClass
```

Scaling is declarative: edit `spec.taskManager.replicas` and re-apply; the
StatefulSet is reconciled in place. See `config/samples/clinkcluster.yaml`.

## HA

`spec.ha.enabled: true` runs `spec.ha.replicas` JobManagers that elect a leader via
the file coordinator over the shared HA PVC (which must be `ReadWriteMany`). This is
the same coordinator the Helm chart uses; see `deploy/helm/clink`.

## Verify (kind)

`kind-operator-smoke.sh` builds the operator image, stands up a kind cluster, installs
the operator, applies a `ClinkCluster`, and asserts it reconciles to `Running` with
the expected TaskManager count and correct owner references. It requires a local
`clink-runtime:latest` image (`docker build -t clink-runtime:latest -f
docker/Dockerfile.runtime .`).

```sh
deploy/operator/kind-operator-smoke.sh
deploy/operator/kind-operator-smoke.sh --cleanup
```

## Scope

v1 covers the cluster lifecycle: create, scale, HA, status and garbage collection.

Not yet implemented (roadmap): declaring a job in the CR (the JobManager submit API
takes an uploaded `.so`; a `ClinkJob` CR would exec the in-image plugin), and
savepoint-on-upgrade (drain + savepoint + redeploy when the image or job changes).
Both build on capabilities clink already has (savepoints, rescale) but are out of
scope for the lifecycle-focused v1.

## Develop

The generated files (deepcopy, CRD, RBAC role) come from controller-gen:

```sh
cd deploy/operator
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5 object paths=./api/...
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5 crd paths=./api/... output:crd:dir=config/crd
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5 rbac:roleName=clink-operator-role paths=./internal/... output:rbac:dir=config/rbac
go build ./...
```

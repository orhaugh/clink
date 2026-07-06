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

`kind-clinkjob-smoke.sh` covers the `ClinkJob` path (submit, savepoint-on-upgrade,
cancel-on-delete) on top of the same kind setup.

## Jobs (ClinkJob + savepoint-on-upgrade)

A `ClinkJob` runs a compiled job plugin (`.so`, baked into the runtime image) on a
`ClinkCluster` and manages its lifecycle declaratively. The controller submits the
job by exec'ing the in-image `clink` CLI inside a JobManager pod and tracks it over
the JobManager HTTP API.

```yaml
apiVersion: clink.dev/v1alpha1
kind: ClinkJob
metadata:
  name: wordcount
spec:
  clusterName: demo
  jobSo: /opt/clink/jobs/rescale_test_job.so
  jobName: wordcount
  upgradeMode: savepoint        # savepoint | stateless
  checkpointIntervalMs: 2000    # > 0 enables checkpointing (required for savepoint upgrades)
  env:
    - CLINK_RESCALE_COUNT=20000000
```

Changing the spec (`jobSo`, `env`, or `args` - hashed into `.status.specHash`)
triggers an **upgrade**. In `savepoint` mode the controller runs a two-stage,
reconcile-safe rollout:

1. **drain** - trigger a savepoint on the running job, then cancel it. Recorded in
   `.status.lastSavepoint` and guarded by `.status.upgradeToHash` so it happens
   exactly once even if the next stage is slow or retried;
2. **restore** - resubmit the job with `--restore-from-dir`/`--restore-from-checkpoint-id`
   pointing at that savepoint, retried until a new job id appears (adopting an
   already-submitted job so a retry never leaks a second one).

`stateless` mode skips the savepoint and resubmits fresh. Deleting the `ClinkJob`
cancels the running job via a finalizer.

Savepoints must be readable by the pod that restores them, so enable shared
checkpoint storage on the `ClinkCluster` (a `ReadWriteMany` PVC in production, or
`hostPath` on a single-node kind):

```yaml
spec:
  checkpointStorage:
    enabled: true
    type: pvc                 # pvc | hostPath
    mountPath: /var/lib/clink/checkpoints
    storageClassName: nfs     # a ReadWriteMany class
    size: 10Gi
```

```
$ kubectl get clinkjob
NAME        CLUSTER   PHASE     JOBID   AGE
wordcount   demo      Running   2       90s
```

`kind-clinkjob-smoke.sh` exercises the whole path end to end: submit -> Running ->
force an upgrade -> assert a savepoint was taken and a new job id restored from it ->
delete -> assert the finalizer cancelled it.

## Scope

v1 covers the cluster lifecycle (create, scale, HA, status, garbage collection) and
jobs (`ClinkJob` submit, status, savepoint-on-upgrade, cancel-on-delete).

Not yet implemented (roadmap): HA leader-targeting for job exec (v1 targets any ready
JobManager pod, correct for single-JM clusters), and image-driven upgrades (rolling
the runtime image as an upgrade trigger).

## Develop

The generated files (deepcopy, CRD, RBAC role) come from controller-gen:

```sh
cd deploy/operator
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5 object paths=./api/...
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5 crd paths=./api/... output:crd:dir=config/crd
go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5 rbac:roleName=clink-operator-role paths=./internal/... output:rbac:dir=config/rbac
go build ./...
```

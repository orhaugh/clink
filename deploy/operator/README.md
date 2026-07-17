# clink operator

A Kubernetes operator that manages clink clusters declaratively through a
`ClinkCluster` custom resource. Instead of running `helm upgrade` and hand-wiring
Coordinator and Worker workloads, you apply one custom resource and the
operator reconciles the cluster to match, continuously.

It is built with controller-runtime (Go) and ships as a small static image; it
needs no host Go toolchain to build (the image is a multi-stage Docker build).

## What it does

Given a `ClinkCluster`, the controller creates and owns:

- a **Coordinator** Deployment (1 replica, or `ha.replicas` when HA is enabled) plus
  a ClusterIP Service (control + http ports),
- a **Worker** StatefulSet (`worker.replicas`, parallel pod management,
  a `wait-for-coordinator` initContainer) plus a headless Service,
- a **ServiceAccount** (unless you supply one),
- for HA, a shared **ReadWriteMany PVC** mounted at `ha.mountPath`, used by the
  file-coordinator leader election.

All owned objects carry an owner reference, so deleting the `ClinkCluster`
garbage-collects the whole cluster. The controller then reports status by reading
the Coordinator's `/api/v1/workers` endpoint:

```
$ kubectl get clinkcluster
NAME   PHASE     TMS-READY   DESIRED-TMS   AGE
demo   Running   2           2             40s
```

`.status.phase` is `Pending` until every Worker has registered, then
`Running`; `.status.workersReady` is the live registered count.

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
  worker:
    replicas: 2
    slots: 4
  # ha:
  #   enabled: true
  #   replicas: 2        # needs a ReadWriteMany StorageClass
```

Scaling is declarative: edit `spec.worker.replicas` and re-apply; the
StatefulSet is reconciled in place. See `config/samples/clinkcluster.yaml`.

`spec.worker.env` / `spec.coordinator.env` set extra `KEY=VALUE`
environment variables on the pods. Job plugins dlopen'd on the Workers
read their build-time configuration from process env (registered factory
closures capture it at dlopen), so runtime-side job env belongs on the
CLUSTER; `ClinkJob.env` only reaches the submit-side spec build.

## HA

`spec.ha.enabled: true` runs `spec.ha.replicas` Coordinators that elect a leader via
the file coordinator over the shared HA PVC (which must be `ReadWriteMany`). This is
the same coordinator the Helm chart uses; see `deploy/helm/clink`.

## Verify (kind)

`kind-operator-smoke.sh` builds the operator image, stands up a kind cluster, installs
the operator, applies a `ClinkCluster`, and asserts it reconciles to `Running` with
the expected Worker count and correct owner references. It requires a local
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
job by exec'ing the in-image `clink` CLI inside a Coordinator pod and tracks it over
the Coordinator HTTP API.

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

## Scale-to-zero (suspend / resume)

A parked job costs nothing: the operator drains it to a savepoint, cancels it
(freeing its Worker slots for other jobs), and the state waits on shared
storage. Waking is a resubmit restored from that savepoint - seconds-class,
exactly-once. Three triggers:

```yaml
spec:
  suspend: true                # 1. manual park (kubectl patch / GitOps).
                               #    Absolute: no wake policy overrides it.
  suspendPolicy:               # 2. auto-park when the job goes idle:
    idleAfterSeconds: 600      #    no operator processed a record for 10min
                               #    (watched via the coordinator's per-op records_in).
  wakePolicy:                  # 3. auto-wake an Idle-parked job:
    kafkaLag:                  #    poll consumer-group lag while parked,
      brokers: kafka:9092      #    resume when pending records reach the
      topics: [orders]         #    threshold. A never-committed group counts
      groupId: orders-job      #    from the topic start offsets.
      lagThreshold: 100
      pollIntervalSeconds: 15
```

`.status.phase` moves Running -> Suspending -> Suspended (with
`.status.suspendReason` Manual or Idle and the restore point in
`.status.suspendSavepoint`) -> Resuming -> Running. The park is two-stage and
crash-safe: the savepoint is persisted to status before the cancel, and a
`Resuming` job can only finish resuming (it can never fall through to a fresh,
state-dropping submit). Setting `suspend: true` on an Idle-parked job hands the
park to Manual; removing `suspendPolicy` from an Idle-parked job wakes it.
Manual wake is `suspend: false`. Idle detection and lag polling both live in
the operator - the engine is untouched.

One property to design for: a lag-woken job should be the consumer of the lag
it wakes on (the normal case - the group id is the job's own). If the woken
job never reduces that lag, the idle policy re-parks it and the lag policy
re-wakes it, cycling at the idle window - the same feedback rule as any
lag-driven autoscaler.

`kind-suspend-smoke.sh` proves all four paths end to end: manual park (savepoint
recorded, job cancelled on the coordinator) -> manual wake (restored, latency printed) ->
idle auto-park (slow-tick job) -> lag wake against an in-cluster single-node
KRaft Kafka, including the negative check (empty topic keeps it parked, status
reporting `lag 0 < threshold`).

## Scope

v1 covers the cluster lifecycle (create, scale, HA, status, garbage collection) and
jobs (`ClinkJob` submit, status, savepoint-on-upgrade, cancel-on-delete).

Not yet implemented (roadmap): HA leader-targeting for job exec (v1 targets any ready
Coordinator pod, correct for single-coordinator clusters), and image-driven upgrades (rolling
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

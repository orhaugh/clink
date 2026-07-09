package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClinkJobSpec is the desired state of a job running on a ClinkCluster.
type ClinkJobSpec struct {
	// ClusterName is the ClinkCluster (same namespace) to run this job on.
	ClusterName string `json:"clusterName"`
	// JobSo is the path to the compiled job plugin (.so) inside the runtime image.
	JobSo string `json:"jobSo"`
	// +kubebuilder:default="job"
	JobName string `json:"jobName,omitempty"`
	// Args are extra `clink run` flags (e.g. --parallelism). Rarely needed.
	Args []string `json:"args,omitempty"`
	// Env are extra environment variables passed to the job at submit (KEY=VALUE).
	// Changing Env or JobSo triggers an upgrade.
	Env []string `json:"env,omitempty"`
	// CheckpointIntervalMs enables periodic checkpointing (0 = off). Must be > 0
	// for upgradeMode=savepoint to preserve state.
	// +kubebuilder:default=0
	CheckpointIntervalMs int64 `json:"checkpointIntervalMs,omitempty"`
	// UpgradeMode selects how a spec change is rolled out:
	//   savepoint - trigger a savepoint, cancel, resubmit with --restore-from-dir
	//   stateless - cancel and resubmit fresh (no state carried over)
	// +kubebuilder:validation:Enum=savepoint;stateless
	// +kubebuilder:default=savepoint
	UpgradeMode string `json:"upgradeMode,omitempty"`
	// Suspend parks the job when true: the operator drains it to a savepoint
	// (upgradeMode=savepoint), cancels it, and frees its TaskManager slots;
	// state waits in the savepoint. Setting it back to false resubmits the
	// job restored from that savepoint. Manual suspension is absolute: a
	// wakePolicy never overrides it.
	// +kubebuilder:default=false
	Suspend bool `json:"suspend,omitempty"`
	// SuspendPolicy parks the job automatically (scale-to-zero on idle).
	SuspendPolicy *SuspendPolicySpec `json:"suspendPolicy,omitempty"`
	// WakePolicy resumes an auto-parked (reason Idle) job when new work
	// appears. Ignored while spec.suspend is true.
	WakePolicy *WakePolicySpec `json:"wakePolicy,omitempty"`
}

// SuspendPolicySpec configures automatic parking.
type SuspendPolicySpec struct {
	// IdleAfterSeconds parks the job once NO operator has processed a record
	// for this long (observed via the JobManager's per-operator records_in
	// counters). 0 disables auto-park. Pair with a wakePolicy, or the job
	// stays parked until this policy is removed or suspend is toggled.
	// +kubebuilder:validation:Minimum=0
	IdleAfterSeconds int64 `json:"idleAfterSeconds,omitempty"`
}

// WakePolicySpec configures automatic wake-up of an Idle-parked job.
type WakePolicySpec struct {
	// KafkaLag wakes the job when consumer-group lag on the named topics
	// reaches a threshold.
	KafkaLag *KafkaLagWakeSpec `json:"kafkaLag,omitempty"`
}

// KafkaLagWakeSpec: while the job is parked, the operator polls the total
// pending records for a consumer group (end offsets minus the group's
// committed offsets; a partition the group never committed counts from its
// start offset) and resumes the job at the threshold.
type KafkaLagWakeSpec struct {
	// Brokers is the bootstrap list, comma-separated (host:port,...).
	Brokers string `json:"brokers"`
	// Topics to measure.
	// +kubebuilder:validation:MinItems=1
	Topics []string `json:"topics"`
	// GroupID whose committed offsets define the lag baseline - the group
	// the job's Kafka source consumes with.
	GroupID string `json:"groupId"`
	// LagThreshold is the total pending-record count that wakes the job.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	LagThreshold int64 `json:"lagThreshold,omitempty"`
	// PollIntervalSeconds is the lag poll cadence while parked.
	// +kubebuilder:default=15
	// +kubebuilder:validation:Minimum=5
	PollIntervalSeconds int64 `json:"pollIntervalSeconds,omitempty"`
}

// SavepointRef records a savepoint the operator took (dir + checkpoint id).
type SavepointRef struct {
	Dir          string `json:"dir,omitempty"`
	CheckpointID int64  `json:"checkpointId,omitempty"`
}

// ClinkJobStatus is the observed state of a job.
type ClinkJobStatus struct {
	// Phase: Pending (cluster not ready), Submitting, Running, Upgrading,
	// Suspending, Suspended, Completed, Failed, Cancelled.
	Phase string `json:"phase,omitempty"`
	// JobID is the JobManager-assigned id of the currently-running job.
	JobID int64 `json:"jobID,omitempty"`
	// SpecHash is a hash of the running job's inputs (JobSo+Env+Args); a change
	// versus the current spec is what triggers an upgrade.
	SpecHash string `json:"specHash,omitempty"`
	// LastSavepoint is the savepoint taken on the most recent upgrade.
	LastSavepoint *SavepointRef `json:"lastSavepoint,omitempty"`
	// UpgradeToHash is the target spec hash of an in-flight upgrade. It is set
	// once the savepoint + cancel of the old job have completed, so a slow or
	// retried resubmit does not re-take a savepoint on the already-cancelled
	// job. Cleared when the new job reaches Running.
	UpgradeToHash string `json:"upgradeToHash,omitempty"`
	// UpgradeRestore is the savepoint the in-flight upgrade will restore from
	// (nil for a stateless upgrade). Held across resubmit retries.
	UpgradeRestore *SavepointRef `json:"upgradeRestore,omitempty"`
	// ObservedGeneration is the .metadata.generation last reconciled.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Message is a short human-readable status detail.
	Message string `json:"message,omitempty"`
	// SuspendReason records why the job is parked: Manual (spec.suspend) or
	// Idle (suspendPolicy). Cleared on resume.
	SuspendReason string `json:"suspendReason,omitempty"`
	// SuspendSavepoint is the savepoint a resume will restore from (nil for
	// a stateless park). Held until the resumed job reaches Running.
	SuspendSavepoint *SavepointRef `json:"suspendSavepoint,omitempty"`
	// SuspendedAt is when the park completed.
	SuspendedAt *metav1.Time `json:"suspendedAt,omitempty"`
	// ActivityCount is the last observed sum of per-operator records_in for
	// the running job; LastActivityTime is when it last changed. Together
	// they drive suspendPolicy.idleAfterSeconds.
	ActivityCount int64 `json:"activityCount,omitempty"`
	// LastActivityTime - see ActivityCount.
	LastActivityTime *metav1.Time `json:"lastActivityTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=cj
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="JobID",type=integer,JSONPath=`.status.jobID`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ClinkJob is a job (compiled .so plugin) run on a ClinkCluster, with
// declarative savepoint-on-upgrade: changing the spec drains the running job to a
// savepoint and restarts it restored from that savepoint.
type ClinkJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClinkJobSpec   `json:"spec,omitempty"`
	Status ClinkJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClinkJobList is a list of ClinkJob.
type ClinkJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClinkJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClinkJob{}, &ClinkJobList{})
}

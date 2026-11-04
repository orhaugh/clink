package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClinkJobSpec is the desired state of a job running on a ClinkCluster.
// +kubebuilder:validation:XValidation:rule="has(self.jobSo) != has(self.sql)",message="exactly one of jobSo and sql"
// +kubebuilder:validation:XValidation:rule="!has(self.sql) || (!has(self.args) && !has(self.env))",message="args and env are a compiled job's (jobSo); a SQL job takes neither"
// +kubebuilder:validation:XValidation:rule="!has(self.parallelism) || has(self.sql)",message="parallelism is a SQL job's; a compiled job's is in its graph"
type ClinkJobSpec struct {
	// ClusterName is the ClinkCluster (same namespace) to run this job on.
	ClusterName string `json:"clusterName"`
	// JobSo is the path to the compiled job plugin (.so) inside the runtime image.
	JobSo string `json:"jobSo,omitempty"`
	// SQL is the job as a self-contained SQL script: its CREATE TABLE
	// statements and the INSERT INTO that is the job, compiled by the
	// Coordinator in a catalog of the script's own. Changing it triggers an
	// upgrade, as changing jobSo does.
	SQL string `json:"sql,omitempty"`
	// JobName is the job's name on the cluster, in its lineage and under the
	// checkpoint volume. It defaults to the ClinkJob's own name. The operator
	// finds its job on the cluster by this name, so each ClinkJob on a
	// cluster needs its own.
	JobName string `json:"jobName,omitempty"`
	// Parallelism runs a SQL job's operators this many ways (default 1).
	// +kubebuilder:validation:Minimum=1
	Parallelism int32 `json:"parallelism,omitempty"`
	// Args are extra `clink run` flags for a compiled job (e.g.
	// --state-backend=rocksdb). Rarely needed.
	Args []string `json:"args,omitempty"`
	// Env are extra environment variables a compiled job is built with at
	// submit (KEY=VALUE). Changing Env, Args or JobSo triggers an upgrade.
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
	// (upgradeMode=savepoint), cancels it, and frees its Worker slots;
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
	// for this long (observed via the Coordinator's per-operator records_in
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

// KafkaLagWakeSpec: while the job is parked, the operator polls the records
// pending for it on the named topics and resumes the job at the threshold. A
// partition's pending records are its end offset less where the job is taken
// to have read to: where the partition ended when the park began
// (status.wakeBaseline), or else the group's committed offset, or else the
// partition's start offset. A clink job's Kafka source keeps its offsets in
// its checkpoints and commits none to its group, so for a clink job it is the
// baseline that makes the lag what arrived after it parked.
type KafkaLagWakeSpec struct {
	// Brokers is the bootstrap list, comma-separated (host:port,...).
	Brokers string `json:"brokers"`
	// Topics to measure.
	// +kubebuilder:validation:MinItems=1
	Topics []string `json:"topics"`
	// GroupID is the group the job's Kafka source consumes with. Its
	// committed offsets count where no baseline was recorded at the park.
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

// PartitionOffset is an offset in one partition of a topic.
type PartitionOffset struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
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
	// JobID is the Coordinator-assigned id of the currently-running job.
	JobID int64 `json:"jobID,omitempty"`
	// SpecHash is a hash of the running job's inputs (JobSo, Env and Args, or
	// SQL and Parallelism); a change versus the current spec is what triggers
	// an upgrade.
	SpecHash string `json:"specHash,omitempty"`
	// LastSavepoint is the savepoint taken on the most recent upgrade.
	LastSavepoint *SavepointRef `json:"lastSavepoint,omitempty"`
	// UpgradeToHash is the target spec hash of an in-flight upgrade. It is set
	// once the savepoint + cancel of the old job have completed, so a slow or
	// retried resubmit does not re-take a savepoint on the already-cancelled
	// job. Cleared when the new job reaches Running.
	UpgradeToHash string `json:"upgradeToHash,omitempty"`
	// UpgradeRestore is the savepoint the in-flight upgrade will restore from
	// (nil for a stateless upgrade). Held across resubmit retries. It is
	// recorded before the old job is cancelled, with the job it was taken
	// from in UpgradeFromJobID, so a cancel whose status update is lost does
	// not lose it: a retry that finds that job no longer live restores from
	// it rather than asking a cancelled job for another savepoint.
	UpgradeRestore *SavepointRef `json:"upgradeRestore,omitempty"`
	// UpgradeFromJobID is the job UpgradeRestore was taken from.
	UpgradeFromJobID int64 `json:"upgradeFromJobID,omitempty"`
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
	// WakeBaseline is where each partition of the wake policy's topics ended
	// when the operator decided to park the job for being idle, read before
	// the savepoint was asked for. The wake policy counts what was produced
	// after it. Cleared when the job runs again, and when it does work before
	// the park begins.
	WakeBaseline []PartitionOffset `json:"wakeBaseline,omitempty"`
	// ActivityCount is the last observed sum of per-operator records_in for
	// the running job; LastActivityTime is when it last changed. Together
	// they drive suspendPolicy.idleAfterSeconds.
	ActivityCount int64 `json:"activityCount,omitempty"`
	// LastActivityTime - see ActivityCount.
	LastActivityTime *metav1.Time `json:"lastActivityTime,omitempty"`
	// JobMissingSince is when the cluster stopped listing the job JobID names.
	// The operator resubmits the job only once it has been missing for two
	// minutes: a job a new leader is still recovering, or one parked until a
	// Worker returns, is missing from the list for a while and then runs, and
	// resubmitting it at once would run it twice.
	JobMissingSince *metav1.Time `json:"jobMissingSince,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=cj
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="JobID",type=integer,JSONPath=`.status.jobID`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ClinkJob is a job (a compiled .so plugin, or a SQL script) run on a ClinkCluster, with
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

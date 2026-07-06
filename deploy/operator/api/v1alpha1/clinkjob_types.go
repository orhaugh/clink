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
}

// SavepointRef records a savepoint the operator took (dir + checkpoint id).
type SavepointRef struct {
	Dir          string `json:"dir,omitempty"`
	CheckpointID int64  `json:"checkpointId,omitempty"`
}

// ClinkJobStatus is the observed state of a job.
type ClinkJobStatus struct {
	// Phase: Pending (cluster not ready), Submitting, Running, Upgrading,
	// Completed, Failed, Cancelled.
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

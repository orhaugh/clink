package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ImageSpec pins the clink runtime image the JobManager + TaskManagers run.
type ImageSpec struct {
	// +kubebuilder:default="clink-runtime"
	Repository string `json:"repository,omitempty"`
	// +kubebuilder:default="latest"
	Tag string `json:"tag,omitempty"`
	// +kubebuilder:validation:Enum=Always;IfNotPresent;Never
	// +kubebuilder:default=IfNotPresent
	PullPolicy corev1.PullPolicy `json:"pullPolicy,omitempty"`
}

// JobManagerSpec configures the JobManager role.
type JobManagerSpec struct {
	// +kubebuilder:default=6123
	ControlPort int32 `json:"controlPort,omitempty"`
	// +kubebuilder:default=8081
	HTTPPort int32 `json:"httpPort,omitempty"`
	// StateBackend passes --state-backend when set (e.g. "rocksdb").
	StateBackend string `json:"stateBackend,omitempty"`
	// ExtraArgs are appended verbatim to the JobManager command line.
	ExtraArgs []string                    `json:"extraArgs,omitempty"`
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
	// Env sets extra environment variables (KEY=VALUE) on the JobManager
	// container. Job plugins dlopen'd in this process read their build-time
	// configuration from process env, so this is how per-cluster job env is
	// supplied (ClinkJob.env only reaches the submit-side build).
	Env []string `json:"env,omitempty"`
}

// TaskManagerSpec configures the TaskManager role.
type TaskManagerSpec struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=2
	Replicas int32 `json:"replicas,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=4
	Slots int32 `json:"slots,omitempty"`
	// +kubebuilder:default=8082
	HTTPPort int32 `json:"httpPort,omitempty"`
	// +kubebuilder:default=30
	TerminationGracePeriodSeconds int64                       `json:"terminationGracePeriodSeconds,omitempty"`
	ExtraArgs                     []string                    `json:"extraArgs,omitempty"`
	Resources                     corev1.ResourceRequirements `json:"resources,omitempty"`
	// Env sets extra environment variables (KEY=VALUE) on every TaskManager
	// container. Job plugins dlopen'd on the TaskManagers read their
	// build-time configuration from process env (registered factory closures
	// capture it), so runtime-side job env belongs here, not on ClinkJob.
	Env []string `json:"env,omitempty"`
}

// HASpec turns on multi-JobManager high availability with file-coordinator
// leader election over a shared RWX volume.
type HASpec struct {
	Enabled bool `json:"enabled,omitempty"`
	// +kubebuilder:validation:Minimum=2
	// +kubebuilder:default=2
	Replicas int32 `json:"replicas,omitempty"`
	// +kubebuilder:default="/var/lib/clink/ha"
	MountPath string `json:"mountPath,omitempty"`
	// StorageClassName for the shared HA PVC (must support ReadWriteMany).
	StorageClassName *string `json:"storageClassName,omitempty"`
	// +kubebuilder:default="1Gi"
	Size string `json:"size,omitempty"`
}

// CheckpointStorageSpec mounts a shared volume into every JobManager and
// TaskManager pod at MountPath, so checkpoints and savepoints written by one pod
// are readable by another (required for savepoint-on-upgrade restore, where the
// resubmitted job's TaskManagers must read the savepoint the old job wrote). A
// ClinkJob points its --checkpoint-dir under this path.
type CheckpointStorageSpec struct {
	Enabled bool `json:"enabled,omitempty"`
	// +kubebuilder:default="/var/lib/clink/checkpoints"
	MountPath string `json:"mountPath,omitempty"`
	// +kubebuilder:validation:Enum=pvc;hostPath
	// +kubebuilder:default=pvc
	Type string `json:"type,omitempty"`
	// StorageClassName for the shared PVC (must support ReadWriteMany).
	StorageClassName *string `json:"storageClassName,omitempty"`
	// +kubebuilder:default="2Gi"
	Size string `json:"size,omitempty"`
	// HostPath is used when Type=hostPath (single-node clusters e.g. kind, where
	// all pods share the node filesystem). Ignored for Type=pvc.
	// +kubebuilder:default="/var/lib/clink-checkpoints"
	HostPath string `json:"hostPath,omitempty"`
}

// ClinkClusterSpec is the desired state of a clink cluster.
type ClinkClusterSpec struct {
	Image              ImageSpec                     `json:"image,omitempty"`
	ServiceAccountName string                        `json:"serviceAccountName,omitempty"`
	ImagePullSecrets   []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
	JobManager         JobManagerSpec                `json:"jobManager,omitempty"`
	TaskManager        TaskManagerSpec               `json:"taskManager,omitempty"`
	HA                 HASpec                        `json:"ha,omitempty"`
	CheckpointStorage  CheckpointStorageSpec         `json:"checkpointStorage,omitempty"`
}

// ClinkClusterStatus is the observed state of a clink cluster.
type ClinkClusterStatus struct {
	// Phase is a coarse lifecycle summary: Pending, Running or Failed.
	Phase string `json:"phase,omitempty"`
	// JobManagerReplicas is the desired JobManager replica count (1, or ha.replicas).
	JobManagerReplicas int32 `json:"jobManagerReplicas,omitempty"`
	// TaskManagersReady is the number of TaskManagers currently registered with
	// the JobManager (read from its /api/v1/tms endpoint).
	TaskManagersReady int32 `json:"taskManagersReady,omitempty"`
	// ObservedGeneration is the .metadata.generation last reconciled.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Conditions follow the standard k8s condition convention.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=clink;cc
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="TMs-Ready",type=integer,JSONPath=`.status.taskManagersReady`
// +kubebuilder:printcolumn:name="Desired-TMs",type=integer,JSONPath=`.spec.taskManager.replicas`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ClinkCluster is a clink stream-processing cluster (a JobManager plus a set of
// TaskManagers) managed declaratively by the clink operator.
type ClinkCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClinkClusterSpec   `json:"spec,omitempty"`
	Status ClinkClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClinkClusterList is a list of ClinkCluster.
type ClinkClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClinkCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClinkCluster{}, &ClinkClusterList{})
}

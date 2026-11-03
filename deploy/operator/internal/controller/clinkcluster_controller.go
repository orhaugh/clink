package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	clinkv1alpha1 "github.com/clink/clink-operator/api/v1alpha1"
)

// ClinkClusterReconciler reconciles a ClinkCluster into its owned workloads.
type ClinkClusterReconciler struct {
	client.Client
	// APIReader reads the cluster's auth Secret from the API server, not the
	// cache, so the operator never lists or watches Secrets.
	APIReader client.Reader
	Scheme    *runtime.Scheme
}

// +kubebuilder:rbac:groups=clink.dev,resources=clinkclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=clink.dev,resources=clinkclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=clink.dev,resources=clinkclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;serviceaccounts;persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get

// Reconcile drives the actual cluster state toward the ClinkCluster spec.
func (r *ClinkClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx)

	var cc clinkv1alpha1.ClinkCluster
	if err := r.Get(ctx, req.NamespacedName, &cc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Normalize zero-value fields to their documented defaults. CRD/OpenAPI
	// defaulting does not materialize a wholly-omitted nested object (e.g. an
	// absent spec.coordinator) to fill its field defaults, so the operator applies
	// them itself - correctness must not depend on API-server defaulting.
	applyDefaults(&cc)

	jmReplicas := int32(1)
	if cc.Spec.HA.Enabled {
		jmReplicas = cc.Spec.HA.Replicas
	}

	// Reconcile the owned objects. Order: SA -> (HA PVC) -> Services -> workloads.
	if err := r.reconcileServiceAccount(ctx, &cc); err != nil {
		return ctrl.Result{}, err
	}
	if cc.Spec.HA.Enabled && cc.Spec.HA.Type == "pvc" {
		if err := r.reconcileHAPVC(ctx, &cc); err != nil {
			return ctrl.Result{}, err
		}
	}
	if cc.Spec.CheckpointStorage.Enabled && cc.Spec.CheckpointStorage.Type == "pvc" {
		if err := r.reconcileCheckpointPVC(ctx, &cc); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.reconcileCoordinatorService(ctx, &cc); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileTMHeadlessService(ctx, &cc); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileCoordinators(ctx, &cc, jmReplicas); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileTMStatefulSet(ctx, &cc); err != nil {
		return ctrl.Result{}, err
	}

	tmReady := r.registeredWorkers(ctx, &cc)

	// Status.
	phase := "Pending"
	if tmReady >= cc.Spec.Worker.Replicas && cc.Spec.Worker.Replicas > 0 {
		phase = "Running"
	}
	cc.Status.Phase = phase
	cc.Status.CoordinatorReplicas = jmReplicas
	cc.Status.WorkersReady = tmReady
	cc.Status.ObservedGeneration = cc.Generation
	if err := r.Status().Update(ctx, &cc); err != nil {
		lg.Info("status update deferred", "reason", err.Error())
	}

	// Requeue to keep status fresh until Running, then a slower cadence.
	if phase != "Running" {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// applyDefaults fills zero-value spec fields with the documented defaults so the
// operator behaves correctly for a minimal CR (only image + worker.replicas,
// say) regardless of whether CRD-level defaulting materialized nested objects.
func applyDefaults(cc *clinkv1alpha1.ClinkCluster) {
	s := &cc.Spec
	if s.Image.Repository == "" {
		s.Image.Repository = "ghcr.io/orhaugh/clink-runtime"
	}
	if s.Image.Tag == "" {
		s.Image.Tag = "main"
	}
	if s.Image.PullPolicy == "" {
		s.Image.PullPolicy = corev1.PullIfNotPresent
	}
	if s.Coordinator.ControlPort == 0 {
		s.Coordinator.ControlPort = 6123
	}
	if s.Coordinator.HTTPPort == 0 {
		s.Coordinator.HTTPPort = 8081
	}
	if s.Worker.Replicas == 0 {
		s.Worker.Replicas = 2
	}
	if s.Worker.Slots == 0 {
		s.Worker.Slots = 4
	}
	if s.Worker.HTTPPort == 0 {
		s.Worker.HTTPPort = 8082
	}
	if s.Worker.TerminationGracePeriodSeconds == 0 {
		s.Worker.TerminationGracePeriodSeconds = 30
	}
	if s.HA.Enabled {
		if s.HA.Replicas < 2 {
			s.HA.Replicas = 2
		}
		if s.HA.MountPath == "" {
			s.HA.MountPath = "/var/lib/clink/ha"
		}
		if s.HA.Size == "" {
			s.HA.Size = "1Gi"
		}
		if s.HA.Type == "" {
			s.HA.Type = "pvc"
		}
		if s.HA.HostPath == "" {
			s.HA.HostPath = "/var/lib/clink-ha"
		}
	}
	if s.CheckpointStorage.Enabled {
		if s.CheckpointStorage.MountPath == "" {
			s.CheckpointStorage.MountPath = "/var/lib/clink/checkpoints"
		}
		if s.CheckpointStorage.Type == "" {
			s.CheckpointStorage.Type = "pvc"
		}
		if s.CheckpointStorage.Size == "" {
			s.CheckpointStorage.Size = "2Gi"
		}
		if s.CheckpointStorage.HostPath == "" {
			s.CheckpointStorage.HostPath = "/var/lib/clink-checkpoints"
		}
	}
}

// ---- naming + labels -------------------------------------------------------

func (r *ClinkClusterReconciler) jmName(cc *clinkv1alpha1.ClinkCluster) string {
	return cc.Name + "-coordinator"
}
func (r *ClinkClusterReconciler) tmName(cc *clinkv1alpha1.ClinkCluster) string {
	return cc.Name + "-worker"
}
func (r *ClinkClusterReconciler) tmHeadlessName(cc *clinkv1alpha1.ClinkCluster) string {
	return cc.Name + "-worker-headless"
}
func (r *ClinkClusterReconciler) haPVCName(cc *clinkv1alpha1.ClinkCluster) string {
	return cc.Name + "-ha"
}
func (r *ClinkClusterReconciler) checkpointPVCName(cc *clinkv1alpha1.ClinkCluster) string {
	return cc.Name + "-checkpoints"
}

// checkpointVolume returns the shared checkpoint volume + mount for a coordinator/worker pod,
// or ok=false when checkpoint storage is disabled.
func (r *ClinkClusterReconciler) checkpointVolume(cc *clinkv1alpha1.ClinkCluster) (corev1.Volume, corev1.VolumeMount, bool) {
	cs := cc.Spec.CheckpointStorage
	if !cs.Enabled {
		return corev1.Volume{}, corev1.VolumeMount{}, false
	}
	mount := corev1.VolumeMount{Name: "checkpoints", MountPath: cs.MountPath}
	var src corev1.VolumeSource
	if cs.Type == "hostPath" {
		hpType := corev1.HostPathDirectoryOrCreate
		src = corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: cs.HostPath, Type: &hpType}}
	} else {
		src = corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: r.checkpointPVCName(cc)}}
	}
	return corev1.Volume{Name: "checkpoints", VolumeSource: src}, mount, true
}
func (r *ClinkClusterReconciler) serviceAccountName(cc *clinkv1alpha1.ClinkCluster) string {
	if cc.Spec.ServiceAccountName != "" {
		return cc.Spec.ServiceAccountName
	}
	return cc.Name
}

func baseLabels(cc *clinkv1alpha1.ClinkCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "clink",
		"app.kubernetes.io/instance":   cc.Name,
		"app.kubernetes.io/managed-by": "clink-operator",
	}
}
func componentLabels(cc *clinkv1alpha1.ClinkCluster, component string) map[string]string {
	l := baseLabels(cc)
	l["app.kubernetes.io/component"] = component
	return l
}
func selectorLabels(cc *clinkv1alpha1.ClinkCluster, component string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/instance":  cc.Name,
		"app.kubernetes.io/component": component,
	}
}

func (r *ClinkClusterReconciler) image(cc *clinkv1alpha1.ClinkCluster) string {
	return fmt.Sprintf("%s:%s", cc.Spec.Image.Repository, cc.Spec.Image.Tag)
}

// ---- owned resources -------------------------------------------------------

func (r *ClinkClusterReconciler) reconcileServiceAccount(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) error {
	if cc.Spec.ServiceAccountName != "" {
		return nil // caller-supplied SA; do not manage it
	}
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: r.serviceAccountName(cc), Namespace: cc.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		sa.Labels = baseLabels(cc)
		return controllerutil.SetControllerReference(cc, sa, r.Scheme)
	})
	return err
}

func (r *ClinkClusterReconciler) reconcileHAPVC(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) error {
	qty, err := resource.ParseQuantity(cc.Spec.HA.Size)
	if err != nil {
		return fmt.Errorf("invalid ha.size %q: %w", cc.Spec.HA.Size, err)
	}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: r.haPVCName(cc), Namespace: cc.Namespace}}
	// A PVC spec is largely immutable; only create it if absent.
	if err := r.Get(ctx, types.NamespacedName{Name: pvc.Name, Namespace: cc.Namespace}, pvc); err == nil {
		return nil
	}
	pvc.Labels = baseLabels(cc)
	pvc.Spec = corev1.PersistentVolumeClaimSpec{
		AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		StorageClassName: cc.Spec.HA.StorageClassName,
		Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: qty}},
	}
	if err := controllerutil.SetControllerReference(cc, pvc, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, pvc)
}

func (r *ClinkClusterReconciler) reconcileCheckpointPVC(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) error {
	qty, err := resource.ParseQuantity(cc.Spec.CheckpointStorage.Size)
	if err != nil {
		return fmt.Errorf("invalid checkpointStorage.size %q: %w", cc.Spec.CheckpointStorage.Size, err)
	}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: r.checkpointPVCName(cc), Namespace: cc.Namespace}}
	if err := r.Get(ctx, types.NamespacedName{Name: pvc.Name, Namespace: cc.Namespace}, pvc); err == nil {
		return nil // PVC spec is largely immutable; create-once
	}
	pvc.Labels = baseLabels(cc)
	pvc.Spec = corev1.PersistentVolumeClaimSpec{
		AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		StorageClassName: cc.Spec.CheckpointStorage.StorageClassName,
		Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: qty}},
	}
	if err := controllerutil.SetControllerReference(cc, pvc, r.Scheme); err != nil {
		return err
	}
	return r.Create(ctx, pvc)
}

func (r *ClinkClusterReconciler) reconcileCoordinatorService(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: r.jmName(cc), Namespace: cc.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = componentLabels(cc, "coordinator")
		svc.Spec.Selector = selectorLabels(cc, "coordinator")
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		svc.Spec.Ports = []corev1.ServicePort{
			{Name: "control", Port: cc.Spec.Coordinator.ControlPort, TargetPort: intstr.FromInt32(cc.Spec.Coordinator.ControlPort)},
			{Name: "http", Port: cc.Spec.Coordinator.HTTPPort, TargetPort: intstr.FromInt32(cc.Spec.Coordinator.HTTPPort)},
		}
		return controllerutil.SetControllerReference(cc, svc, r.Scheme)
	})
	return err
}

func (r *ClinkClusterReconciler) reconcileTMHeadlessService(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: r.tmHeadlessName(cc), Namespace: cc.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = componentLabels(cc, "worker")
		svc.Spec.Selector = selectorLabels(cc, "worker")
		svc.Spec.ClusterIP = corev1.ClusterIPNone
		svc.Spec.PublishNotReadyAddresses = true
		svc.Spec.Ports = []corev1.ServicePort{
			{Name: "http", Port: cc.Spec.Worker.HTTPPort, TargetPort: intstr.FromInt32(cc.Spec.Worker.HTTPPort)},
		}
		return controllerutil.SetControllerReference(cc, svc, r.Scheme)
	})
	return err
}

// reconcileCoordinators applies the Coordinators: without HA one Deployment,
// with HA a StatefulSet behind its headless Service, so each Coordinator has
// the stable name it advertises. A cluster that changed modes, or one an
// earlier operator made with an HA Deployment, loses the other kind here.
func (r *ClinkClusterReconciler) reconcileCoordinators(ctx context.Context, cc *clinkv1alpha1.ClinkCluster, replicas int32) error {
	key := metav1.ObjectMeta{Name: r.jmName(cc), Namespace: cc.Namespace}
	if !cc.Spec.HA.Enabled {
		if err := r.reconcileCoordinatorDeployment(ctx, cc); err != nil {
			return err
		}
		if err := r.deleteIfOwned(ctx, cc, &appsv1.StatefulSet{ObjectMeta: key}); err != nil {
			return err
		}
		return r.deleteIfOwned(ctx, cc, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: r.jmHeadlessName(cc), Namespace: cc.Namespace}})
	}
	if err := r.reconcileCoordinatorHeadlessService(ctx, cc); err != nil {
		return err
	}
	if err := r.reconcileCoordinatorStatefulSet(ctx, cc, replicas); err != nil {
		return err
	}
	return r.deleteIfOwned(ctx, cc, &appsv1.Deployment{ObjectMeta: key})
}

// deleteIfOwned deletes obj when it exists and this ClinkCluster controls it.
func (r *ClinkClusterReconciler) deleteIfOwned(ctx context.Context, cc *clinkv1alpha1.ClinkCluster, obj client.Object) error {
	if err := r.Get(ctx, client.ObjectKeyFromObject(obj), obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(obj, cc) {
		return nil
	}
	return client.IgnoreNotFound(r.Delete(ctx, obj))
}

func (r *ClinkClusterReconciler) reconcileCoordinatorDeployment(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) error {
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: r.jmName(cc), Namespace: cc.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		one := int32(1)
		dep.Labels = componentLabels(cc, "coordinator")
		dep.Spec.Replicas = &one
		dep.Spec.Strategy = appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
		dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: selectorLabels(cc, "coordinator")}
		dep.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: componentLabels(cc, "coordinator")},
			Spec:       r.coordinatorPodSpec(cc),
		}
		return controllerutil.SetControllerReference(cc, dep, r.Scheme)
	})
	return err
}

func (r *ClinkClusterReconciler) reconcileCoordinatorStatefulSet(ctx context.Context, cc *clinkv1alpha1.ClinkCluster, replicas int32) error {
	ss := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: r.jmName(cc), Namespace: cc.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, ss, func() error {
		ss.Labels = componentLabels(cc, "coordinator")
		ss.Spec.Replicas = &replicas
		ss.Spec.ServiceName = r.jmHeadlessName(cc)
		// Start every replica at once: the election, not the ordinal, picks the leader.
		ss.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		ss.Spec.Selector = &metav1.LabelSelector{MatchLabels: selectorLabels(cc, "coordinator")}
		ss.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: componentLabels(cc, "coordinator")},
			Spec:       r.coordinatorPodSpec(cc),
		}
		return controllerutil.SetControllerReference(cc, ss, r.Scheme)
	})
	return err
}

// reconcileCoordinatorHeadlessService gives each HA Coordinator the stable DNS
// name it advertises as leader. Not-ready addresses are published, so a
// leader's name resolves for the Workers while it is still binding.
func (r *ClinkClusterReconciler) reconcileCoordinatorHeadlessService(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: r.jmHeadlessName(cc), Namespace: cc.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = componentLabels(cc, "coordinator")
		svc.Spec.Selector = selectorLabels(cc, "coordinator")
		svc.Spec.ClusterIP = corev1.ClusterIPNone
		svc.Spec.PublishNotReadyAddresses = true
		svc.Spec.Ports = []corev1.ServicePort{
			{Name: "control", Port: cc.Spec.Coordinator.ControlPort, TargetPort: intstr.FromInt32(cc.Spec.Coordinator.ControlPort)},
			{Name: "http", Port: cc.Spec.Coordinator.HTTPPort, TargetPort: intstr.FromInt32(cc.Spec.Coordinator.HTTPPort)},
		}
		return controllerutil.SetControllerReference(cc, svc, r.Scheme)
	})
	return err
}

func (r *ClinkClusterReconciler) reconcileTMStatefulSet(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) error {
	ss := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: r.tmName(cc), Namespace: cc.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, ss, func() error {
		ss.Labels = componentLabels(cc, "worker")
		replicas := cc.Spec.Worker.Replicas
		ss.Spec.Replicas = &replicas
		ss.Spec.ServiceName = r.tmHeadlessName(cc)
		ss.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		ss.Spec.Selector = &metav1.LabelSelector{MatchLabels: selectorLabels(cc, "worker")}
		ss.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: componentLabels(cc, "worker")},
			Spec:       r.workerPodSpec(cc),
		}
		return controllerutil.SetControllerReference(cc, ss, r.Scheme)
	})
	return err
}

func httpProbe(path, port string, initialDelay, period, failureThreshold int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromString(port)}},
		InitialDelaySeconds: initialDelay,
		PeriodSeconds:       period,
		TimeoutSeconds:      2,
		FailureThreshold:    failureThreshold,
	}
}

// parseEnvKVs turns KEY=VALUE strings (the ClinkCluster env format) into
// container env vars; malformed entries (no '=') are skipped.
func parseEnvKVs(kvs []string) []corev1.EnvVar {
	var out []corev1.EnvVar
	for _, kv := range kvs {
		if k, v, ok := strings.Cut(kv, "="); ok && k != "" {
			out = append(out, corev1.EnvVar{Name: k, Value: v})
		}
	}
	return out
}

// ---- Coordinator HTTP --------------------------------------------------------

// registeredWorkers counts the Workers the Coordinator has registered and not
// lost, asking with the cluster's token; 0 while the Coordinator cannot answer
// (the cluster is still coming up).
func (r *ClinkClusterReconciler) registeredWorkers(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) int32 {
	api, err := newCoordinatorClient(ctx, r.APIReader, cc)
	if err != nil {
		log.FromContext(ctx).Info("cannot present the cluster's token", "err", err.Error())
		return 0
	}
	n, err := api.liveWorkers(ctx)
	if err != nil {
		return 0
	}
	return n
}

// SetupWithManager wires the reconciler to watch ClinkCluster + its owned kinds.
func (r *ClinkClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Scheme = mgr.GetScheme()
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&clinkv1alpha1.ClinkCluster{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Complete(r)
}

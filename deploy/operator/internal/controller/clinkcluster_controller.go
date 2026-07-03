package controller

import (
	"context"
	"fmt"
	"io"
	"net/http"
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
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=clink.dev,resources=clinkclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=clink.dev,resources=clinkclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=clink.dev,resources=clinkclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;serviceaccounts;persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile drives the actual cluster state toward the ClinkCluster spec.
func (r *ClinkClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := log.FromContext(ctx)

	var cc clinkv1alpha1.ClinkCluster
	if err := r.Get(ctx, req.NamespacedName, &cc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Normalize zero-value fields to their documented defaults. CRD/OpenAPI
	// defaulting does not materialize a wholly-omitted nested object (e.g. an
	// absent spec.jobManager) to fill its field defaults, so the operator applies
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
	if cc.Spec.HA.Enabled {
		if err := r.reconcileHAPVC(ctx, &cc); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.reconcileJMService(ctx, &cc); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileTMHeadlessService(ctx, &cc); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileJMDeployment(ctx, &cc, jmReplicas); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileTMStatefulSet(ctx, &cc); err != nil {
		return ctrl.Result{}, err
	}

	tmReady := r.registeredTaskManagers(ctx, &cc)

	// Status.
	phase := "Pending"
	if tmReady >= cc.Spec.TaskManager.Replicas && cc.Spec.TaskManager.Replicas > 0 {
		phase = "Running"
	}
	cc.Status.Phase = phase
	cc.Status.JobManagerReplicas = jmReplicas
	cc.Status.TaskManagersReady = tmReady
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
// operator behaves correctly for a minimal CR (only image + taskManager.replicas,
// say) regardless of whether CRD-level defaulting materialized nested objects.
func applyDefaults(cc *clinkv1alpha1.ClinkCluster) {
	s := &cc.Spec
	if s.Image.Repository == "" {
		s.Image.Repository = "clink-runtime"
	}
	if s.Image.Tag == "" {
		s.Image.Tag = "latest"
	}
	if s.Image.PullPolicy == "" {
		s.Image.PullPolicy = corev1.PullIfNotPresent
	}
	if s.JobManager.ControlPort == 0 {
		s.JobManager.ControlPort = 6123
	}
	if s.JobManager.HTTPPort == 0 {
		s.JobManager.HTTPPort = 8081
	}
	if s.TaskManager.Replicas == 0 {
		s.TaskManager.Replicas = 2
	}
	if s.TaskManager.Slots == 0 {
		s.TaskManager.Slots = 4
	}
	if s.TaskManager.HTTPPort == 0 {
		s.TaskManager.HTTPPort = 8082
	}
	if s.TaskManager.TerminationGracePeriodSeconds == 0 {
		s.TaskManager.TerminationGracePeriodSeconds = 30
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
	}
}

// ---- naming + labels -------------------------------------------------------

func (r *ClinkClusterReconciler) jmName(cc *clinkv1alpha1.ClinkCluster) string {
	return cc.Name + "-jobmanager"
}
func (r *ClinkClusterReconciler) tmName(cc *clinkv1alpha1.ClinkCluster) string {
	return cc.Name + "-taskmanager"
}
func (r *ClinkClusterReconciler) tmHeadlessName(cc *clinkv1alpha1.ClinkCluster) string {
	return cc.Name + "-taskmanager-headless"
}
func (r *ClinkClusterReconciler) haPVCName(cc *clinkv1alpha1.ClinkCluster) string {
	return cc.Name + "-ha"
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

func (r *ClinkClusterReconciler) reconcileJMService(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: r.jmName(cc), Namespace: cc.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = componentLabels(cc, "jobmanager")
		svc.Spec.Selector = selectorLabels(cc, "jobmanager")
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		svc.Spec.Ports = []corev1.ServicePort{
			{Name: "control", Port: cc.Spec.JobManager.ControlPort, TargetPort: intstr.FromInt32(cc.Spec.JobManager.ControlPort)},
			{Name: "http", Port: cc.Spec.JobManager.HTTPPort, TargetPort: intstr.FromInt32(cc.Spec.JobManager.HTTPPort)},
		}
		return controllerutil.SetControllerReference(cc, svc, r.Scheme)
	})
	return err
}

func (r *ClinkClusterReconciler) reconcileTMHeadlessService(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: r.tmHeadlessName(cc), Namespace: cc.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = componentLabels(cc, "taskmanager")
		svc.Spec.Selector = selectorLabels(cc, "taskmanager")
		svc.Spec.ClusterIP = corev1.ClusterIPNone
		svc.Spec.PublishNotReadyAddresses = true
		svc.Spec.Ports = []corev1.ServicePort{
			{Name: "http", Port: cc.Spec.TaskManager.HTTPPort, TargetPort: intstr.FromInt32(cc.Spec.TaskManager.HTTPPort)},
		}
		return controllerutil.SetControllerReference(cc, svc, r.Scheme)
	})
	return err
}

func (r *ClinkClusterReconciler) reconcileJMDeployment(ctx context.Context, cc *clinkv1alpha1.ClinkCluster, replicas int32) error {
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: r.jmName(cc), Namespace: cc.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		dep.Labels = componentLabels(cc, "jobmanager")
		dep.Spec.Replicas = &replicas
		strat := appsv1.RecreateDeploymentStrategyType
		if cc.Spec.HA.Enabled {
			strat = appsv1.RollingUpdateDeploymentStrategyType
		}
		dep.Spec.Strategy = appsv1.DeploymentStrategy{Type: strat}
		dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: selectorLabels(cc, "jobmanager")}

		args := []string{
			"--role=jm",
			fmt.Sprintf("--port=%d", cc.Spec.JobManager.ControlPort),
			"--bind-host=0.0.0.0",
		}
		var env []corev1.EnvVar
		if cc.Spec.HA.Enabled {
			env = append(env, corev1.EnvVar{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}})
			args = append(args, "--advertise-host=$(POD_IP)", "--ha-dir="+cc.Spec.HA.MountPath)
		} else {
			args = append(args, "--advertise-host="+r.jmName(cc))
		}
		args = append(args, fmt.Sprintf("--http-port=%d", cc.Spec.JobManager.HTTPPort), "--http-bind=0.0.0.0")
		if cc.Spec.JobManager.StateBackend != "" {
			args = append(args, "--state-backend="+cc.Spec.JobManager.StateBackend)
		}
		args = append(args, cc.Spec.JobManager.ExtraArgs...)

		c := corev1.Container{
			Name:            "jobmanager",
			Image:           r.image(cc),
			ImagePullPolicy: cc.Spec.Image.PullPolicy,
			Args:            args,
			Env:             env,
			Ports: []corev1.ContainerPort{
				{Name: "control", ContainerPort: cc.Spec.JobManager.ControlPort},
				{Name: "http", ContainerPort: cc.Spec.JobManager.HTTPPort},
			},
			ReadinessProbe: httpProbe("/api/v1/health", "http", 2, 5, 6),
			LivenessProbe:  httpProbe("/api/v1/health", "http", 10, 10, 6),
			Resources:      cc.Spec.JobManager.Resources,
		}
		var volumes []corev1.Volume
		if cc.Spec.HA.Enabled {
			c.VolumeMounts = []corev1.VolumeMount{{Name: "ha", MountPath: cc.Spec.HA.MountPath}}
			volumes = []corev1.Volume{{Name: "ha", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: r.haPVCName(cc)},
			}}}
		}
		dep.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: componentLabels(cc, "jobmanager")},
			Spec: corev1.PodSpec{
				ServiceAccountName: r.serviceAccountName(cc),
				ImagePullSecrets:   cc.Spec.ImagePullSecrets,
				Containers:         []corev1.Container{c},
				Volumes:            volumes,
			},
		}
		return controllerutil.SetControllerReference(cc, dep, r.Scheme)
	})
	return err
}

func (r *ClinkClusterReconciler) reconcileTMStatefulSet(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) error {
	ss := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: r.tmName(cc), Namespace: cc.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, ss, func() error {
		ss.Labels = componentLabels(cc, "taskmanager")
		replicas := cc.Spec.TaskManager.Replicas
		ss.Spec.Replicas = &replicas
		ss.Spec.ServiceName = r.tmHeadlessName(cc)
		ss.Spec.PodManagementPolicy = appsv1.ParallelPodManagement
		ss.Spec.Selector = &metav1.LabelSelector{MatchLabels: selectorLabels(cc, "taskmanager")}

		waitCmd := fmt.Sprintf(
			"until curl -fsS \"http://%s:%d/api/v1/health\" >/dev/null 2>&1; do echo waiting for jobmanager...; sleep 2; done; echo jobmanager is ready",
			r.jmName(cc), cc.Spec.JobManager.HTTPPort)

		args := []string{
			"--role=tm",
			"--id=$(POD_NAME)",
			"--jm-host=" + r.jmName(cc),
			fmt.Sprintf("--jm-port=%d", cc.Spec.JobManager.ControlPort),
			"--data-host=$(POD_IP)",
			fmt.Sprintf("--slots=%d", cc.Spec.TaskManager.Slots),
			fmt.Sprintf("--http-port=%d", cc.Spec.TaskManager.HTTPPort),
			"--http-bind=0.0.0.0",
		}
		if cc.Spec.HA.Enabled {
			args = append(args, "--ha-dir="+cc.Spec.HA.MountPath)
		}
		args = append(args, cc.Spec.TaskManager.ExtraArgs...)

		grace := cc.Spec.TaskManager.TerminationGracePeriodSeconds
		tm := corev1.Container{
			Name:            "taskmanager",
			Image:           r.image(cc),
			ImagePullPolicy: cc.Spec.Image.PullPolicy,
			Env: []corev1.EnvVar{
				{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
				{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
				{Name: "CLINK_DATA_BIND_HOST", Value: "0.0.0.0"},
			},
			Args:      args,
			Ports:     []corev1.ContainerPort{{Name: "http", ContainerPort: cc.Spec.TaskManager.HTTPPort}},
			Resources: cc.Spec.TaskManager.Resources,
		}
		var volumeMounts []corev1.VolumeMount
		var volumes []corev1.Volume
		if cc.Spec.HA.Enabled {
			volumeMounts = []corev1.VolumeMount{{Name: "ha", MountPath: cc.Spec.HA.MountPath}}
			volumes = []corev1.Volume{{Name: "ha", VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: r.haPVCName(cc)},
			}}}
			tm.VolumeMounts = volumeMounts
		}
		ss.Spec.Template = corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: componentLabels(cc, "taskmanager")},
			Spec: corev1.PodSpec{
				ServiceAccountName:            r.serviceAccountName(cc),
				ImagePullSecrets:              cc.Spec.ImagePullSecrets,
				TerminationGracePeriodSeconds: &grace,
				InitContainers: []corev1.Container{{
					Name:            "wait-for-jobmanager",
					Image:           r.image(cc),
					ImagePullPolicy: cc.Spec.Image.PullPolicy,
					Command:         []string{"sh", "-c", waitCmd},
				}},
				Containers: []corev1.Container{tm},
				Volumes:    volumes,
			},
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

// ---- JobManager HTTP (status + job submission) -----------------------------

func (r *ClinkClusterReconciler) jmBaseURL(cc *clinkv1alpha1.ClinkCluster) string {
	return fmt.Sprintf("http://%s.%s.svc:%d", r.jmName(cc), cc.Namespace, cc.Spec.JobManager.HTTPPort)
}

// registeredTaskManagers reads the JM's /api/v1/tms and counts entries. Returns
// 0 if the JM is not yet reachable (cluster still coming up).
func (r *ClinkClusterReconciler) registeredTaskManagers(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) int32 {
	cli := &http.Client{Timeout: 3 * time.Second}
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodGet, r.jmBaseURL(cc)+"/api/v1/tms", nil)
	if err != nil {
		return 0
	}
	resp, err := cli.Do(httpReq)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0
	}
	return int32(strings.Count(string(body), "\"tm_id\""))
}

// SetupWithManager wires the reconciler to watch ClinkCluster + its owned kinds.
func (r *ClinkClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Scheme = mgr.GetScheme()
	return ctrl.NewControllerManagedBy(mgr).
		For(&clinkv1alpha1.ClinkCluster{}).
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Complete(r)
}

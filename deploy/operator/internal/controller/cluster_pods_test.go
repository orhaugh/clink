package controller

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clinkv1alpha1 "github.com/clink/clink-operator/api/v1alpha1"
)

func cluster(mutate func(*clinkv1alpha1.ClinkClusterSpec)) *clinkv1alpha1.ClinkCluster {
	cc := &clinkv1alpha1.ClinkCluster{ObjectMeta: metav1.ObjectMeta{Name: "clink", Namespace: "streams"}}
	if mutate != nil {
		mutate(&cc.Spec)
	}
	applyDefaults(cc)
	return cc
}

func hasArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func argWithPrefix(args []string, prefix string) string {
	for _, a := range args {
		if strings.HasPrefix(a, prefix) {
			return a
		}
	}
	return ""
}

func envVar(env []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range env {
		if env[i].Name == name {
			return &env[i]
		}
	}
	return nil
}

func volume(vols []corev1.Volume, name string) *corev1.Volume {
	for i := range vols {
		if vols[i].Name == name {
			return &vols[i]
		}
	}
	return nil
}

func TestWithoutHAOneCoordinatorAdvertisesTheServiceName(t *testing.T) {
	r := &ClinkClusterReconciler{}
	c := r.coordinatorPodSpec(cluster(nil)).Containers[0]
	if got := argWithPrefix(c.Args, "--advertise-host="); got != "--advertise-host=clink-coordinator" {
		t.Errorf("advertise: %q", got)
	}
	if argWithPrefix(c.Args, "--ha-dir=") != "" {
		t.Error("an HA dir without HA")
	}
	if !hasArg(c.Args, "--http-bind=0.0.0.0") {
		t.Error("the API is not bound on the pod's address")
	}
	if c.ReadinessProbe.HTTPGet.Path != "/readyz" || c.LivenessProbe.HTTPGet.Path != "/livez" {
		t.Errorf("probes: %s, %s", c.ReadinessProbe.HTTPGet.Path, c.LivenessProbe.HTTPGet.Path)
	}
}

func TestUnderHAEachCoordinatorAdvertisesItsStableName(t *testing.T) {
	r := &ClinkClusterReconciler{}
	cc := cluster(func(s *clinkv1alpha1.ClinkClusterSpec) { s.HA.Enabled = true })
	c := r.coordinatorPodSpec(cc).Containers[0]
	want := "--advertise-host=$(POD_NAME).clink-coordinator-headless.$(POD_NAMESPACE).svc.cluster.local"
	if !hasArg(c.Args, want) {
		t.Errorf("advertise: %q", argWithPrefix(c.Args, "--advertise-host="))
	}
	if !hasArg(c.Args, "--ha-dir=/var/lib/clink/ha") {
		t.Error("no HA dir")
	}
	if envVar(c.Env, "POD_NAME") == nil || envVar(c.Env, "POD_NAMESPACE") == nil {
		t.Error("the advertised name's parts are not in the environment")
	}
}

func TestTheHADirectoryIsAClaimOrANodeDirectory(t *testing.T) {
	r := &ClinkClusterReconciler{}
	pvc := r.coordinatorPodSpec(cluster(func(s *clinkv1alpha1.ClinkClusterSpec) { s.HA.Enabled = true }))
	if v := volume(pvc.Volumes, "ha"); v == nil || v.PersistentVolumeClaim == nil || v.PersistentVolumeClaim.ClaimName != "clink-ha" {
		t.Errorf("pvc: %+v", v)
	}
	host := r.workerPodSpec(cluster(func(s *clinkv1alpha1.ClinkClusterSpec) {
		s.HA.Enabled = true
		s.HA.Type = "hostPath"
	}))
	if v := volume(host.Volumes, "ha"); v == nil || v.HostPath == nil || v.HostPath.Path != "/var/lib/clink-ha" {
		t.Errorf("hostPath: %+v", v)
	}
}

func TestTheTokenComesFromTheAuthSecret(t *testing.T) {
	r := &ClinkClusterReconciler{}
	for key, want := range map[string]string{"": "token", "api-token": "api-token"} {
		cc := cluster(func(s *clinkv1alpha1.ClinkClusterSpec) {
			s.Auth = clinkv1alpha1.AuthSpec{ExistingSecret: "clink-auth", SecretKey: key}
		})
		for _, spec := range []corev1.PodSpec{r.coordinatorPodSpec(cc), r.workerPodSpec(cc)} {
			e := envVar(spec.Containers[0].Env, "CLINK_AUTH_TOKEN")
			if e == nil || e.ValueFrom == nil || e.ValueFrom.SecretKeyRef == nil {
				t.Fatalf("%s: no token from a Secret", spec.Containers[0].Name)
			}
			if ref := e.ValueFrom.SecretKeyRef; ref.Name != "clink-auth" || ref.Key != want {
				t.Errorf("%s: %s/%s", spec.Containers[0].Name, ref.Name, ref.Key)
			}
		}
	}
	if envVar(r.coordinatorPodSpec(cluster(nil)).Containers[0].Env, "CLINK_AUTH_TOKEN") != nil {
		t.Error("a token without auth")
	}
}

func TestAWorkerWithoutACertificateOfItsOwnMountsOnlyTheCA(t *testing.T) {
	r := &ClinkClusterReconciler{}
	cc := cluster(func(s *clinkv1alpha1.ClinkClusterSpec) {
		s.TLS = clinkv1alpha1.TLSSpec{Enabled: true, CoordinatorSecret: "coord-tls"}
	})
	coord := r.coordinatorPodSpec(cc)
	if v := volume(coord.Volumes, "tls"); v == nil || v.Secret.SecretName != "coord-tls" || len(v.Secret.Items) != 0 {
		t.Errorf("coordinator volume: %+v", v)
	}
	if args := coord.Containers[0].Args; !hasArg(args, "--tls-cert="+tlsDir+"/tls.crt") || !hasArg(args, "--tls-key="+tlsDir+"/tls.key") ||
		argWithPrefix(args, "--tls-client-ca=") != "" {
		t.Errorf("coordinator args: %v", args)
	}
	worker := r.workerPodSpec(cc)
	v := volume(worker.Volumes, "tls")
	if v == nil || v.Secret.SecretName != "coord-tls" || len(v.Secret.Items) != 1 || v.Secret.Items[0].Key != "ca.crt" {
		t.Fatalf("worker volume: %+v", v)
	}
	if args := worker.Containers[0].Args; !hasArg(args, "--tls-ca="+tlsDir+"/ca.crt") || argWithPrefix(args, "--tls-client-cert=") != "" {
		t.Errorf("worker args: %v", args)
	}
}

func TestMutualTLSGivesEachWorkerItsCertificate(t *testing.T) {
	r := &ClinkClusterReconciler{}
	cc := cluster(func(s *clinkv1alpha1.ClinkClusterSpec) {
		s.TLS = clinkv1alpha1.TLSSpec{Enabled: true, CoordinatorSecret: "coord-tls", WorkerSecret: "worker-tls", SkipHostnameVerification: true}
	})
	if args := r.coordinatorPodSpec(cc).Containers[0].Args; !hasArg(args, "--tls-client-ca="+tlsDir+"/ca.crt") {
		t.Errorf("coordinator does not ask for client certificates: %v", args)
	}
	worker := r.workerPodSpec(cc)
	if v := volume(worker.Volumes, "tls"); v == nil || v.Secret.SecretName != "worker-tls" || len(v.Secret.Items) != 0 {
		t.Errorf("worker volume: %+v", v)
	}
	args := worker.Containers[0].Args
	for _, want := range []string{"--tls-client-cert=" + tlsDir + "/tls.crt", "--tls-client-key=" + tlsDir + "/tls.key", "--tls-no-verify-hostname"} {
		if !hasArg(args, want) {
			t.Errorf("worker args lack %s: %v", want, args)
		}
	}
}

func TestARolesExtrasAreAddedToItsPod(t *testing.T) {
	r := &ClinkClusterReconciler{}
	cc := cluster(func(s *clinkv1alpha1.ClinkClusterSpec) {
		s.Worker.EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "azure"}}}}
		s.Worker.ExtraEnv = []corev1.EnvVar{{Name: "AWS_REGION", Value: "eu-west-1"}}
		s.Worker.ExtraVolumes = []corev1.Volume{{Name: "gcs-key", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "gcs"}}}}
		s.Worker.ExtraVolumeMounts = []corev1.VolumeMount{{Name: "gcs-key", MountPath: "/var/run/gcs"}}
	})
	w := r.workerPodSpec(cc)
	c := w.Containers[0]
	if len(c.EnvFrom) != 1 || c.EnvFrom[0].SecretRef.Name != "azure" {
		t.Errorf("envFrom: %+v", c.EnvFrom)
	}
	if e := envVar(c.Env, "AWS_REGION"); e == nil || e.Value != "eu-west-1" {
		t.Errorf("env: %+v", c.Env)
	}
	if volume(w.Volumes, "gcs-key") == nil || len(c.VolumeMounts) == 0 || c.VolumeMounts[len(c.VolumeMounts)-1].MountPath != "/var/run/gcs" {
		t.Errorf("volumes: %+v / %+v", w.Volumes, c.VolumeMounts)
	}
	// The Coordinator gets none of the Workers' extras.
	coord := r.coordinatorPodSpec(cc).Containers[0]
	if len(coord.EnvFrom) != 0 || envVar(coord.Env, "AWS_REGION") != nil {
		t.Errorf("the Coordinator took the Workers' extras: %+v %+v", coord.EnvFrom, coord.Env)
	}
}

func TestAWorkerWaitsForAReadyCoordinator(t *testing.T) {
	r := &ClinkClusterReconciler{}
	w := r.workerPodSpec(cluster(nil))
	if len(w.InitContainers) != 1 || !strings.Contains(strings.Join(w.InitContainers[0].Command, " "), "http://clink-coordinator:8081/readyz") {
		t.Fatalf("init: %+v", w.InitContainers)
	}
}

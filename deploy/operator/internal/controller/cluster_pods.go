package controller

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	clinkv1alpha1 "github.com/clink/clink-operator/api/v1alpha1"
)

// What a ClinkCluster's pods are, built from the spec alone: the reconcilers
// apply these, and the tests read them without a cluster.

// tlsDir is where each role mounts its control-plane certificates.
const tlsDir = "/etc/clink/tls"

func (r *ClinkClusterReconciler) jmHeadlessName(cc *clinkv1alpha1.ClinkCluster) string {
	return cc.Name + "-coordinator-headless"
}

func fieldEnv(name, path string) corev1.EnvVar {
	return corev1.EnvVar{Name: name, ValueFrom: &corev1.EnvVarSource{
		FieldRef: &corev1.ObjectFieldSelector{FieldPath: path},
	}}
}

// authEnv gives a clink container the cluster's HTTP API token from the auth
// Secret, as CLINK_AUTH_TOKEN, which a node requires and a client presents.
func authEnv(cc *clinkv1alpha1.ClinkCluster) []corev1.EnvVar {
	a := cc.Spec.Auth
	if a.ExistingSecret == "" {
		return nil
	}
	return []corev1.EnvVar{{Name: "CLINK_AUTH_TOKEN", ValueFrom: &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: a.ExistingSecret},
			Key:                  authSecretKey(cc),
		},
	}}}
}

func authSecretKey(cc *clinkv1alpha1.ClinkCluster) string {
	if cc.Spec.Auth.SecretKey == "" {
		return "token"
	}
	return cc.Spec.Auth.SecretKey
}

// tlsVolume is the certificates a role mounts at tlsDir. The Coordinators
// mount theirs whole; the Workers mount their client certificate for mutual
// TLS, or else only the Coordinator Secret's ca.crt, never its key.
func tlsVolume(cc *clinkv1alpha1.ClinkCluster, role string) (corev1.Volume, corev1.VolumeMount, bool) {
	t := cc.Spec.TLS
	if !t.Enabled {
		return corev1.Volume{}, corev1.VolumeMount{}, false
	}
	src := &corev1.SecretVolumeSource{SecretName: t.CoordinatorSecret}
	if role == "worker" {
		if t.WorkerSecret != "" {
			src = &corev1.SecretVolumeSource{SecretName: t.WorkerSecret}
		} else {
			src.Items = []corev1.KeyToPath{{Key: "ca.crt", Path: "ca.crt"}}
		}
	}
	return corev1.Volume{Name: "tls", VolumeSource: corev1.VolumeSource{Secret: src}},
		corev1.VolumeMount{Name: "tls", MountPath: tlsDir, ReadOnly: true}, true
}

func coordinatorTLSArgs(cc *clinkv1alpha1.ClinkCluster) []string {
	t := cc.Spec.TLS
	if !t.Enabled {
		return nil
	}
	args := []string{"--tls-cert=" + tlsDir + "/tls.crt", "--tls-key=" + tlsDir + "/tls.key"}
	if t.WorkerSecret != "" {
		args = append(args, "--tls-client-ca="+tlsDir+"/ca.crt")
	}
	return args
}

func workerTLSArgs(cc *clinkv1alpha1.ClinkCluster) []string {
	t := cc.Spec.TLS
	if !t.Enabled {
		return nil
	}
	args := []string{"--tls-ca=" + tlsDir + "/ca.crt"}
	if t.WorkerSecret != "" {
		args = append(args, "--tls-client-cert="+tlsDir+"/tls.crt", "--tls-client-key="+tlsDir+"/tls.key")
	}
	if t.SkipHostnameVerification {
		args = append(args, "--tls-no-verify-hostname")
	}
	return args
}

// sharedVolumes are the HA directory and the checkpoint volume every pod of
// the cluster mounts at the same paths.
func (r *ClinkClusterReconciler) sharedVolumes(cc *clinkv1alpha1.ClinkCluster) ([]corev1.Volume, []corev1.VolumeMount) {
	var volumes []corev1.Volume
	var mounts []corev1.VolumeMount
	if cc.Spec.HA.Enabled {
		src := corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: r.haPVCName(cc)},
		}
		if cc.Spec.HA.Type == "hostPath" {
			hpType := corev1.HostPathDirectoryOrCreate
			src = corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: cc.Spec.HA.HostPath, Type: &hpType}}
		}
		mounts = append(mounts, corev1.VolumeMount{Name: "ha", MountPath: cc.Spec.HA.MountPath})
		volumes = append(volumes, corev1.Volume{Name: "ha", VolumeSource: src})
	}
	if v, m, ok := r.checkpointVolume(cc); ok {
		mounts = append(mounts, m)
		volumes = append(volumes, v)
	}
	return volumes, mounts
}

// coordinatorPodSpec is the Coordinators' pod. With HA each advertises its
// stable name under the headless Service, which a TLS certificate can carry,
// and takes leadership through the shared HA directory; without HA the one
// Coordinator advertises the Service name.
func (r *ClinkClusterReconciler) coordinatorPodSpec(cc *clinkv1alpha1.ClinkCluster) corev1.PodSpec {
	args := []string{
		"--role=coordinator",
		fmt.Sprintf("--port=%d", cc.Spec.Coordinator.ControlPort),
		"--bind-host=0.0.0.0",
	}
	var env []corev1.EnvVar
	if cc.Spec.HA.Enabled {
		env = append(env, fieldEnv("POD_NAME", "metadata.name"), fieldEnv("POD_NAMESPACE", "metadata.namespace"))
		args = append(args,
			"--advertise-host=$(POD_NAME)."+r.jmHeadlessName(cc)+".$(POD_NAMESPACE).svc.cluster.local",
			"--ha-dir="+cc.Spec.HA.MountPath)
	} else {
		args = append(args, "--advertise-host="+r.jmName(cc))
	}
	// Every pod's address, not loopback: a standby forwards the API to the leader there.
	args = append(args, fmt.Sprintf("--http-port=%d", cc.Spec.Coordinator.HTTPPort), "--http-bind=0.0.0.0")
	if cc.Spec.Coordinator.StateBackend != "" {
		args = append(args, "--state-backend="+cc.Spec.Coordinator.StateBackend)
	}
	args = append(args, coordinatorTLSArgs(cc)...)
	args = append(args, cc.Spec.Coordinator.ExtraArgs...)
	env = append(env, authEnv(cc)...)
	env = append(env, parseEnvKVs(cc.Spec.Coordinator.Env)...)
	env = append(env, cc.Spec.Coordinator.ExtraEnv...)

	volumes, mounts := r.sharedVolumes(cc)
	if v, m, ok := tlsVolume(cc, "coordinator"); ok {
		volumes, mounts = append(volumes, v), append(mounts, m)
	}
	volumes = append(volumes, cc.Spec.Coordinator.ExtraVolumes...)
	mounts = append(mounts, cc.Spec.Coordinator.ExtraVolumeMounts...)

	return corev1.PodSpec{
		ServiceAccountName: r.serviceAccountName(cc),
		ImagePullSecrets:   cc.Spec.ImagePullSecrets,
		Containers: []corev1.Container{{
			Name:            "coordinator",
			Image:           r.image(cc),
			ImagePullPolicy: cc.Spec.Image.PullPolicy,
			Args:            args,
			Env:             env,
			EnvFrom:         cc.Spec.Coordinator.EnvFrom,
			Ports: []corev1.ContainerPort{
				{Name: "control", ContainerPort: cc.Spec.Coordinator.ControlPort},
				{Name: "http", ContainerPort: cc.Spec.Coordinator.HTTPPort},
			},
			// Both answer without the token. /readyz admits a Coordinator once
			// it leads with its control port bound, or as a standby that knows
			// a leader to forward the API to.
			ReadinessProbe: httpProbe("/readyz", "http", 2, 5, 6),
			LivenessProbe:  httpProbe("/livez", "http", 10, 10, 6),
			Resources:      cc.Spec.Coordinator.Resources,
			VolumeMounts:   mounts,
		}},
		Volumes: volumes,
	}
}

// workerPodSpec is the Workers' pod. A Worker waits for a ready Coordinator
// before it starts, finds the leader through the HA directory under HA, and
// reaches it over TLS when the cluster asks for it.
func (r *ClinkClusterReconciler) workerPodSpec(cc *clinkv1alpha1.ClinkCluster) corev1.PodSpec {
	waitCmd := fmt.Sprintf(
		"until curl -fsS \"http://%s:%d/readyz\" >/dev/null 2>&1; do echo waiting for coordinator...; sleep 2; done; echo coordinator is ready",
		r.jmName(cc), cc.Spec.Coordinator.HTTPPort)
	args := []string{
		"--role=worker",
		"--id=$(POD_NAME)",
		"--coordinator-host=" + r.jmName(cc),
		fmt.Sprintf("--coordinator-port=%d", cc.Spec.Coordinator.ControlPort),
		"--data-host=$(POD_IP)",
		fmt.Sprintf("--slots=%d", cc.Spec.Worker.Slots),
		fmt.Sprintf("--http-port=%d", cc.Spec.Worker.HTTPPort),
		"--http-bind=0.0.0.0",
	}
	if cc.Spec.HA.Enabled {
		args = append(args, "--ha-dir="+cc.Spec.HA.MountPath)
	}
	args = append(args, workerTLSArgs(cc)...)
	args = append(args, cc.Spec.Worker.ExtraArgs...)
	env := []corev1.EnvVar{
		fieldEnv("POD_NAME", "metadata.name"),
		fieldEnv("POD_IP", "status.podIP"),
		{Name: "CLINK_DATA_BIND_HOST", Value: "0.0.0.0"},
	}
	env = append(env, authEnv(cc)...)
	env = append(env, parseEnvKVs(cc.Spec.Worker.Env)...)
	env = append(env, cc.Spec.Worker.ExtraEnv...)

	volumes, mounts := r.sharedVolumes(cc)
	if v, m, ok := tlsVolume(cc, "worker"); ok {
		volumes, mounts = append(volumes, v), append(mounts, m)
	}
	volumes = append(volumes, cc.Spec.Worker.ExtraVolumes...)
	mounts = append(mounts, cc.Spec.Worker.ExtraVolumeMounts...)

	grace := cc.Spec.Worker.TerminationGracePeriodSeconds
	return corev1.PodSpec{
		ServiceAccountName:            r.serviceAccountName(cc),
		ImagePullSecrets:              cc.Spec.ImagePullSecrets,
		TerminationGracePeriodSeconds: &grace,
		InitContainers: []corev1.Container{{
			Name:            "wait-for-coordinator",
			Image:           r.image(cc),
			ImagePullPolicy: cc.Spec.Image.PullPolicy,
			Command:         []string{"sh", "-c", waitCmd},
		}},
		Containers: []corev1.Container{{
			Name:            "worker",
			Image:           r.image(cc),
			ImagePullPolicy: cc.Spec.Image.PullPolicy,
			Env:             env,
			EnvFrom:         cc.Spec.Worker.EnvFrom,
			Args:            args,
			Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: cc.Spec.Worker.HTTPPort}},
			ReadinessProbe:  httpProbe("/readyz", "http", 2, 5, 6),
			LivenessProbe:   httpProbe("/livez", "http", 10, 10, 6),
			Resources:       cc.Spec.Worker.Resources,
			VolumeMounts:    mounts,
		}},
		Volumes: volumes,
	}
}

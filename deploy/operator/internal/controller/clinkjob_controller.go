package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	kscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	clinkv1alpha1 "github.com/clink/clink-operator/api/v1alpha1"
)

const clinkJobFinalizer = "clink.dev/clinkjob-finalizer"

// ClinkJobReconciler runs a compiled-.so job on a ClinkCluster and drains it to a
// savepoint + restores on upgrade. It orchestrates the clink control plane by
// exec'ing the in-image `clink` CLI inside a Coordinator pod (submit / savepoint /
// cancel all speak the control-plane binary protocol on 127.0.0.1) and reads job
// status over the Coordinator's HTTP API.
type ClinkJobReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	Clientset  kubernetes.Interface
	RestConfig *rest.Config
}

// +kubebuilder:rbac:groups=clink.dev,resources=clinkjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=clink.dev,resources=clinkjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=clink.dev,resources=clinkjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create

func (r *ClinkJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var cj clinkv1alpha1.ClinkJob
	if err := r.Get(ctx, req.NamespacedName, &cj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	applyJobDefaults(&cj)

	// Resolve the target cluster (needed for both delete-cancel and reconcile).
	var cc clinkv1alpha1.ClinkCluster
	clusterErr := r.Get(ctx, types.NamespacedName{Name: cj.Spec.ClusterName, Namespace: cj.Namespace}, &cc)
	if clusterErr == nil {
		// The stored ClinkCluster spec may omit nested objects (e.g. coordinator),
		// whose field defaults CRD defaulting does not materialize; the cluster
		// controller only defaults in-memory during its own reconcile. Apply the
		// same defaults here so ports (control 6123 / http 8081) are correct.
		applyDefaults(&cc)
	}

	// Deletion: cancel the running job (best-effort) then drop the finalizer.
	if !cj.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&cj, clinkJobFinalizer) {
			if clusterErr == nil && cj.Status.JobID != 0 {
				if pod, err := r.readyCoordinatorPod(ctx, &cc); err == nil && pod != "" {
					_, _, _ = r.execClink(ctx, cc.Namespace, pod, r.cancelArgs(&cc, cj.Status.JobID))
				}
			}
			controllerutil.RemoveFinalizer(&cj, clinkJobFinalizer)
			if err := r.Update(ctx, &cj); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&cj, clinkJobFinalizer) {
		controllerutil.AddFinalizer(&cj, clinkJobFinalizer)
		if err := r.Update(ctx, &cj); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Gate on a Running cluster with a ready coordinator pod.
	if clusterErr != nil || cc.Status.Phase != "Running" {
		return r.setPhase(ctx, &cj, "Pending", "waiting for ClinkCluster to be Running", 5*time.Second)
	}
	pod, err := r.readyCoordinatorPod(ctx, &cc)
	if err != nil || pod == "" {
		return r.setPhase(ctx, &cj, "Pending", "waiting for a ready Coordinator pod", 5*time.Second)
	}

	wantHash := specHash(&cj)

	// Scale-to-zero: an in-flight or requested park/wake takes precedence
	// over submit/upgrade/refresh. Handled first so a Resuming job can never
	// fall through to a fresh (state-dropping) initial submit.
	if done, res, err := r.reconcileSuspend(ctx, &cj, &cc, pod, wantHash); done {
		return res, err
	}

	// Decide: initial submit, upgrade, or steady-state status refresh.
	switch {
	case cj.Status.JobID == 0:
		return r.submitInitial(ctx, &cj, &cc, pod, wantHash)
	case cj.Status.SpecHash != wantHash:
		return r.upgrade(ctx, &cj, &cc, pod, wantHash)
	default:
		return r.refreshStatus(ctx, &cj, &cc, pod)
	}
}

// ---- scale-to-zero (park / wake) --------------------------------------------

const (
	suspendReasonManual = "Manual"
	suspendReasonIdle   = "Idle"
)

// reconcileSuspend owns every park/wake transition. Returns done=false only
// when the job is active with no suspend intent, letting the normal
// submit/upgrade/refresh flow proceed.
func (r *ClinkJobReconciler) reconcileSuspend(ctx context.Context, cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster, pod, wantHash string) (bool, ctrl.Result, error) {
	parking := cj.Status.Phase == "Suspending"
	// Resuming counts as parked: its restore point is still staged, and only
	// the resume path may submit (an initial submit would drop the state).
	parked := cj.Status.Phase == "Suspended" || cj.Status.Phase == "Resuming"

	// Finish an in-flight park (stage 2: cancel) before honouring any new
	// intent - the savepoint is already persisted.
	if parking {
		res, err := r.parkCancelStage(ctx, cj, cc, pod)
		return true, res, err
	}

	if cj.Spec.Suspend {
		switch {
		case parked:
			// A resume submit may have landed just before the re-suspend;
			// adopt it so the park path can drain it rather than leak it.
			if id := r.runningJobID(ctx, cc); id > 0 && id > cj.Status.JobID {
				cj.Status.JobID = id
				res, err := r.setPhase(ctx, cj, "Running", "adopted resumed job before re-suspend", time.Second)
				return true, res, err
			}
			if cj.Status.SuspendReason != suspendReasonManual {
				cj.Status.SuspendReason = suspendReasonManual // manual now owns the park
			}
			res, err := r.setPhase(ctx, cj, "Suspended", "suspended (manual)", 60*time.Second)
			return true, res, err
		case cj.Status.JobID != 0:
			res, err := r.park(ctx, cj, cc, pod, suspendReasonManual)
			return true, res, err
		default:
			// Suspended before ever submitting: nothing to drain.
			cj.Status.SuspendReason = suspendReasonManual
			res, err := r.setPhase(ctx, cj, "Suspended", "suspended before first submit", 60*time.Second)
			return true, res, err
		}
	}

	if parked {
		// Wake rules: an interrupted resume always finishes; a manual park
		// wakes when spec.suspend clears (this branch); an Idle park wakes
		// when its auto-park policy is removed or the wake policy fires.
		if cj.Status.Phase == "Resuming" || cj.Status.SuspendReason == suspendReasonManual ||
			cj.Spec.SuspendPolicy == nil || cj.Spec.SuspendPolicy.IdleAfterSeconds <= 0 {
			res, err := r.resume(ctx, cj, cc, pod, wantHash)
			return true, res, err
		}
		wake, detail, poll := r.wakeDue(ctx, cj)
		if wake {
			res, err := r.resume(ctx, cj, cc, pod, wantHash)
			return true, res, err
		}
		res, err := r.setPhase(ctx, cj, "Suspended", "parked (idle); "+detail, poll)
		return true, res, err
	}

	return false, ctrl.Result{}, nil
}

// park drains the running job to a savepoint (upgradeMode=savepoint) and
// persists it BEFORE cancelling, so a crash between the two never loses the
// restore point. The cancel is stage 2 (parkCancelStage) on the next
// reconcile, keyed off the Suspending phase.
func (r *ClinkJobReconciler) park(ctx context.Context, cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster, pod, reason string) (ctrl.Result, error) {
	lg := log.FromContext(ctx)
	// A park landing mid-upgrade (old job already drained + cancelled, new
	// one not yet up) adopts the upgrade's savepoint instead of trying to
	// savepoint the already-cancelled job.
	if cj.Status.UpgradeToHash != "" {
		cj.Status.SuspendSavepoint = cj.Status.UpgradeRestore
		cj.Status.UpgradeToHash = ""
		cj.Status.UpgradeRestore = nil
		cj.Status.SuspendReason = reason
		cj.Status.JobID = 0
		now := metav1.Now()
		cj.Status.SuspendedAt = &now
		return r.setPhase(ctx, cj, "Suspended", "parked mid-upgrade ("+reason+")", 30*time.Second)
	}
	if cj.Spec.UpgradeMode == "savepoint" && cj.Status.SuspendSavepoint == nil {
		sp, err := r.triggerSavepoint(ctx, cc, pod, cj.Status.JobID)
		if err != nil {
			lg.Info("suspend savepoint failed; will retry", "err", err.Error())
			return r.setPhase(ctx, cj, cj.Status.Phase, "suspend savepoint failed: "+err.Error(), 10*time.Second)
		}
		cj.Status.SuspendSavepoint = sp
		cj.Status.LastSavepoint = sp
	}
	cj.Status.SuspendReason = reason
	return r.setPhase(ctx, cj, "Suspending", "drained to savepoint; cancelling", time.Second)
}

// parkCancelStage completes a park: cancel the job (tolerating failure - the
// job may already be gone), free its slots, and mark Suspended.
func (r *ClinkJobReconciler) parkCancelStage(ctx context.Context, cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster, pod string) (ctrl.Result, error) {
	lg := log.FromContext(ctx)
	if cj.Status.JobID != 0 {
		if _, stderr, err := r.execClink(ctx, cc.Namespace, pod, r.cancelArgs(cc, cj.Status.JobID)); err != nil {
			lg.Info("cancel during park failed; continuing", "err", err.Error(), "stderr", stderr)
		}
		cj.Status.JobID = 0
	}
	now := metav1.Now()
	cj.Status.SuspendedAt = &now
	reason := cj.Status.SuspendReason
	if reason == "" {
		reason = suspendReasonManual
	}
	msg := "parked (" + reason + "); slots freed"
	if sp := cj.Status.SuspendSavepoint; sp != nil {
		msg += fmt.Sprintf("; savepoint %s@%d", sp.Dir, sp.CheckpointID)
	}
	return r.setPhase(ctx, cj, "Suspended", msg, 15*time.Second)
}

// resume resubmits the job restored from the park savepoint. Retried until a
// new job id appears; adopts a submit that raced status recording.
func (r *ClinkJobReconciler) resume(ctx context.Context, cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster, pod, wantHash string) (ctrl.Result, error) {
	if id := r.runningJobID(ctx, cc); id > 0 && id > cj.Status.JobID {
		msg := fmt.Sprintf("resumed as job %d (adopted)", id)
		r.clearSuspend(cj, wantHash, id)
		return r.setPhase(ctx, cj, "Running", msg, 20*time.Second)
	}
	restore := cj.Status.SuspendSavepoint
	before := r.maxJobID(ctx, cc)
	if ok, detail := r.doSubmit(ctx, cc, cj, pod, restore); !ok {
		return r.setPhase(ctx, cj, "Resuming", "resume submit rejected: "+detail, 10*time.Second)
	}
	id := r.awaitNewJob(ctx, cc, before)
	if id == 0 {
		return r.setPhase(ctx, cj, "Resuming", "resubmitted; awaiting job id", 5*time.Second)
	}
	msg := fmt.Sprintf("resumed as job %d", id)
	if restore != nil {
		msg += fmt.Sprintf(" restored from %s@%d", restore.Dir, restore.CheckpointID)
	}
	r.clearSuspend(cj, wantHash, id)
	return r.setPhase(ctx, cj, "Running", msg, 20*time.Second)
}

// clearSuspend resets park bookkeeping once the resumed job is Running. The
// activity clock restarts so an idle policy grants the fresh job a full
// window before re-parking.
func (r *ClinkJobReconciler) clearSuspend(cj *clinkv1alpha1.ClinkJob, wantHash string, id int64) {
	cj.Status.JobID = id
	cj.Status.SpecHash = wantHash
	cj.Status.SuspendSavepoint = nil
	cj.Status.SuspendReason = ""
	cj.Status.SuspendedAt = nil
	now := metav1.Now()
	cj.Status.ActivityCount = 0
	cj.Status.LastActivityTime = &now
}

// wakeDue evaluates the wake policy for an Idle-parked job. Returns whether
// to wake, a human-readable detail for status, and the poll cadence.
func (r *ClinkJobReconciler) wakeDue(ctx context.Context, cj *clinkv1alpha1.ClinkJob) (bool, string, time.Duration) {
	wp := cj.Spec.WakePolicy
	if wp == nil || wp.KafkaLag == nil {
		return false, "no wake policy; awaiting manual resume or policy removal", 60 * time.Second
	}
	k := wp.KafkaLag
	threshold := k.LagThreshold
	if threshold <= 0 {
		threshold = 1
	}
	poll := time.Duration(k.PollIntervalSeconds) * time.Second
	if poll < 5*time.Second {
		poll = 15 * time.Second
	}
	lag, err := kafkaGroupLag(ctx, k)
	if err != nil {
		return false, "lag poll failed: " + firstLine(err.Error()), poll
	}
	if lag >= threshold {
		return true, fmt.Sprintf("lag %d >= threshold %d", lag, threshold), poll
	}
	return false, fmt.Sprintf("lag %d < threshold %d", lag, threshold), poll
}

// ---- state transitions -----------------------------------------------------

func (r *ClinkJobReconciler) submitInitial(ctx context.Context, cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster, pod, wantHash string) (ctrl.Result, error) {
	// Retry-safe: if a live job already exists (a prior submit that raced status
	// recording, or an operator restart), adopt it instead of submitting again.
	if id := r.runningJobID(ctx, cc); id > 0 {
		cj.Status.JobID = id
		cj.Status.SpecHash = wantHash
		return r.setPhase(ctx, cj, "Running", fmt.Sprintf("adopted running job %d", id), 20*time.Second)
	}
	before := r.maxJobID(ctx, cc)
	if ok, detail := r.doSubmit(ctx, cc, cj, pod, nil); !ok {
		return r.setPhase(ctx, cj, "Submitting", "submit rejected: "+detail, 10*time.Second)
	}
	id := r.awaitNewJob(ctx, cc, before)
	if id == 0 {
		return r.setPhase(ctx, cj, "Submitting", "submitted; awaiting job id", 5*time.Second)
	}
	cj.Status.JobID = id
	cj.Status.SpecHash = wantHash
	return r.setPhase(ctx, cj, "Running", fmt.Sprintf("submitted job %d", id), 20*time.Second)
}

// upgrade rolls a spec change out in two persisted stages so it is safe across
// reconciles: (1) drain the old job to a savepoint and cancel it - done exactly
// once, guarded by Status.UpgradeToHash; (2) (re)submit the new job restored
// from that savepoint, retried until a new job id appears. Stage 1 must not
// repeat on a slow/failed stage 2, or it would re-savepoint an already-cancelled
// job (which fails).
func (r *ClinkJobReconciler) upgrade(ctx context.Context, cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster, pod, wantHash string) (ctrl.Result, error) {
	lg := log.FromContext(ctx)

	// Stage 1: savepoint + cancel the old job, once per target hash.
	if cj.Status.UpgradeToHash != wantHash {
		var restore *clinkv1alpha1.SavepointRef
		if cj.Spec.UpgradeMode == "savepoint" {
			sp, err := r.triggerSavepoint(ctx, cc, pod, cj.Status.JobID)
			if err != nil {
				lg.Info("savepoint failed; will retry", "err", err.Error())
				return r.setPhase(ctx, cj, "Upgrading", "savepoint failed: "+err.Error(), 10*time.Second)
			}
			restore = sp
			cj.Status.LastSavepoint = sp
		}
		if _, stderr, err := r.execClink(ctx, cc.Namespace, pod, r.cancelArgs(cc, cj.Status.JobID)); err != nil {
			lg.Info("cancel during upgrade failed; continuing", "err", err.Error(), "stderr", stderr)
		}
		// Commit stage 1: from here the old job is gone; never savepoint it again.
		cj.Status.UpgradeToHash = wantHash
		cj.Status.UpgradeRestore = restore
		return r.setPhase(ctx, cj, "Upgrading", "drained to savepoint; resubmitting", 2*time.Second)
	}

	// Stage 2: (re)submit the new job, restored from the stage-1 savepoint.
	// Adopt an already-submitted new job on retry so a slow awaitNewJob does
	// not leak a second job. The old (cancelled) job is not "live" so it is
	// skipped; only a fresh job with a higher id counts.
	if id := r.runningJobID(ctx, cc); id > cj.Status.JobID {
		cj.Status.JobID = id
		cj.Status.SpecHash = wantHash
		cj.Status.UpgradeToHash = ""
		cj.Status.UpgradeRestore = nil
		return r.setPhase(ctx, cj, "Running", fmt.Sprintf("upgraded to job %d (adopted)", id), 20*time.Second)
	}
	restore := cj.Status.UpgradeRestore
	before := r.maxJobID(ctx, cc)
	if ok, detail := r.doSubmit(ctx, cc, cj, pod, restore); !ok {
		return r.setPhase(ctx, cj, "Upgrading", "resubmit rejected: "+detail, 10*time.Second)
	}
	id := r.awaitNewJob(ctx, cc, before)
	if id == 0 {
		return r.setPhase(ctx, cj, "Upgrading", "resubmitted; awaiting job id", 5*time.Second)
	}
	cj.Status.JobID = id
	cj.Status.SpecHash = wantHash
	cj.Status.UpgradeToHash = ""
	cj.Status.UpgradeRestore = nil
	msg := fmt.Sprintf("upgraded to job %d", id)
	if restore != nil {
		msg += fmt.Sprintf(" restored from %s@%d", restore.Dir, restore.CheckpointID)
	}
	return r.setPhase(ctx, cj, "Running", msg, 20*time.Second)
}

func (r *ClinkJobReconciler) refreshStatus(ctx context.Context, cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster, pod string) (ctrl.Result, error) {
	js, found := r.jobSummary(ctx, cc, cj.Status.JobID)
	phase, msg := "Running", fmt.Sprintf("job %d running", cj.Status.JobID)
	switch {
	case !found:
		phase, msg = "Pending", "job not found on cluster; will resubmit"
		cj.Status.JobID = 0 // force resubmit next reconcile
	case js.ErrorCount > 0:
		phase, msg = "Failed", fmt.Sprintf("job %d has %d error(s)", cj.Status.JobID, js.ErrorCount)
	case js.CompletionSignalled:
		phase, msg = "Completed", fmt.Sprintf("job %d completed", cj.Status.JobID)
	case js.CancelRequested:
		phase, msg = "Cancelled", fmt.Sprintf("job %d cancelling", cj.Status.JobID)
	}

	// Idle auto-park: a healthy Running job with the policy armed parks once
	// no operator has processed a record for the window. Activity = the sum
	// of per-operator records_in; any change resets the clock.
	if found && phase == "Running" && cj.Spec.SuspendPolicy != nil && cj.Spec.SuspendPolicy.IdleAfterSeconds > 0 {
		idleAfter := time.Duration(cj.Spec.SuspendPolicy.IdleAfterSeconds) * time.Second
		if count, ok := r.jobActivity(ctx, cc, cj.Status.JobID); ok {
			now := metav1.Now()
			if count != cj.Status.ActivityCount || cj.Status.LastActivityTime == nil {
				cj.Status.ActivityCount = count
				cj.Status.LastActivityTime = &now
			} else if now.Time.Sub(cj.Status.LastActivityTime.Time) >= idleAfter {
				return r.park(ctx, cj, cc, pod, suspendReasonIdle)
			}
			msg += fmt.Sprintf("; idle %ds/%ds",
				int(now.Time.Sub(cj.Status.LastActivityTime.Time).Seconds()),
				int(idleAfter.Seconds()))
		}
		// Poll fast enough to catch the window promptly without hammering.
		requeue := idleAfter / 2
		if requeue > 20*time.Second {
			requeue = 20 * time.Second
		}
		if requeue < 5*time.Second {
			requeue = 5 * time.Second
		}
		return r.setPhase(ctx, cj, phase, msg, requeue)
	}
	return r.setPhase(ctx, cj, phase, msg, 20*time.Second)
}

// jobActivity sums per-operator records_in for a job from the Coordinator's
// per-operator stats endpoint. false when the endpoint is unreachable or the
// body does not parse (the idle clock then simply does not advance).
func (r *ClinkJobReconciler) jobActivity(ctx context.Context, cc *clinkv1alpha1.ClinkCluster, id int64) (int64, bool) {
	reqCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	url := fmt.Sprintf("%s/api/v1/jobs/%d/operators", r.jmBaseURL(cc), id)
	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return 0, false
	}
	resp, err := (&http.Client{Timeout: 4 * time.Second}).Do(httpReq)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	var wrapper struct {
		Operators []struct {
			RecordsIn int64 `json:"records_in"`
		} `json:"operators"`
	}
	if json.Unmarshal(body, &wrapper) != nil || wrapper.Operators == nil {
		return 0, false
	}
	var sum int64
	for _, o := range wrapper.Operators {
		sum += o.RecordsIn
	}
	return sum, true
}

func (r *ClinkJobReconciler) setPhase(ctx context.Context, cj *clinkv1alpha1.ClinkJob, phase, msg string, requeue time.Duration) (ctrl.Result, error) {
	cj.Status.Phase = phase
	cj.Status.Message = msg
	cj.Status.ObservedGeneration = cj.Generation
	if err := r.Status().Update(ctx, cj); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// ---- clink CLI arg builders ------------------------------------------------

func (r *ClinkJobReconciler) submitArgs(cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster, restore *clinkv1alpha1.SavepointRef) []string {
	run := []string{
		"clink", "run",
		"--job=" + cj.Spec.JobSo,
		"--name=" + cj.Spec.JobName,
		"--coordinator-host=127.0.0.1",
		fmt.Sprintf("--coordinator-port=%d", cc.Spec.Coordinator.ControlPort),
		"--wait-timeout-s=0", // fire-and-forget: return after the coordinator accepts
	}
	if cc.Spec.CheckpointStorage.Enabled && cj.Spec.CheckpointIntervalMs > 0 {
		run = append(run,
			"--checkpoint-dir="+cc.Spec.CheckpointStorage.MountPath+"/"+cj.Spec.JobName,
			fmt.Sprintf("--checkpoint-interval-ms=%d", cj.Spec.CheckpointIntervalMs))
	}
	if restore != nil && restore.Dir != "" {
		run = append(run,
			"--restore-from-dir="+restore.Dir,
			fmt.Sprintf("--restore-from-checkpoint-id=%d", restore.CheckpointID))
	}
	run = append(run, cj.Spec.Args...)
	// Prefix env exports (job build-time config, e.g. CLINK_*). Single-quote
	// values so spaces survive; job env is not expected to contain single quotes.
	var exports strings.Builder
	for _, kv := range cj.Spec.Env {
		if k, v, ok := strings.Cut(kv, "="); ok {
			exports.WriteString(fmt.Sprintf("export %s='%s'; ", k, v))
		}
	}
	return []string{"sh", "-c", exports.String() + strings.Join(run, " ")}
}

func (r *ClinkJobReconciler) savepointArgs(cc *clinkv1alpha1.ClinkCluster, jobID int64) []string {
	return []string{"clink", "savepoint",
		fmt.Sprintf("--job-id=%d", jobID),
		"--coordinator-host=127.0.0.1",
		fmt.Sprintf("--coordinator-port=%d", cc.Spec.Coordinator.ControlPort)}
}

func (r *ClinkJobReconciler) cancelArgs(cc *clinkv1alpha1.ClinkCluster, jobID int64) []string {
	return []string{"clink", "cancel",
		fmt.Sprintf("--job-id=%d", jobID),
		"--coordinator-host=127.0.0.1",
		fmt.Sprintf("--coordinator-port=%d", cc.Spec.Coordinator.ControlPort)}
}

var savepointRe = regexp.MustCompile(`dir="([^"]*)"\s+id=(\d+)`)

func (r *ClinkJobReconciler) triggerSavepoint(ctx context.Context, cc *clinkv1alpha1.ClinkCluster, pod string, jobID int64) (*clinkv1alpha1.SavepointRef, error) {
	stdout, stderr, err := r.execClink(ctx, cc.Namespace, pod, r.savepointArgs(cc, jobID))
	if err != nil {
		return nil, fmt.Errorf("savepoint exec: %v (%s)", err, firstLine(stderr))
	}
	// stdout: savepoint: job_id=N ok=1 dir="/path" id=42 message="..."
	if !strings.Contains(stdout, "ok=1") {
		return nil, fmt.Errorf("savepoint not ok: %s", firstLine(stdout))
	}
	m := savepointRe.FindStringSubmatch(stdout)
	if m == nil {
		return nil, fmt.Errorf("could not parse savepoint dir/id from: %s", firstLine(stdout))
	}
	var id int64
	fmt.Sscanf(m[2], "%d", &id)
	return &clinkv1alpha1.SavepointRef{Dir: m[1], CheckpointID: id}, nil
}

// ---- Coordinator HTTP (job status) ------------------------------------------

type jobSummaryJSON struct {
	ID                  int64 `json:"id"`
	CompletedCount      int64 `json:"completed_count"`
	CompletionSignalled bool  `json:"completion_signalled"`
	CancelRequested     bool  `json:"cancel_requested"`
	ErrorCount          int64 `json:"error_count"`
}

func (r *ClinkJobReconciler) jmBaseURL(cc *clinkv1alpha1.ClinkCluster) string {
	return fmt.Sprintf("http://%s-coordinator.%s.svc:%d", cc.Name, cc.Namespace, cc.Spec.Coordinator.HTTPPort)
}

func (r *ClinkJobReconciler) listJobs(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) []jobSummaryJSON {
	reqCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodGet, r.jmBaseURL(cc)+"/api/v1/jobs", nil)
	if err != nil {
		return nil
	}
	resp, err := (&http.Client{Timeout: 4 * time.Second}).Do(httpReq)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	// Response is {"jobs":[...]} or a bare array; try both.
	var wrapper struct {
		Jobs []jobSummaryJSON `json:"jobs"`
	}
	if json.Unmarshal(body, &wrapper) == nil && wrapper.Jobs != nil {
		return wrapper.Jobs
	}
	var arr []jobSummaryJSON
	_ = json.Unmarshal(body, &arr)
	return arr
}

func (r *ClinkJobReconciler) maxJobID(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) int64 {
	var mx int64
	for _, j := range r.listJobs(ctx, cc) {
		if j.ID > mx {
			mx = j.ID
		}
	}
	return mx
}

// awaitNewJob polls for a job id greater than `after` (the just-submitted job;
// ids are monotonic and this operator serialises its own submits).
func (r *ClinkJobReconciler) awaitNewJob(ctx context.Context, cc *clinkv1alpha1.ClinkCluster, after int64) int64 {
	for i := 0; i < 15; i++ {
		if id := r.maxJobID(ctx, cc); id > after {
			return id
		}
		select {
		case <-ctx.Done():
			return 0
		case <-time.After(1 * time.Second):
		}
	}
	return 0
}

func (r *ClinkJobReconciler) jobSummary(ctx context.Context, cc *clinkv1alpha1.ClinkCluster, id int64) (jobSummaryJSON, bool) {
	for _, j := range r.listJobs(ctx, cc) {
		if j.ID == id {
			return j, true
		}
	}
	return jobSummaryJSON{}, false
}

// runningJobID returns the newest live (not completed/cancelled/failed) job id,
// or 0. Used to adopt a job on a retry so a submit that raced status recording
// does not leak a second job (v1 assumes one ClinkJob per cluster).
func (r *ClinkJobReconciler) runningJobID(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) int64 {
	var best int64
	for _, j := range r.listJobs(ctx, cc) {
		if j.CompletionSignalled || j.CancelRequested || j.ErrorCount > 0 {
			continue
		}
		if j.ID > best {
			best = j.ID
		}
	}
	return best
}

// doSubmit runs `clink run` and reports acceptance. The submit is fire-and-forget
// (--wait-timeout-s=0), so clink run exits non-zero ("job did not complete") even
// on success; the authoritative signal is "ok=1" in its stdout, not the exit code.
func (r *ClinkJobReconciler) doSubmit(ctx context.Context, cc *clinkv1alpha1.ClinkCluster, cj *clinkv1alpha1.ClinkJob, pod string, restore *clinkv1alpha1.SavepointRef) (bool, string) {
	stdout, stderr, _ := r.execClink(ctx, cc.Namespace, pod, r.submitArgs(cj, cc, restore))
	if strings.Contains(stdout, "ok=1") {
		return true, ""
	}
	if d := firstLine(stdout); d != "" {
		return false, d
	}
	return false, firstLine(stderr)
}

// ---- pod discovery + exec --------------------------------------------------

// readyCoordinatorPod returns the name of a Ready Coordinator pod for the cluster. In HA,
// only the leader binds the control port; v1 targets a single-coordinator cluster (any
// ready coordinator pod is the leader). HA leader-targeting is a follow-on.
func (r *ClinkJobReconciler) readyCoordinatorPod(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) (string, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(cc.Namespace), client.MatchingLabels{
		"app.kubernetes.io/instance":  cc.Name,
		"app.kubernetes.io/component": "coordinator",
	}); err != nil {
		return "", err
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase != corev1.PodRunning {
			continue
		}
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				return p.Name, nil
			}
		}
	}
	return "", nil
}

// execClink runs a command in the coordinator container of pod and returns
// stdout/stderr.
func (r *ClinkJobReconciler) execClink(ctx context.Context, namespace, pod string, cmd []string) (string, string, error) {
	req := r.Clientset.CoreV1().RESTClient().Post().
		Resource("pods").Name(pod).Namespace(namespace).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "coordinator",
			Command:   cmd,
			Stdout:    true,
			Stderr:    true,
		}, kscheme.ParameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(r.RestConfig, "POST", req.URL())
	if err != nil {
		return "", "", err
	}
	var stdout, stderr bytes.Buffer
	execCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = exec.StreamWithContext(execCtx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
	return stdout.String(), stderr.String(), err
}

// ---- helpers ---------------------------------------------------------------

func applyJobDefaults(cj *clinkv1alpha1.ClinkJob) {
	if cj.Spec.JobName == "" {
		cj.Spec.JobName = "job"
	}
	if cj.Spec.UpgradeMode == "" {
		cj.Spec.UpgradeMode = "savepoint"
	}
}

func specHash(cj *clinkv1alpha1.ClinkJob) string {
	h := sha256.New()
	io.WriteString(h, cj.Spec.JobSo+"\n")
	io.WriteString(h, strings.Join(cj.Spec.Args, "\x00")+"\n")
	io.WriteString(h, strings.Join(cj.Spec.Env, "\x00")+"\n")
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// SetupWithManager wires the ClinkJob reconciler.
func (r *ClinkJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Scheme = mgr.GetScheme()
	r.RestConfig = mgr.GetConfig()
	cs, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return err
	}
	r.Clientset = cs
	return ctrl.NewControllerManagedBy(mgr).
		For(&clinkv1alpha1.ClinkJob{}).
		Complete(r)
}

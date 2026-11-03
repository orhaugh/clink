package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"sort"
	"strconv"
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

// ClinkJobReconciler runs a job, a SQL script or a compiled .so, on a
// ClinkCluster, and drains it to a savepoint and restores it on upgrade. It
// speaks the Coordinator's HTTP API with the cluster's token: a SQL job is
// submitted there, and every job's status, savepoint and cancel go there. A
// compiled job is built and submitted by the in-image `clink` CLI, exec'd in
// the leading Coordinator's pod, so the job builds with spec.env.
type ClinkJobReconciler struct {
	client.Client
	// APIReader reads the cluster's auth Secret from the API server, not the
	// cache, so the operator never lists or watches Secrets.
	APIReader  client.Reader
	Scheme     *runtime.Scheme
	Clientset  kubernetes.Interface
	RestConfig *rest.Config
}

// +kubebuilder:rbac:groups=clink.dev,resources=clinkjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=clink.dev,resources=clinkjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=clink.dev,resources=clinkjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get

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
				if api, err := newCoordinatorClient(ctx, r.APIReader, &cc); err == nil {
					if err := api.cancel(ctx, cj.Status.JobID); err != nil {
						log.FromContext(ctx).Info("cancel on delete failed; dropping the finalizer", "err", err.Error())
					}
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

	// Gate on a Running cluster with a ready Coordinator, whose API this
	// operator can present the cluster's token to.
	if clusterErr != nil || cc.Status.Phase != "Running" {
		return r.wait(ctx, &cj, "waiting for ClinkCluster to be Running", 5*time.Second)
	}
	if pods, err := r.readyCoordinatorPods(ctx, &cc); err != nil || len(pods) == 0 {
		return r.wait(ctx, &cj, "waiting for a ready Coordinator pod", 5*time.Second)
	}
	if other, err := r.jobNameClaimedBy(ctx, &cj); err != nil {
		return ctrl.Result{}, err
	} else if other != "" {
		return r.wait(ctx, &cj, fmt.Sprintf("jobName %q on cluster %s is ClinkJob %s's; give this one its own", cj.Spec.JobName, cj.Spec.ClusterName, other), 30*time.Second)
	}
	api, err := newCoordinatorClient(ctx, r.APIReader, &cc)
	if err != nil {
		return r.wait(ctx, &cj, "cannot present the cluster's token: "+err.Error(), 15*time.Second)
	}
	jobs, err := api.listJobs(ctx)
	if err != nil {
		// Nothing is decided on a list that failed: read as empty, it would
		// send a running job to a second submit.
		return r.wait(ctx, &cj, "cannot list the cluster's jobs: "+firstLine(err.Error()), 10*time.Second)
	}

	wantHash := specHash(&cj)

	// Scale-to-zero: an in-flight or requested park/wake takes precedence
	// over submit/upgrade/refresh. Handled first so a Resuming job can never
	// fall through to a fresh (state-dropping) initial submit.
	if done, res, err := r.reconcileSuspend(ctx, &cj, &cc, api, jobs, wantHash); done {
		return res, err
	}

	// Decide: initial submit, upgrade, or steady-state status refresh.
	switch {
	case cj.Status.JobID == 0:
		return r.submitInitial(ctx, &cj, &cc, api, jobs, wantHash)
	case cj.Status.SpecHash != wantHash:
		return r.upgrade(ctx, &cj, &cc, api, jobs, wantHash)
	default:
		return r.refreshStatus(ctx, &cj, api, jobs)
	}
}

// wait records why the reconcile cannot go on yet. A phase the job's state
// machine resumes from (Upgrading, Suspending, Suspended, Resuming) is kept:
// overwritten with Pending, a job parked on Idle would come back as a fresh
// submit and drop the savepoint its state waits in.
func (r *ClinkJobReconciler) wait(ctx context.Context, cj *clinkv1alpha1.ClinkJob, msg string, requeue time.Duration) (ctrl.Result, error) {
	phase := "Pending"
	switch cj.Status.Phase {
	case "Upgrading", "Suspending", "Suspended", "Resuming":
		phase = cj.Status.Phase
	}
	return r.setPhase(ctx, cj, phase, msg, requeue)
}

// jobNameClaimedBy names the ClinkJob that has this one's jobName on the same
// cluster first (the older, then the lower name), or "". The operator finds a
// job on the cluster by its name, so two ClinkJobs sharing one would adopt
// each other's jobs; the later one waits instead.
func (r *ClinkJobReconciler) jobNameClaimedBy(ctx context.Context, cj *clinkv1alpha1.ClinkJob) (string, error) {
	var all clinkv1alpha1.ClinkJobList
	if err := r.List(ctx, &all, client.InNamespace(cj.Namespace)); err != nil {
		return "", err
	}
	var claims []clinkv1alpha1.ClinkJob
	for _, o := range all.Items {
		applyJobDefaults(&o)
		if o.Spec.ClusterName == cj.Spec.ClusterName && o.Spec.JobName == cj.Spec.JobName && o.DeletionTimestamp.IsZero() {
			claims = append(claims, o)
		}
	}
	sort.Slice(claims, func(i, j int) bool {
		ti, tj := claims[i].CreationTimestamp, claims[j].CreationTimestamp
		if !ti.Equal(&tj) {
			return ti.Before(&tj)
		}
		return claims[i].Name < claims[j].Name
	})
	if len(claims) > 0 && claims[0].Name != cj.Name {
		return claims[0].Name, nil
	}
	return "", nil
}

// ---- scale-to-zero (park / wake) --------------------------------------------

const (
	suspendReasonManual = "Manual"
	suspendReasonIdle   = "Idle"
)

// reconcileSuspend owns every park/wake transition. Returns done=false only
// when the job is active with no suspend intent, letting the normal
// submit/upgrade/refresh flow proceed.
func (r *ClinkJobReconciler) reconcileSuspend(ctx context.Context, cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster, api *coordinatorClient, jobs []jobSummaryJSON, wantHash string) (bool, ctrl.Result, error) {
	parking := cj.Status.Phase == "Suspending"
	// Resuming counts as parked: its restore point is still staged, and only
	// the resume path may submit (an initial submit would drop the state).
	parked := cj.Status.Phase == "Suspended" || cj.Status.Phase == "Resuming"

	// Finish an in-flight park (stage 2: cancel) before honouring any new
	// intent - the savepoint is already persisted.
	if parking {
		res, err := r.parkCancelStage(ctx, cj, api)
		return true, res, err
	}

	if cj.Spec.Suspend {
		switch {
		case parked:
			// A resume submit may have landed just before the re-suspend;
			// adopt it so the park path can drain it rather than leak it.
			if id := liveJob(jobs, cj.Spec.JobName, cj.Status.JobID); id > 0 {
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
			res, err := r.park(ctx, cj, api, suspendReasonManual)
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
			res, err := r.resume(ctx, cj, cc, api, jobs, wantHash)
			return true, res, err
		}
		wake, detail, poll := r.wakeDue(ctx, cj)
		if wake {
			res, err := r.resume(ctx, cj, cc, api, jobs, wantHash)
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
func (r *ClinkJobReconciler) park(ctx context.Context, cj *clinkv1alpha1.ClinkJob, api *coordinatorClient, reason string) (ctrl.Result, error) {
	lg := log.FromContext(ctx)
	// A park landing mid-upgrade (old job already drained + cancelled, new
	// one not yet up) adopts the upgrade's savepoint instead of trying to
	// savepoint the already-cancelled job.
	if cj.Status.UpgradeToHash != "" {
		cj.Status.SuspendSavepoint = cj.Status.UpgradeRestore
		cj.Status.UpgradeToHash = ""
		cj.Status.UpgradeRestore = nil
		cj.Status.UpgradeFromJobID = 0
		cj.Status.SuspendReason = reason
		cj.Status.JobID = 0
		now := metav1.Now()
		cj.Status.SuspendedAt = &now
		return r.setPhase(ctx, cj, "Suspended", "parked mid-upgrade ("+reason+")", 30*time.Second)
	}
	if cj.Spec.UpgradeMode == "savepoint" && cj.Status.SuspendSavepoint == nil {
		sp, err := api.savepoint(ctx, cj.Status.JobID)
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
func (r *ClinkJobReconciler) parkCancelStage(ctx context.Context, cj *clinkv1alpha1.ClinkJob, api *coordinatorClient) (ctrl.Result, error) {
	lg := log.FromContext(ctx)
	if cj.Status.JobID != 0 {
		if err := api.cancel(ctx, cj.Status.JobID); err != nil {
			lg.Info("cancel during park failed; continuing", "err", err.Error())
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

// resume resubmits the job restored from the park savepoint, retried until it
// is accepted; adopts a submit whose status update was lost.
func (r *ClinkJobReconciler) resume(ctx context.Context, cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster, api *coordinatorClient, jobs []jobSummaryJSON, wantHash string) (ctrl.Result, error) {
	if id := liveJob(jobs, cj.Spec.JobName, cj.Status.JobID); id > 0 {
		msg := fmt.Sprintf("resumed as job %d (adopted)", id)
		r.clearSuspend(cj, wantHash, id)
		return r.setPhase(ctx, cj, "Running", msg, 20*time.Second)
	}
	restore := cj.Status.SuspendSavepoint
	id, err := r.doSubmit(ctx, cj, cc, api, restore)
	if err != nil {
		return r.setPhase(ctx, cj, "Resuming", "resume submit rejected: "+err.Error(), 10*time.Second)
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

func (r *ClinkJobReconciler) submitInitial(ctx context.Context, cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster, api *coordinatorClient, jobs []jobSummaryJSON, wantHash string) (ctrl.Result, error) {
	// Retry-safe: if a live job of this name already exists (a prior submit
	// whose status update was lost, or an operator restart), adopt it instead
	// of submitting again.
	if id := liveJob(jobs, cj.Spec.JobName, 0); id > 0 {
		cj.Status.JobID = id
		cj.Status.SpecHash = wantHash
		return r.setPhase(ctx, cj, "Running", fmt.Sprintf("adopted running job %d", id), 20*time.Second)
	}
	id, err := r.doSubmit(ctx, cj, cc, api, nil)
	if err != nil {
		return r.setPhase(ctx, cj, "Submitting", "submit rejected: "+err.Error(), 10*time.Second)
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
func (r *ClinkJobReconciler) upgrade(ctx context.Context, cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster, api *coordinatorClient, jobs []jobSummaryJSON, wantHash string) (ctrl.Result, error) {
	lg := log.FromContext(ctx)

	// Stage 1: savepoint + cancel the old job, once per target hash. The
	// savepoint is recorded before the cancel. A retry after a cancel whose
	// status update was lost finds the job no longer live and restores from
	// the recorded savepoint; one that finds it still live takes a fresh one,
	// as the job has run on since.
	if cj.Status.UpgradeToHash != wantHash {
		var restore *clinkv1alpha1.SavepointRef
		if cj.Spec.UpgradeMode == "savepoint" {
			old, listed := findJob(jobs, cj.Status.JobID)
			if cj.Status.UpgradeRestore != nil && cj.Status.UpgradeFromJobID == cj.Status.JobID && (!listed || !old.live()) {
				restore = cj.Status.UpgradeRestore
			} else {
				sp, err := api.savepoint(ctx, cj.Status.JobID)
				if err != nil {
					lg.Info("savepoint failed; will retry", "err", err.Error())
					return r.setPhase(ctx, cj, "Upgrading", "savepoint failed: "+err.Error(), 10*time.Second)
				}
				cj.Status.UpgradeRestore = sp
				cj.Status.UpgradeFromJobID = cj.Status.JobID
				cj.Status.LastSavepoint = sp
				cj.Status.Phase = "Upgrading"
				cj.Status.Message = fmt.Sprintf("savepoint %s@%d taken; cancelling job %d", sp.Dir, sp.CheckpointID, cj.Status.JobID)
				if err := r.Status().Update(ctx, cj); err != nil {
					// Not cancelled: the retry takes a fresh savepoint.
					return ctrl.Result{}, err
				}
				restore = sp
			}
		}
		if err := api.cancel(ctx, cj.Status.JobID); err != nil {
			lg.Info("cancel during upgrade failed; continuing", "err", err.Error())
		}
		// Commit stage 1: from here the old job is gone; never savepoint it again.
		cj.Status.UpgradeToHash = wantHash
		cj.Status.UpgradeRestore = restore
		return r.setPhase(ctx, cj, "Upgrading", "drained to savepoint; resubmitting", 2*time.Second)
	}

	// Stage 2: (re)submit the new job, restored from the stage-1 savepoint.
	// Adopt an already-submitted new job on retry so a lost status update does
	// not leak a second job. The old (cancelled) job is not live, so it is
	// skipped; only a fresh job with a higher id counts.
	if id := liveJob(jobs, cj.Spec.JobName, cj.Status.JobID); id > 0 {
		cj.Status.JobID = id
		cj.Status.SpecHash = wantHash
		cj.Status.UpgradeToHash = ""
		cj.Status.UpgradeRestore = nil
		cj.Status.UpgradeFromJobID = 0
		return r.setPhase(ctx, cj, "Running", fmt.Sprintf("upgraded to job %d (adopted)", id), 20*time.Second)
	}
	restore := cj.Status.UpgradeRestore
	id, err := r.doSubmit(ctx, cj, cc, api, restore)
	if err != nil {
		return r.setPhase(ctx, cj, "Upgrading", "resubmit rejected: "+err.Error(), 10*time.Second)
	}
	cj.Status.JobID = id
	cj.Status.SpecHash = wantHash
	cj.Status.UpgradeToHash = ""
	cj.Status.UpgradeRestore = nil
	cj.Status.UpgradeFromJobID = 0
	msg := fmt.Sprintf("upgraded to job %d", id)
	if restore != nil {
		msg += fmt.Sprintf(" restored from %s@%d", restore.Dir, restore.CheckpointID)
	}
	return r.setPhase(ctx, cj, "Running", msg, 20*time.Second)
}

// jobMissingGrace is how long the cluster may not list a job before the
// operator resubmits it (see ClinkJobStatus.JobMissingSince).
const jobMissingGrace = 2 * time.Minute

func (r *ClinkJobReconciler) refreshStatus(ctx context.Context, cj *clinkv1alpha1.ClinkJob, api *coordinatorClient, jobs []jobSummaryJSON) (ctrl.Result, error) {
	js, found := findJob(jobs, cj.Status.JobID)
	if !found {
		now := metav1.Now()
		if cj.Status.JobMissingSince == nil {
			cj.Status.JobMissingSince = &now
		}
		missing := now.Sub(cj.Status.JobMissingSince.Time)
		if missing < jobMissingGrace {
			return r.setPhase(ctx, cj, "Pending", fmt.Sprintf("job %d is not listed by the cluster (%ds); resubmitting it if it is still missing at %ds",
				cj.Status.JobID, int(missing.Seconds()), int(jobMissingGrace.Seconds())), 10*time.Second)
		}
		msg := fmt.Sprintf("job %d was not listed for %ds; resubmitting", cj.Status.JobID, int(missing.Seconds()))
		cj.Status.JobID = 0 // force resubmit next reconcile
		cj.Status.JobMissingSince = nil
		return r.setPhase(ctx, cj, "Pending", msg, time.Second)
	}
	cj.Status.JobMissingSince = nil
	phase, msg := jobPhase(js)

	// Idle auto-park: a healthy Running job with the policy armed parks once
	// no operator has processed a record for the window. Activity = the sum
	// of per-operator records_in; any change resets the clock.
	if phase == "Running" && cj.Spec.SuspendPolicy != nil && cj.Spec.SuspendPolicy.IdleAfterSeconds > 0 {
		idleAfter := time.Duration(cj.Spec.SuspendPolicy.IdleAfterSeconds) * time.Second
		if count, ok := api.recordsIn(ctx, cj.Status.JobID); ok {
			now := metav1.Now()
			if count != cj.Status.ActivityCount || cj.Status.LastActivityTime == nil {
				cj.Status.ActivityCount = count
				cj.Status.LastActivityTime = &now
			} else if now.Time.Sub(cj.Status.LastActivityTime.Time) >= idleAfter {
				return r.park(ctx, cj, api, suspendReasonIdle)
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

func (r *ClinkJobReconciler) setPhase(ctx context.Context, cj *clinkv1alpha1.ClinkJob, phase, msg string, requeue time.Duration) (ctrl.Result, error) {
	cj.Status.Phase = phase
	cj.Status.Message = msg
	cj.Status.ObservedGeneration = cj.Generation
	if err := r.Status().Update(ctx, cj); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeue}, nil
}

// ---- submission ------------------------------------------------------------

// doSubmit starts the job, restored from restore when it is set, and returns
// its id on the cluster.
func (r *ClinkJobReconciler) doSubmit(ctx context.Context, cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster, api *coordinatorClient, restore *clinkv1alpha1.SavepointRef) (int64, error) {
	if cj.Spec.SQL != "" {
		return api.submitSQL(ctx, sqlSubmission{
			Script:               cj.Spec.SQL,
			Name:                 cj.Spec.JobName,
			Parallelism:          cj.Spec.Parallelism,
			CheckpointDir:        checkpointDir(cj, cc),
			CheckpointIntervalMs: cj.Spec.CheckpointIntervalMs,
			Restore:              restore,
		})
	}
	return r.submitCompiled(ctx, cj, cc, restore)
}

// checkpointDir is where the job checkpoints on the cluster's checkpoint
// volume, or "" when it does not checkpoint.
func checkpointDir(cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster) string {
	if !cc.Spec.CheckpointStorage.Enabled || cj.Spec.CheckpointIntervalMs <= 0 {
		return ""
	}
	return cc.Spec.CheckpointStorage.MountPath + "/" + cj.Spec.JobName
}

// errCompiledJobOverTLS is why a compiled job is not submitted to a cluster
// whose control plane is TLS.
var errCompiledJobOverTLS = errors.New("a compiled job (jobSo) is submitted by the clink CLI over the control port, " +
	"which does not speak TLS, and this cluster's control plane is TLS: run the job as SQL, or on a cluster without TLS")

// submitCompiled runs `clink run` for a compiled job in the leading
// Coordinator's pod, which alone binds the control port: the job is built
// there, with spec.env, and submitted over that port.
func (r *ClinkJobReconciler) submitCompiled(ctx context.Context, cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster, restore *clinkv1alpha1.SavepointRef) (int64, error) {
	if cc.Spec.TLS.Enabled {
		return 0, errCompiledJobOverTLS
	}
	pod, err := r.leaderCoordinatorPod(ctx, cc)
	if err != nil {
		return 0, err
	}
	if pod == "" {
		return 0, errors.New("no Coordinator pod says it leads")
	}
	stdout, stderr, err := r.execClink(ctx, cc.Namespace, pod, r.submitArgs(cj, cc, restore))
	if id, ok := submittedJobID(stdout); ok {
		return id, nil
	}
	return 0, errors.New(submitRefusal(stdout, stderr, err))
}

// submitRefusal is why `clink run` did not start the job: its "submit:" line,
// which carries the Coordinator's reject= reason, rather than the JSON line
// before it, which says only ok:false; else the first thing it printed.
func submitRefusal(stdout, stderr string, err error) string {
	for _, line := range strings.Split(stdout, "\n") {
		if line = strings.TrimSpace(line); strings.HasPrefix(line, "submit:") {
			return line
		}
	}
	if d := firstLine(stdout); d != "" {
		return d
	}
	if d := firstLine(stderr); d != "" {
		return d
	}
	if err != nil {
		return err.Error()
	}
	return "clink run printed nothing"
}

func (r *ClinkJobReconciler) submitArgs(cj *clinkv1alpha1.ClinkJob, cc *clinkv1alpha1.ClinkCluster, restore *clinkv1alpha1.SavepointRef) []string {
	run := []string{
		"clink", "run",
		"--job=" + cj.Spec.JobSo,
		"--name=" + cj.Spec.JobName,
		"--coordinator-host=127.0.0.1",
		fmt.Sprintf("--coordinator-port=%d", cc.Spec.Coordinator.ControlPort),
		"--wait-timeout-s=0", // fire-and-forget: return after the coordinator accepts
	}
	if dir := checkpointDir(cj, cc); dir != "" {
		run = append(run,
			"--checkpoint-dir="+dir,
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

var submitLineRe = regexp.MustCompile(`\bjob_id=(\d+)\b.*\bok=1\b`)

// submittedJobID reads the accepted job's id from `clink run`'s output: the
// JSON line it prints first, or, from a CLI that predates it, the
// "submit: ... job_id=N ... ok=1" line. The exit status says nothing here, as
// a streaming job submitted without waiting never completes.
func submittedJobID(stdout string) (int64, bool) {
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "{") {
			var ack struct {
				OK    bool  `json:"ok"`
				JobID int64 `json:"job_id"`
			}
			if json.Unmarshal([]byte(line), &ack) == nil && ack.OK && ack.JobID > 0 {
				return ack.JobID, true
			}
			continue
		}
		if m := submitLineRe.FindStringSubmatch(line); m != nil {
			if id, err := strconv.ParseInt(m[1], 10, 64); err == nil && id > 0 {
				return id, true
			}
		}
	}
	return 0, false
}

// ---- the cluster's jobs ----------------------------------------------------

// liveJob is the newest live job on the cluster with this name and an id above
// after, or 0. The operator adopts it rather than submitting again.
func liveJob(jobs []jobSummaryJSON, name string, after int64) int64 {
	var best int64
	for _, j := range jobs {
		if j.Name == name && j.live() && j.ID > after && j.ID > best {
			best = j.ID
		}
	}
	return best
}

func findJob(jobs []jobSummaryJSON, id int64) (jobSummaryJSON, bool) {
	for _, j := range jobs {
		if j.ID == id {
			return j, true
		}
	}
	return jobSummaryJSON{}, false
}

// jobPhase is the ClinkJob phase, and its message, for the job as the
// Coordinator reports it.
func jobPhase(js jobSummaryJSON) (string, string) {
	phase := "Running"
	switch {
	case js.CancelRequested:
		phase = "Cancelled"
	case js.Status == "FAILED", js.Status == "" && js.ErrorCount > 0:
		phase = "Failed"
	case js.Status == "COMPLETED_OK", js.Status == "" && js.CompletionSignalled:
		phase = "Completed"
	case js.Status == "CANCELLED":
		phase = "Cancelled"
	}
	switch phase {
	case "Failed":
		return phase, fmt.Sprintf("job %d failed with %d error(s)", js.ID, js.ErrorCount)
	case "Completed":
		return phase, fmt.Sprintf("job %d completed", js.ID)
	case "Cancelled":
		return phase, fmt.Sprintf("job %d cancelled", js.ID)
	}
	msg := fmt.Sprintf("job %d running", js.ID)
	if js.ErrorCount > 0 {
		msg += fmt.Sprintf("; %d task error(s), recovered", js.ErrorCount)
	}
	return phase, msg
}

// ---- pod discovery + exec --------------------------------------------------

// readyCoordinatorPods are the cluster's Running, Ready Coordinator pods.
// Under HA a standby is ready too: it answers the API by forwarding it.
func (r *ClinkJobReconciler) readyCoordinatorPods(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) ([]corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(cc.Namespace), client.MatchingLabels{
		"app.kubernetes.io/instance":  cc.Name,
		"app.kubernetes.io/component": "coordinator",
	}); err != nil {
		return nil, err
	}
	var ready []corev1.Pod
	for _, p := range pods.Items {
		if p.Status.Phase != corev1.PodRunning || !p.DeletionTimestamp.IsZero() {
			continue
		}
		for _, c := range p.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				ready = append(ready, p)
				break
			}
		}
	}
	return ready, nil
}

// leaderCoordinatorPod is the Coordinator pod that leads, and so binds the
// control port: without HA the one ready Coordinator, under HA the one whose
// /readyz says "leader".
func (r *ClinkJobReconciler) leaderCoordinatorPod(ctx context.Context, cc *clinkv1alpha1.ClinkCluster) (string, error) {
	pods, err := r.readyCoordinatorPods(ctx, cc)
	if err != nil {
		return "", err
	}
	port := strconv.Itoa(int(cc.Spec.Coordinator.HTTPPort))
	for _, p := range pods {
		if !cc.Spec.HA.Enabled {
			return p.Name, nil
		}
		if p.Status.PodIP != "" && coordinatorRole(ctx, net.JoinHostPort(p.Status.PodIP, port)) == "leader" {
			return p.Name, nil
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
		cj.Spec.JobName = cj.Name
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
	// A SQL job's inputs, written only for a SQL job, so a compiled job hashes
	// as it did before SQL jobs existed and upgrading the operator upgrades
	// none of them.
	if cj.Spec.SQL != "" {
		io.WriteString(h, "sql\x00"+cj.Spec.SQL+"\n")
		fmt.Fprintf(h, "parallelism\x00%d\n", cj.Spec.Parallelism)
	}
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
	if r.APIReader == nil {
		r.APIReader = mgr.GetAPIReader()
	}
	cs, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		return err
	}
	r.Clientset = cs
	return ctrl.NewControllerManagedBy(mgr).
		For(&clinkv1alpha1.ClinkJob{}).
		Complete(r)
}

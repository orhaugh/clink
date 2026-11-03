package controller

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clinkv1alpha1 "github.com/clink/clink-operator/api/v1alpha1"
)

func TestTheSubmittedJobIdIsReadFromTheCLIsOutput(t *testing.T) {
	cases := map[string]struct {
		stdout string
		id     int64
		ok     bool
	}{
		"json line": {"{\"ok\":true,\"job_id\":5,\"completed\":false}\nsubmit: name=wc job_id=5 completed=0 ok=1\n", 5, true},
		"legacy":    {"submit: name=wc job_id=8 completed=0 ok=1\n", 8, true},
		"refused":   {"{\"ok\":false,\"job_id\":0,\"completed\":false}\nsubmit: name=wc job_id=0 completed=0 ok=0 reject=no slots\n", 0, false},
		"nothing":   {"", 0, false},
	}
	for name, c := range cases {
		id, ok := submittedJobID(c.stdout)
		if id != c.id || ok != c.ok {
			t.Errorf("%s: (%d, %v), want (%d, %v)", name, id, ok, c.id, c.ok)
		}
	}
}

func TestARefusedSubmitSaysWhy(t *testing.T) {
	stdout := "{\"ok\":false,\"job_id\":0,\"completed\":false}\n" +
		"submit: name=wc job_id=0 completed=0 ok=0 reject=plugin ABI fingerprint mismatch\n"
	if got := submitRefusal(stdout, "", nil); !strings.Contains(got, "reject=plugin ABI fingerprint mismatch") {
		t.Errorf("refusal: %q", got)
	}
	if got := submitRefusal("", "clink_submit_job: job .so not found\n", nil); got != "clink_submit_job: job .so not found" {
		t.Errorf("stderr: %q", got)
	}
}

func TestOnlyALiveJobOfTheSameNameIsAdopted(t *testing.T) {
	jobs := []jobSummaryJSON{
		{ID: 1, Name: "qoe", Status: "CANCELLED", CompletionSignalled: true, CancelRequested: true},
		{ID: 2, Name: "alerts", Status: "RUNNING"},
		{ID: 3, Name: "qoe", Status: "RUNNING", CancelRequested: true}, // cancelling
		{ID: 4, Name: "qoe", Status: "RUNNING", ErrorCount: 2},         // restarted, still running
		{ID: 5, Name: "edges", Status: "FAILED", CompletionSignalled: true, ErrorCount: 1},
	}
	if got := liveJob(jobs, "qoe", 0); got != 4 {
		t.Errorf("qoe: %d, want 4", got)
	}
	if got := liveJob(jobs, "qoe", 4); got != 0 {
		t.Errorf("qoe after 4: %d, want 0", got)
	}
	if got := liveJob(jobs, "alerts", 0); got != 2 {
		t.Errorf("alerts: %d, want 2", got)
	}
	if got := liveJob(jobs, "edges", 0); got != 0 {
		t.Errorf("edges: %d, want 0 (failed)", got)
	}
	if got := liveJob(jobs, "ledger", 0); got != 0 {
		t.Errorf("ledger: %d, want 0", got)
	}
}

func TestTheJobsPhaseIsTheCoordinatorsStatus(t *testing.T) {
	cases := []struct {
		js    jobSummaryJSON
		phase string
		msg   string
	}{
		{jobSummaryJSON{ID: 1, Status: "RUNNING"}, "Running", "job 1 running"},
		{jobSummaryJSON{ID: 1, Status: "RUNNING", ErrorCount: 3}, "Running", "job 1 running; 3 task error(s), recovered"},
		{jobSummaryJSON{ID: 1, Status: "RUNNING", CancelRequested: true}, "Cancelled", "job 1 cancelled"},
		{jobSummaryJSON{ID: 1, Status: "FAILED", ErrorCount: 1, CompletionSignalled: true}, "Failed", "job 1 failed with 1 error(s)"},
		{jobSummaryJSON{ID: 1, Status: "COMPLETED_OK", CompletionSignalled: true}, "Completed", "job 1 completed"},
		{jobSummaryJSON{ID: 1, Status: "CANCELLED", CompletionSignalled: true}, "Cancelled", "job 1 cancelled"},
		// A Coordinator too old to report a status.
		{jobSummaryJSON{ID: 1, ErrorCount: 1}, "Failed", "job 1 failed with 1 error(s)"},
		{jobSummaryJSON{ID: 1, CompletionSignalled: true}, "Completed", "job 1 completed"},
		{jobSummaryJSON{ID: 1}, "Running", "job 1 running"},
	}
	for _, c := range cases {
		phase, msg := jobPhase(c.js)
		if phase != c.phase || msg != c.msg {
			t.Errorf("%+v: (%q, %q), want (%q, %q)", c.js, phase, msg, c.phase, c.msg)
		}
	}
}

// A compiled job's hash is what the operator computed before SQL jobs existed,
// so upgrading the operator upgrades no running job.
func TestACompiledJobsSpecHashIsUnchanged(t *testing.T) {
	cj := &clinkv1alpha1.ClinkJob{Spec: clinkv1alpha1.ClinkJobSpec{
		JobSo: "/opt/clink/jobs/rescale_test_job.so",
		Args:  []string{"--state-backend=rocksdb"},
		Env:   []string{"CLINK_RESCALE_TICK_MS=100"},
	}}
	if got := specHash(cj); got != "eaa45e2a75214e4e" {
		t.Fatalf("specHash = %s", got)
	}
}

func TestASQLJobsScriptAndParallelismAreItsSpec(t *testing.T) {
	a := &clinkv1alpha1.ClinkJob{Spec: clinkv1alpha1.ClinkJobSpec{SQL: "INSERT INTO b SELECT * FROM a"}}
	b := a.DeepCopy()
	b.Spec.SQL += " WHERE x > 1"
	c := a.DeepCopy()
	c.Spec.Parallelism = 4
	if specHash(a) == specHash(b) || specHash(a) == specHash(c) {
		t.Fatal("a change to the script or its parallelism does not change the hash")
	}
	d := a.DeepCopy()
	d.Spec.JobName = "renamed"
	if specHash(d) != specHash(a) {
		t.Fatal("the job's name changed its hash")
	}
}

func TestAJobIsNamedAfterItsClinkJob(t *testing.T) {
	cj := &clinkv1alpha1.ClinkJob{ObjectMeta: metav1.ObjectMeta{Name: "qoe"}}
	applyJobDefaults(cj)
	if cj.Spec.JobName != "qoe" || cj.Spec.UpgradeMode != "savepoint" {
		t.Fatalf("defaults: %+v", cj.Spec)
	}
	named := &clinkv1alpha1.ClinkJob{ObjectMeta: metav1.ObjectMeta{Name: "qoe"}, Spec: clinkv1alpha1.ClinkJobSpec{JobName: "job"}}
	applyJobDefaults(named)
	if named.Spec.JobName != "job" {
		t.Fatalf("a given jobName was replaced: %q", named.Spec.JobName)
	}
}

func jobScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clinkv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

// A job parked on Idle waits in its savepoint. Reported as Pending while the
// cluster is away, it came back as a fresh submit and dropped that state.
func TestWaitingKeepsAPhaseTheStateMachineResumesFrom(t *testing.T) {
	for phase, want := range map[string]string{
		"Suspended": "Suspended", "Resuming": "Resuming", "Suspending": "Suspending",
		"Upgrading": "Upgrading", "Running": "Pending", "Submitting": "Pending", "": "Pending",
	} {
		cj := &clinkv1alpha1.ClinkJob{
			ObjectMeta: metav1.ObjectMeta{Name: "qoe", Namespace: "ns"},
			Status:     clinkv1alpha1.ClinkJobStatus{Phase: phase},
		}
		c := fake.NewClientBuilder().WithScheme(jobScheme(t)).WithObjects(cj).WithStatusSubresource(cj).Build()
		r := &ClinkJobReconciler{Client: c}
		if _, err := r.wait(context.Background(), cj, "waiting for ClinkCluster to be Running", time.Second); err != nil {
			t.Fatal(err)
		}
		var got clinkv1alpha1.ClinkJob
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "qoe"}, &got); err != nil {
			t.Fatal(err)
		}
		if got.Status.Phase != want {
			t.Errorf("%q: phase %q, want %q", phase, got.Status.Phase, want)
		}
	}
}

func TestTheFirstClinkJobToClaimAJobNameKeepsIt(t *testing.T) {
	older := metav1.NewTime(time.Unix(1000, 0))
	newer := metav1.NewTime(time.Unix(2000, 0))
	mk := func(name, cluster, jobName string, at metav1.Time) *clinkv1alpha1.ClinkJob {
		return &clinkv1alpha1.ClinkJob{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", CreationTimestamp: at},
			Spec:       clinkv1alpha1.ClinkJobSpec{ClusterName: cluster, JobName: jobName},
		}
	}
	first := mk("first", "c", "shared", older)
	second := mk("second", "c", "shared", newer)
	elsewhere := mk("elsewhere", "other", "shared", older)
	defaulted := mk("defaulted", "c", "", older) // named after itself
	c := fake.NewClientBuilder().WithScheme(jobScheme(t)).WithObjects(first, second, elsewhere, defaulted).Build()
	r := &ClinkJobReconciler{Client: c}
	for _, cj := range []*clinkv1alpha1.ClinkJob{first, second, elsewhere, defaulted} {
		applyJobDefaults(cj)
	}
	cases := map[*clinkv1alpha1.ClinkJob]string{first: "", second: "first", elsewhere: "", defaulted: ""}
	for cj, want := range cases {
		got, err := r.jobNameClaimedBy(context.Background(), cj)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s: claimed by %q, want %q", cj.Name, got, want)
		}
	}
}

func TestACompiledJobIsNotSentOverAControlPlaneItCannotReach(t *testing.T) {
	cc := &clinkv1alpha1.ClinkCluster{Spec: clinkv1alpha1.ClinkClusterSpec{TLS: clinkv1alpha1.TLSSpec{Enabled: true, CoordinatorSecret: "tls"}}}
	cj := &clinkv1alpha1.ClinkJob{Spec: clinkv1alpha1.ClinkJobSpec{JobSo: "/opt/clink/jobs/x.so"}}
	_, err := (&ClinkJobReconciler{}).submitCompiled(context.Background(), cj, cc, nil)
	if err == nil || !strings.Contains(err.Error(), "does not speak TLS") {
		t.Fatalf("err = %v", err)
	}
}

// A job the cluster stops listing is resubmitted only once it has been missing
// for the grace period: a new leader recovering it, or a recovery parked until
// a Worker returns, lists it again in time, and a resubmission would run it
// twice.
func TestAMissingJobIsResubmittedOnlyAfterTheGracePeriod(t *testing.T) {
	cj := &clinkv1alpha1.ClinkJob{
		ObjectMeta: metav1.ObjectMeta{Name: "qoe", Namespace: "ns"},
		Spec:       clinkv1alpha1.ClinkJobSpec{JobName: "qoe"},
		Status:     clinkv1alpha1.ClinkJobStatus{Phase: "Running", JobID: 7},
	}
	c := fake.NewClientBuilder().WithScheme(jobScheme(t)).WithObjects(cj).WithStatusSubresource(cj).Build()
	r := &ClinkJobReconciler{Client: c}
	ctx := context.Background()
	others := []jobSummaryJSON{{ID: 3, Name: "alerts", Status: "RUNNING"}}

	if _, err := r.refreshStatus(ctx, cj, nil, others); err != nil {
		t.Fatal(err)
	}
	if cj.Status.JobID != 7 || cj.Status.JobMissingSince == nil {
		t.Fatalf("first miss: jobID %d, missing since %v", cj.Status.JobID, cj.Status.JobMissingSince)
	}

	// Listed again: the clock stops.
	if _, err := r.refreshStatus(ctx, cj, nil, append(others, jobSummaryJSON{ID: 7, Name: "qoe", Status: "RUNNING"})); err != nil {
		t.Fatal(err)
	}
	if cj.Status.JobMissingSince != nil || cj.Status.Phase != "Running" {
		t.Fatalf("relisted: phase %q, missing since %v", cj.Status.Phase, cj.Status.JobMissingSince)
	}

	// Missing for longer than the grace period: resubmitted.
	if _, err := r.refreshStatus(ctx, cj, nil, others); err != nil {
		t.Fatal(err)
	}
	long := metav1.NewTime(time.Now().Add(-jobMissingGrace - time.Second))
	cj.Status.JobMissingSince = &long
	if _, err := r.refreshStatus(ctx, cj, nil, others); err != nil {
		t.Fatal(err)
	}
	if cj.Status.JobID != 0 || cj.Status.JobMissingSince != nil {
		t.Fatalf("after the grace period: jobID %d, missing since %v", cj.Status.JobID, cj.Status.JobMissingSince)
	}
}

// An upgrade ends the old job at a savepoint, which the Coordinator takes and
// cancels the job at with no checkpoint completing in between, and records it.
// A retry whose recording was lost finds the job ended and restores from what
// was recorded, or from the job's last completed checkpoint, the savepoint.
func TestAnUpgradeEndsTheOldJobAtASavepoint(t *testing.T) {
	recorded := &clinkv1alpha1.SavepointRef{Dir: "/ckpt/edges/_jobs/5", CheckpointID: 9}
	running := jobSummaryJSON{ID: 5, Name: "edges", Status: "RUNNING"}
	ended := jobSummaryJSON{ID: 5, Name: "edges", Status: "CANCELLED", CompletionSignalled: true, CancelRequested: true}
	cases := map[string]struct {
		recorded    *clinkv1alpha1.SavepointRef
		fromJob     int64
		old         jobSummaryJSON
		supported   bool  // the Coordinator takes ?cancel=true
		savepoints  int   // savepoint requests the upgrade makes
		cancels     int   // separate cancel requests
		restoreFrom int64 // the checkpoint the new job restores from
	}{
		"a running job is ended at a fresh savepoint":                          {nil, 0, running, true, 1, 0, 12},
		"a stale record of a running job is not reused":                        {recorded, 5, running, true, 1, 0, 12},
		"an ended job restores from what was recorded":                         {recorded, 5, ended, true, 0, 0, 9},
		"an ended job with nothing recorded restores from its last checkpoint": {nil, 0, ended, true, 0, 0, 18},
		"an older Coordinator's savepoint is followed by a cancel":             {nil, 0, running, false, 1, 1, 12},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var savepoints, cancels int
			f := &fakeCoordinator{answer: func(r *http.Request, _ string) (int, string) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/savepoint"):
					savepoints++
					if c.supported {
						return 200, `{"job_id":5,"ok":true,"checkpoint_id":12,"checkpoint_dir":"/ckpt/edges/_jobs/5","message":"","cancelled":true}`
					}
					return 200, `{"job_id":5,"ok":true,"checkpoint_id":12,"checkpoint_dir":"/ckpt/edges/_jobs/5","message":""}`
				case strings.HasSuffix(r.URL.Path, "/cancel"):
					cancels++
					return 200, `{"job_id":5,"ok":true,"message":""}`
				case r.URL.Path == "/api/v1/jobs/5":
					return 200, `{"id":5,"checkpoint_dir":"/ckpt/edges","latest_completed_checkpoint_id":18}`
				}
				return 404, ""
			}}
			cj := &clinkv1alpha1.ClinkJob{
				ObjectMeta: metav1.ObjectMeta{Name: "edges", Namespace: "ns"},
				Spec:       clinkv1alpha1.ClinkJobSpec{JobName: "edges", SQL: "v2", UpgradeMode: "savepoint"},
				Status: clinkv1alpha1.ClinkJobStatus{Phase: "Running", JobID: 5, SpecHash: "v1hash",
					UpgradeRestore: c.recorded, UpgradeFromJobID: c.fromJob},
			}
			k := fake.NewClientBuilder().WithScheme(jobScheme(t)).WithObjects(cj).WithStatusSubresource(cj).Build()
			r := &ClinkJobReconciler{Client: k}
			want := specHash(cj)
			if _, err := r.upgrade(context.Background(), cj, cluster(nil), f.client(t, ""), []jobSummaryJSON{c.old}, want); err != nil {
				t.Fatal(err)
			}
			if savepoints != c.savepoints || cancels != c.cancels {
				t.Errorf("savepoints %d, cancels %d; want %d, %d", savepoints, cancels, c.savepoints, c.cancels)
			}
			if cj.Status.UpgradeToHash != want || cj.Status.UpgradeRestore == nil || cj.Status.UpgradeFromJobID != 5 {
				t.Fatalf("status: %+v", cj.Status)
			}
			if cj.Status.UpgradeRestore.CheckpointID != c.restoreFrom || cj.Status.UpgradeRestore.Dir != "/ckpt/edges/_jobs/5" {
				t.Errorf("restores from %+v, want checkpoint %d", cj.Status.UpgradeRestore, c.restoreFrom)
			}
		})
	}
}

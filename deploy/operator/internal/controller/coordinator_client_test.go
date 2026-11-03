package controller

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	clinkv1alpha1 "github.com/clink/clink-operator/api/v1alpha1"
)

// fakeCoordinator answers the operator's requests as a Coordinator would and
// records what it was sent.
type fakeCoordinator struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   []string
	answer   func(r *http.Request, body string) (int, string)
}

func (f *fakeCoordinator) client(t *testing.T, token string) *coordinatorClient {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.requests = append(f.requests, r)
		f.bodies = append(f.bodies, string(b))
		f.mu.Unlock()
		status, body := f.answer(r, string(b))
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return &coordinatorClient{base: srv.URL, token: token, http: srv.Client()}
}

func (f *fakeCoordinator) modes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, r := range f.requests {
		out = append(out, r.URL.Query().Get("mode"))
	}
	return out
}

func TestTheClientPresentsTheClusterTokenAndNoneWithoutOne(t *testing.T) {
	var seen []string
	f := &fakeCoordinator{answer: func(r *http.Request, _ string) (int, string) {
		seen = append(seen, r.Header.Get("Authorization"))
		return 200, `{"jobs":[]}`
	}}
	if _, err := f.client(t, "s3cret").listJobs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client(t, "").listJobs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if seen[0] != "Bearer s3cret" || seen[1] != "" {
		t.Fatalf("Authorization headers = %q", seen)
	}
}

func TestAFailedListIsAnErrorNotAnEmptyCluster(t *testing.T) {
	f := &fakeCoordinator{answer: func(*http.Request, string) (int, string) {
		return 401, `{"error":"unauthorized"}`
	}}
	if jobs, err := f.client(t, "wrong").listJobs(context.Background()); err == nil {
		t.Fatalf("a 401 read as %d jobs", len(jobs))
	}
}

func TestASQLJobIsCompiledInACatalogOfItsOwnThenSubmitted(t *testing.T) {
	f := &fakeCoordinator{answer: func(r *http.Request, _ string) (int, string) {
		if r.URL.Query().Get("mode") == "compile" {
			return 200, `{"ok":true,"mode":"compile","specs":[{"name":"qoe","spec_json":"{}"}]}`
		}
		return 200, `{"ok":true,"mode":"submit","jobs":[{"name":"qoe","job_id":7}]}`
	}}
	id, err := f.client(t, "t").submitSQL(context.Background(), sqlSubmission{
		Script:               "CREATE TABLE a (x INT) WITH (connector='datagen'); INSERT INTO b SELECT x FROM a;",
		Name:                 "qoe",
		Parallelism:          2,
		CheckpointDir:        "/ckpt/qoe",
		CheckpointIntervalMs: 2000,
		Restore:              &clinkv1alpha1.SavepointRef{Dir: "/ckpt/qoe/savepoint-1", CheckpointID: 41},
	})
	if err != nil || id != 7 {
		t.Fatalf("submitSQL = %d, %v", id, err)
	}
	if got := f.modes(); len(got) != 2 || got[0] != "compile" || got[1] != "submit" {
		t.Fatalf("modes = %v", got)
	}
	q := f.requests[1].URL.Query()
	want := map[string]string{
		"catalog": "own", "name": "qoe", "parallelism": "2",
		"checkpoint_dir": "/ckpt/qoe", "checkpoint_interval_ms": "2000",
		"restore_from_dir": "/ckpt/qoe/savepoint-1", "restore_from_checkpoint_id": "41",
	}
	for k, v := range want {
		if q.Get(k) != v {
			t.Errorf("%s = %q, want %q", k, q.Get(k), v)
		}
	}
	if f.bodies[1] != "CREATE TABLE a (x INT) WITH (connector='datagen'); INSERT INTO b SELECT x FROM a;" {
		t.Errorf("body = %q", f.bodies[1])
	}
}

func TestAScriptOfTwoJobsIsRefusedBeforeEitherStarts(t *testing.T) {
	f := &fakeCoordinator{answer: func(r *http.Request, _ string) (int, string) {
		return 200, `{"ok":true,"mode":"compile","specs":[{"name":"x"},{"name":"x_1"}]}`
	}}
	_, err := f.client(t, "").submitSQL(context.Background(), sqlSubmission{Script: "...", Name: "x"})
	if err == nil || !strings.Contains(err.Error(), "makes 2 jobs") {
		t.Fatalf("err = %v", err)
	}
	if got := f.modes(); len(got) != 1 || got[0] != "compile" {
		t.Fatalf("modes = %v: a submit was sent", got)
	}
}

func TestTheCoordinatorsRefusalIsReportedInItsWords(t *testing.T) {
	f := &fakeCoordinator{answer: func(*http.Request, string) (int, string) {
		return 400, `{"ok":false,"error":"compile error: no table named plays"}`
	}}
	_, err := f.client(t, "").submitSQL(context.Background(), sqlSubmission{Script: "...", Name: "x"})
	if err == nil || !strings.Contains(err.Error(), "no table named plays") {
		t.Fatalf("err = %v", err)
	}
}

func TestASavepointCancelsTheJobAtIt(t *testing.T) {
	f := &fakeCoordinator{answer: func(r *http.Request, _ string) (int, string) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/jobs/3/savepoint" || r.URL.Query().Get("cancel") != "true" {
			return 404, ""
		}
		return 200, `{"job_id":3,"ok":true,"checkpoint_id":12,"checkpoint_dir":"/ckpt/qoe/_jobs/3","message":"savepoint complete","cancelled":true,"cancel_message":"cancel requested"}`
	}}
	sp, cancelled, err := f.client(t, "").savepointAndCancel(context.Background(), 3)
	if err != nil || sp.Dir != "/ckpt/qoe/_jobs/3" || sp.CheckpointID != 12 || !cancelled {
		t.Fatalf("savepointAndCancel = %+v, %v, %v", sp, cancelled, err)
	}
}

// A Coordinator that predates ?cancel=true takes the savepoint and leaves the
// job running, and says nothing of a cancel: the caller cancels.
func TestAnOlderCoordinatorTakesTheSavepointAlone(t *testing.T) {
	f := &fakeCoordinator{answer: func(*http.Request, string) (int, string) {
		return 200, `{"job_id":3,"ok":true,"checkpoint_id":12,"checkpoint_dir":"/ckpt/qoe/_jobs/3","message":"savepoint complete"}`
	}}
	_, cancelled, err := f.client(t, "").savepointAndCancel(context.Background(), 3)
	if err != nil || cancelled {
		t.Fatalf("cancelled = %v, %v", cancelled, err)
	}
}

func TestAnEndedJobsLastCheckpointIsInItsMarkerDirectory(t *testing.T) {
	f := &fakeCoordinator{answer: func(r *http.Request, _ string) (int, string) {
		if r.URL.Path == "/api/v1/jobs/5" {
			return 200, `{"id":5,"checkpoint_dir":"/ckpt/edges/","latest_completed_checkpoint_id":18}`
		}
		return 200, `{"id":6,"checkpoint_dir":"/ckpt/edges","latest_completed_checkpoint_id":0}`
	}}
	sp, err := f.client(t, "").lastCheckpoint(context.Background(), 5)
	if err != nil || sp == nil || sp.Dir != "/ckpt/edges/_jobs/5" || sp.CheckpointID != 18 {
		t.Fatalf("lastCheckpoint = %+v, %v", sp, err)
	}
	if sp, err := f.client(t, "").lastCheckpoint(context.Background(), 6); err != nil || sp != nil {
		t.Fatalf("a job with no completed checkpoint: %+v, %v", sp, err)
	}
}

func TestASavepointNotTakenSaysWhy(t *testing.T) {
	f := &fakeCoordinator{answer: func(*http.Request, string) (int, string) {
		return 409, `{"job_id":3,"ok":false,"checkpoint_id":0,"checkpoint_dir":"","message":"job has no checkpoint dir"}`
	}}
	_, _, err := f.client(t, "").savepointAndCancel(context.Background(), 3)
	if err == nil || !strings.Contains(err.Error(), "job has no checkpoint dir") {
		t.Fatalf("err = %v", err)
	}
}

func TestCancellingAJobAlreadyGoneOrEndingIsNotAnError(t *testing.T) {
	for status, wantErr := range map[int]bool{200: false, 404: false, 409: false, 500: true} {
		f := &fakeCoordinator{answer: func(*http.Request, string) (int, string) { return status, `{}` }}
		if err := f.client(t, "").cancel(context.Background(), 9); (err != nil) != wantErr {
			t.Errorf("HTTP %d: err = %v", status, err)
		}
	}
}

func TestALostWorkerIsNotCounted(t *testing.T) {
	f := &fakeCoordinator{answer: func(*http.Request, string) (int, string) {
		return 200, `{"workers":[{"worker_id":"a","lost":false},{"worker_id":"b","lost":true},{"worker_id":"c","lost":false}]}`
	}}
	n, err := f.client(t, "").liveWorkers(context.Background())
	if err != nil || n != 2 {
		t.Fatalf("liveWorkers = %d, %v", n, err)
	}
}

func authCluster(secret, key string) *clinkv1alpha1.ClinkCluster {
	return &clinkv1alpha1.ClinkCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec:       clinkv1alpha1.ClinkClusterSpec{Auth: clinkv1alpha1.AuthSpec{ExistingSecret: secret, SecretKey: key}},
	}
}

func TestTheTokenIsReadWithoutTheWhitespaceAroundIt(t *testing.T) {
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "clink-auth", Namespace: "ns"},
		Data:       map[string][]byte{"token": []byte("abc123\n"), "other": []byte("  "), "api": []byte("xyz")},
	}
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	reader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sec).Build()

	if got, err := clusterToken(context.Background(), reader, authCluster("clink-auth", "")); err != nil || got != "abc123" {
		t.Fatalf("default key: %q, %v", got, err)
	}
	if got, err := clusterToken(context.Background(), reader, authCluster("clink-auth", "api")); err != nil || got != "xyz" {
		t.Fatalf("named key: %q, %v", got, err)
	}
	if got, err := clusterToken(context.Background(), reader, authCluster("", "")); err != nil || got != "" {
		t.Fatalf("no auth: %q, %v", got, err)
	}
	for _, key := range []string{"missing", "other"} {
		if _, err := clusterToken(context.Background(), reader, authCluster("clink-auth", key)); err == nil {
			t.Errorf("key %q: no error", key)
		}
	}
	if _, err := clusterToken(context.Background(), reader, authCluster("no-such-secret", "")); err == nil {
		t.Error("a missing Secret read as no token")
	}
}

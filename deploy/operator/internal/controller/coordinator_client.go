package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clinkv1alpha1 "github.com/clink/clink-operator/api/v1alpha1"
)

// How long the operator waits on the Coordinator: a read answers at once; a
// submission can wait for free slots, and a savepoint for its checkpoint.
const (
	coordinatorReadTimeout   = 5 * time.Second
	coordinatorActionTimeout = 90 * time.Second
)

// clusterToken is the cluster's HTTP API token, read from its auth Secret
// without the whitespace around it, as clink reads it; empty when the cluster
// asks for none. The reader is the API server itself, not the manager's cache,
// so the operator gets the one Secret it names and never lists or watches the
// namespace's Secrets.
func clusterToken(ctx context.Context, reader client.Reader, cc *clinkv1alpha1.ClinkCluster) (string, error) {
	name := cc.Spec.Auth.ExistingSecret
	if name == "" {
		return "", nil
	}
	var sec corev1.Secret
	if err := reader.Get(ctx, types.NamespacedName{Namespace: cc.Namespace, Name: name}, &sec); err != nil {
		return "", fmt.Errorf("auth secret %s: %w", name, err)
	}
	v, ok := sec.Data[authSecretKey(cc)]
	if !ok {
		return "", fmt.Errorf("auth secret %s has no key %q", name, authSecretKey(cc))
	}
	token := strings.TrimSpace(string(v))
	if token == "" {
		return "", fmt.Errorf("auth secret %s: key %q is empty", name, authSecretKey(cc))
	}
	return token, nil
}

// coordinatorClient speaks a ClinkCluster's HTTP API through the Coordinator
// Service, which any Coordinator answers (a standby forwards the request to
// the leader), presenting the cluster's token.
type coordinatorClient struct {
	base  string
	token string
	http  *http.Client
}

func newCoordinatorClient(ctx context.Context, reader client.Reader, cc *clinkv1alpha1.ClinkCluster) (*coordinatorClient, error) {
	token, err := clusterToken(ctx, reader, cc)
	if err != nil {
		return nil, err
	}
	return &coordinatorClient{
		base:  fmt.Sprintf("http://%s-coordinator.%s.svc:%d", cc.Name, cc.Namespace, cc.Spec.Coordinator.HTTPPort),
		token: token,
		http:  &http.Client{},
	}, nil
}

// do sends one request, bounded by timeout, and returns the status and up to
// 4 MiB of the body.
func (c *coordinatorClient) do(ctx context.Context, timeout time.Duration, method, path, contentType string, body io.Reader) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return 0, nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return resp.StatusCode, out, err
}

// get reads a JSON document into out.
func (c *coordinatorClient) get(ctx context.Context, path string, out any) error {
	status, body, err := c.do(ctx, coordinatorReadTimeout, http.MethodGet, path, "", nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("GET %s: HTTP %d: %s", path, status, firstLine(string(body)))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("GET %s: %w", path, err)
	}
	return nil
}

// jobSummaryJSON is one entry of GET /api/v1/jobs.
type jobSummaryJSON struct {
	ID                  int64  `json:"id"`
	Name                string `json:"name"`
	Status              string `json:"status"`
	CompletedCount      int64  `json:"completed_count"`
	CompletionSignalled bool   `json:"completion_signalled"`
	CancelRequested     bool   `json:"cancel_requested"`
	ErrorCount          int64  `json:"error_count"`
}

// live says whether the job is still running. A running job can carry errors
// from tasks the Coordinator restarted, so errors alone do not end it: the
// Coordinator's status does, and a Coordinator too old to report one is read
// from the flags it does report.
func (j jobSummaryJSON) live() bool {
	if j.Status != "" {
		return j.Status == "RUNNING"
	}
	return !j.CompletionSignalled && !j.CancelRequested && j.ErrorCount == 0
}

func (c *coordinatorClient) listJobs(ctx context.Context) ([]jobSummaryJSON, error) {
	var wrapper struct {
		Jobs []jobSummaryJSON `json:"jobs"`
	}
	if err := c.get(ctx, "/api/v1/jobs", &wrapper); err != nil {
		return nil, err
	}
	return wrapper.Jobs, nil
}

// liveWorkers counts the registered Workers the Coordinator has not lost.
func (c *coordinatorClient) liveWorkers(ctx context.Context) (int32, error) {
	var wrapper struct {
		Workers []struct {
			Lost bool `json:"lost"`
		} `json:"workers"`
	}
	if err := c.get(ctx, "/api/v1/workers", &wrapper); err != nil {
		return 0, err
	}
	var n int32
	for _, w := range wrapper.Workers {
		if !w.Lost {
			n++
		}
	}
	return n, nil
}

// recordsIn sums the job's per-operator records_in; false when the Coordinator
// cannot say.
func (c *coordinatorClient) recordsIn(ctx context.Context, id int64) (int64, bool) {
	var wrapper struct {
		Operators []struct {
			RecordsIn int64 `json:"records_in"`
		} `json:"operators"`
	}
	if c.get(ctx, fmt.Sprintf("/api/v1/jobs/%d/operators", id), &wrapper) != nil || wrapper.Operators == nil {
		return 0, false
	}
	var sum int64
	for _, o := range wrapper.Operators {
		sum += o.RecordsIn
	}
	return sum, true
}

// sqlSubmission is a SQL ClinkJob's request: the script, compiled by the
// Coordinator in a catalog of its own (catalog=own), so the same script can be
// sent again on an upgrade and two jobs may declare the same table.
type sqlSubmission struct {
	Script               string
	Name                 string
	Parallelism          int32
	CheckpointDir        string
	CheckpointIntervalMs int64
	Restore              *clinkv1alpha1.SavepointRef
}

func (s sqlSubmission) path(mode string) string {
	q := url.Values{}
	q.Set("mode", mode)
	q.Set("catalog", "own")
	q.Set("name", s.Name)
	if s.Parallelism > 0 {
		q.Set("parallelism", fmt.Sprint(s.Parallelism))
	}
	if s.CheckpointDir != "" {
		q.Set("checkpoint_dir", s.CheckpointDir)
		q.Set("checkpoint_interval_ms", fmt.Sprint(s.CheckpointIntervalMs))
	}
	if s.Restore != nil && s.Restore.Dir != "" {
		q.Set("restore_from_dir", s.Restore.Dir)
		q.Set("restore_from_checkpoint_id", fmt.Sprint(s.Restore.CheckpointID))
	}
	return "/api/v1/jobs/sql?" + q.Encode()
}

// sqlReply is what POST /api/v1/jobs/sql answers: the specs a compile made, or
// the jobs a submit started, or why it refused the script.
type sqlReply struct {
	OK    bool   `json:"ok"`
	Error string `json:"error"`
	Specs []struct {
		Name string `json:"name"`
	} `json:"specs"`
	Jobs []struct {
		Name  string `json:"name"`
		JobID int64  `json:"job_id"`
	} `json:"jobs"`
}

func (c *coordinatorClient) postSQL(ctx context.Context, s sqlSubmission, mode string) (sqlReply, error) {
	var reply sqlReply
	status, body, err := c.do(ctx, coordinatorActionTimeout, http.MethodPost, s.path(mode), "text/plain", strings.NewReader(s.Script))
	if err != nil {
		return reply, err
	}
	_ = json.Unmarshal(body, &reply)
	if status != http.StatusOK || !reply.OK {
		if reply.Error != "" {
			return reply, fmt.Errorf("the Coordinator refused the script: %s", reply.Error)
		}
		return reply, fmt.Errorf("POST /api/v1/jobs/sql: HTTP %d: %s", status, firstLine(string(body)))
	}
	return reply, nil
}

// submitSQL starts the script's job and returns its id. A SQL ClinkJob is one
// job, one INSERT INTO with the tables it reads and writes, so the script is
// compiled first and sent to run only when it makes exactly one: a script of
// two would otherwise start both before the operator could refuse it.
func (c *coordinatorClient) submitSQL(ctx context.Context, s sqlSubmission) (int64, error) {
	compiled, err := c.postSQL(ctx, s, "compile")
	if err != nil {
		return 0, err
	}
	if len(compiled.Specs) != 1 {
		return 0, fmt.Errorf("the script makes %d jobs; a SQL ClinkJob is one INSERT INTO", len(compiled.Specs))
	}
	started, err := c.postSQL(ctx, s, "submit")
	if err != nil {
		return 0, err
	}
	if len(started.Jobs) != 1 {
		for _, j := range started.Jobs {
			_ = c.cancel(ctx, j.JobID)
		}
		return 0, fmt.Errorf("the script started %d jobs, cancelled; a SQL ClinkJob is one INSERT INTO", len(started.Jobs))
	}
	return started.Jobs[0].JobID, nil
}

// savepoint takes a savepoint of the job and returns where it is.
func (c *coordinatorClient) savepoint(ctx context.Context, id int64) (*clinkv1alpha1.SavepointRef, error) {
	status, body, err := c.do(ctx, coordinatorActionTimeout, http.MethodPost, fmt.Sprintf("/api/v1/jobs/%d/savepoint", id), "", nil)
	if err != nil {
		return nil, err
	}
	var ack struct {
		OK            bool   `json:"ok"`
		CheckpointID  int64  `json:"checkpoint_id"`
		CheckpointDir string `json:"checkpoint_dir"`
		Message       string `json:"message"`
	}
	_ = json.Unmarshal(body, &ack)
	if status != http.StatusOK || !ack.OK {
		detail := ack.Message
		if detail == "" {
			detail = firstLine(string(body))
		}
		return nil, fmt.Errorf("savepoint of job %d not taken (HTTP %d): %s", id, status, detail)
	}
	if ack.CheckpointDir == "" {
		return nil, fmt.Errorf("savepoint of job %d named no directory", id)
	}
	return &clinkv1alpha1.SavepointRef{Dir: ack.CheckpointDir, CheckpointID: ack.CheckpointID}, nil
}

// cancel ends the job. A job already gone (404) or already ending (409) is
// what cancelling it would have made it, so neither is an error.
func (c *coordinatorClient) cancel(ctx context.Context, id int64) error {
	status, body, err := c.do(ctx, coordinatorActionTimeout, http.MethodPost, fmt.Sprintf("/api/v1/jobs/%d/cancel", id), "", nil)
	if err != nil {
		return err
	}
	switch status {
	case http.StatusOK, http.StatusNotFound, http.StatusConflict:
		return nil
	}
	return fmt.Errorf("cancel of job %d: HTTP %d: %s", id, status, firstLine(string(body)))
}

// coordinatorRole is what a Coordinator's /readyz says it is, at its own
// address (host:port): "leader" or "standby" under HA, "ok" without it, or ""
// when it is not ready. The probe answers without the token.
func coordinatorRole(ctx context.Context, addr string) string {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/readyz", nil)
	if err != nil {
		return ""
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64))
	if resp.StatusCode != http.StatusOK {
		return ""
	}
	return strings.TrimSpace(string(body))
}

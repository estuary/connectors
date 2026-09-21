package connector

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
)

// sparkClient submits PySpark jobs to a long-lived daemon running in the
// docker-compose stack. It is the test-only counterpart of emrClient.
//
// Unlike EMR Serverless, where each job is a fresh `spark-submit` that pays
// JVM startup + Iceberg classpath + Polaris OAuth costs, the daemon holds a
// single SparkSession across submissions. Each runJob call is an HTTP POST
// rather than a process spawn.
type sparkClient struct {
	cfg        sparkConfig
	httpClient *http.Client
}

func (s *sparkClient) checkPrereqs(ctx context.Context, errs *cerrors.PrereqErr) {
	url := strings.TrimSuffix(s.cfg.DaemonURL, "/") + "/health"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		errs.Err(fmt.Errorf("building daemon health request: %w", err))
		return
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		errs.Err(fmt.Errorf("spark daemon at %s is unreachable: %w", s.cfg.DaemonURL, err))
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		errs.Err(fmt.Errorf("spark daemon at %s returned %d: %s", s.cfg.DaemonURL, resp.StatusCode, string(body)))
	}
}

// ensureSecret is a no-op for the Spark Standalone backend: the daemon already
// has the catalog credential baked into its SparkSession (read from
// /shared/polaris-creds.json at container start), and there is no out-of-band
// secret store like SSM to write to.
func (s *sparkClient) ensureSecret(ctx context.Context, wantCred string) error {
	return nil
}

// runJob submits a single load/merge/exec job to the daemon and waits for the
// response. The choice of action is derived from the entryPointURI's basename
// (load.py, merge.py, exec.py); pyFilesCommonURI and workingPrefix are part
// of the EMR contract but unused here — the daemon imports the python modules
// directly and returns status in the HTTP response body.
func (s *sparkClient) runJob(ctx context.Context, job computeJob) error {
	action, err := actionFromEntryPoint(job.EntryPointURI)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{"job": job.Name, "action": action}).Debug("submitting spark job to daemon")

	var result daemonResult
	if err := s.postDaemon(ctx, action, job.Input, &result); err != nil {
		return fmt.Errorf("spark job %q: %w", job.Name, err)
	}
	return nil
}

// daemonResult is the status every daemon action reports. Actions that return
// data embed it in their own result type.
type daemonResult struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
}

func (r *daemonResult) status() (bool, string) { return r.Success, r.Error }

type daemonResponse interface {
	status() (success bool, errMsg string)
}

// postDaemon submits an action to the daemon and decodes its response into
// result, returning the daemon's error when the action did not succeed.
func (s *sparkClient) postDaemon(ctx context.Context, action string, input any, result daemonResponse) error {
	body, err := json.Marshal(struct {
		Action string `json:"action"`
		Input  any    `json:"input"`
	}{Action: action, Input: input})
	if err != nil {
		return fmt.Errorf("encoding daemon %s request: %w", action, err)
	}

	url := strings.TrimSuffix(s.cfg.DaemonURL, "/") + "/run"
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building daemon %s request: %w", action, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("daemon %s request failed: %w", action, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading daemon %s response: %w", action, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("daemon returned %d for %s: %s", resp.StatusCode, action, string(respBody))
	}

	if err := json.Unmarshal(respBody, result); err != nil {
		return fmt.Errorf("decoding daemon %s response: %w (body: %s)", action, err, string(respBody))
	}
	if ok, errMsg := result.status(); !ok {
		return fmt.Errorf("%s failed: %s", action, errMsg)
	}

	return nil
}

// rowQuerier is implemented by compute backends that can return the rows of
// a query. Only the test daemon does; EMR jobs report success or failure.
type rowQuerier interface {
	queryRows(ctx context.Context, query string) (columns []string, rows [][]any, _ error)
}

// queryRows runs a query through the daemon's "query" action. Each value comes
// back as the JSON Spark produces for it and is kept as raw JSON, so numbers
// and nested documents are rendered exactly as Spark returned them.
func (s *sparkClient) queryRows(ctx context.Context, query string) ([]string, [][]any, error) {
	var result struct {
		daemonResult
		Columns []string                     `json:"columns"`
		Rows    []map[string]json.RawMessage `json:"rows"`
	}
	input := struct {
		Query string `json:"query"`
	}{Query: query}
	if err := s.postDaemon(ctx, "query", input, &result); err != nil {
		return nil, nil, err
	}

	rows := make([][]any, 0, len(result.Rows))
	for _, r := range result.Rows {
		row := make([]any, len(result.Columns))
		for i, c := range result.Columns {
			if v, ok := r[c]; ok {
				row[i] = v
			}
		}
		rows = append(rows, row)
	}

	return result.Columns, rows, nil
}

// actionFromEntryPoint maps a python script URI (or local path) to the daemon
// action name. The connector's putPyFiles emits stable basenames for each
// pipeline stage, so a basename match is sufficient.
func actionFromEntryPoint(entryPointURI string) (string, error) {
	switch path.Base(entryPointURI) {
	case "load.py":
		return "load", nil
	case "merge.py":
		return "merge", nil
	case "exec.py":
		return "exec", nil
	default:
		return "", fmt.Errorf("no daemon action for entry point %q", entryPointURI)
	}
}

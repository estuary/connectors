package connector

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"

	"github.com/estuary/connectors/go/blob"
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
	cfg                 sparkConfig
	emrCfg              emrConfig
	catalogAuth         catalogAuthConfig
	catalogURL          string
	warehouse           string
	materializationName string
	bucket              blob.Bucket
	tokenURL            string
	httpClient          *http.Client
}

func (s *sparkClient) checkPrereqs(ctx context.Context, errs *cerrors.PrereqErr) {
	url := strings.TrimSuffix(s.cfg.DaemonURL, "/") + "/health"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		errs.Err(fmt.Errorf("building daemon health request: %w", err))
		return
	}
	resp, err := s.client().Do(req)
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
func (s *sparkClient) runJob(ctx context.Context, input any, entryPointURI, _ string, jobName, _ string) error {
	action, err := actionFromEntryPoint(entryPointURI)
	if err != nil {
		return err
	}

	body, err := json.Marshal(struct {
		Action string `json:"action"`
		Input  any    `json:"input"`
	}{
		Action: action,
		Input:  input,
	})
	if err != nil {
		return fmt.Errorf("encoding daemon request for %q: %w", jobName, err)
	}

	url := strings.TrimSuffix(s.cfg.DaemonURL, "/") + "/run"
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building daemon request for %q: %w", jobName, err)
	}
	req.Header.Set("Content-Type", "application/json")

	log.WithFields(log.Fields{"job": jobName, "action": action}).Debug("submitting spark job to daemon")

	resp, err := s.client().Do(req)
	if err != nil {
		return fmt.Errorf("spark job %q request failed: %w", jobName, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading daemon response for %q: %w", jobName, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("spark job %q daemon returned %d: %s", jobName, resp.StatusCode, string(respBody))
	}

	var result struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("decoding daemon response for %q: %w (body: %s)", jobName, err, string(respBody))
	}
	if !result.Success {
		return fmt.Errorf("spark job %q failed: %s", jobName, result.Error)
	}

	return nil
}

func (s *sparkClient) client() *http.Client {
	if s.httpClient != nil {
		return s.httpClient
	}
	// No timeout; jobs can run for tens of seconds and the connector's own
	// context already carries the cancellation signal we care about.
	s.httpClient = &http.Client{Timeout: 0}
	return s.httpClient
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

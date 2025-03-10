package dbt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	log "github.com/sirupsen/logrus"
)

type JobConfig struct {
	JobID         string `json:"job_id" jsonschema:"title=Job ID,description=dbt job ID"`
	AccountID     string `json:"account_id" jsonschema:"title=Account ID,description=dbt account ID"`
	AccessURL     string `json:"access_url,omitempty" jsonschema:"title=Access URL,description=dbt access URL can be found in your Account Settings. See go.estuary.dev/dbt-cloud-trigger" jsonschema_extras:"pattern=^https://.+$"`
	AccountPrefix string `json:"account_prefix" jsonschema:"-"`
	APIKey        string `json:"api_key" jsonschema:"title=API Key,description=dbt API Key" jsonschema_extras:"secret=true"`
	Cause         string `json:"cause,omitempty" jsonschema:"title=Cause Message,description=You can set a custom 'cause' message for the job trigger. Defaults to 'Estuary Flow'."`
	Mode          string `json:"mode,omitempty" jsonschema:"title=Job Trigger Mode,description=Specifies how should already-running jobs be treated. Defaults to 'skip' which skips the trigger if a job is already running; 'replace' cancels the running job and runs a new one; while 'ignore' triggers a new job regardless of existing jobs.,enum=skip,enum=replace,enum=ignore,default=skip"`
	Interval      string `json:"interval,omitempty" jsonschema:"title=Minimum Run Interval,description=Minimum time between dbt job triggers. This interval is only triggered if data has been materialized by your task.,default=30m"`
}

func (c *JobConfig) Validate() error {
	if !c.Enabled() {
		return nil
	}

	var requiredProperties = [][]string{
		{"job_id", c.JobID},
		{"account_id", c.AccountID},
		{"api_key", c.APIKey},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("dbt job trigger missing '%s'", req[0])
		}
	}

	if c.AccountPrefix == "" && c.AccessURL == "" {
		return fmt.Errorf("dbt job trigger missing access_url")
	}

	return nil
}

const (
	ModeSkip    = "skip"
	ModeReplace = "replace"
	ModeIgnore  = "ignore"
)

func (c *JobConfig) Enabled() bool {
	return c.JobID != "" && c.AccountID != "" && (c.AccountPrefix != "" || c.AccessURL != "") && c.APIKey != ""
}

// See https://docs.getdbt.com/dbt-cloud/api-v2 for more details
// on different hostnames
func (config *JobConfig) accessURL() string {
	if config.AccessURL != "" {
		return strings.TrimRight(config.AccessURL, "/")
	}

	var host = fmt.Sprintf("https://%s.us1.dbt.com", config.AccountPrefix)

	return host
}

const contentType = "application/json;charset=UTF-8"
const userAgent = "Estuary Technologies Flow"

type ResponseStatus struct {
	Code             int    `json:"code"`
	IsSuccess        bool   `json:"is_success"`
	UserMessage      string `json:"user_message"`
	DeveloperMessage string `json:"developer_message"`
}

type Response struct {
	Status ResponseStatus  `json:"status"`
	Data   json.RawMessage `json:"data"`
}

func JobTrigger(config JobConfig) error {
	var mode = config.Mode
	if mode == "" {
		mode = ModeSkip
	}

	if mode != ModeIgnore {
		var runs, err = CurrentRuns(config)
		if err != nil {
			return fmt.Errorf("current runs: %w", err)
		}

		if mode == ModeSkip && len(runs) > 0 {
			log.WithFields(log.Fields{
				"runs": fmt.Sprintf("%+v", runs),
			}).Debug("skipping dbt job trigger since there are runs in progress for this job")
			return nil
		} else if mode == ModeReplace {
			// On replace mode we cancel current runs and continue with triggering a new run
			log.WithFields(log.Fields{
				"runs": fmt.Sprintf("%+v", runs),
			}).Debug("cancelling runs in progress before triggering the job")
			for _, run := range runs {
				if err := CancelRun(config, run.ID); err != nil {
					return fmt.Errorf("cancelling run %d: %w", run.ID, err)
				}
			}
		}
	}

	var cause = config.Cause
	if cause == "" {
		cause = "Estuary Flow"
	}
	var url = fmt.Sprintf("%s/api/v2/accounts/%s/jobs/%s/run", config.accessURL(), config.AccountID, config.JobID)
	var reqBodyJson = fmt.Sprintf(`{"cause": "%s"}`, cause)
	var response, err = req(config, "POST", url, bytes.NewReader([]byte(reqBodyJson)))
	if err != nil {
		return fmt.Errorf("run request: %w", err)
	}

	if !response.Status.IsSuccess {
		return fmt.Errorf("%s", response.Status.UserMessage)
	}

	return nil
}

const (
	RunStatusError     = 20
	RunStatusCancelled = 30
	RunStatusRunning   = 3
)

type RunResponseData struct {
	ID        int    `json:"id"`
	StartedAt string `json:"started_at"`
	Status    int    `json:"status"`
}

func CurrentRuns(config JobConfig) ([]RunResponseData, error) {
	var url = fmt.Sprintf("%s/api/v2/accounts/%s/runs?job_definition_id=%s&status=%d", config.accessURL(), config.AccountID, config.JobID, RunStatusRunning)
	var response, err = req(config, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	if !response.Status.IsSuccess {
		return nil, fmt.Errorf("%s", response.Status.UserMessage)
	}

	var runs []RunResponseData
	if err := json.Unmarshal(response.Data, &runs); err != nil {
		return nil, fmt.Errorf("parsing response data: %w", err)
	}

	return runs, nil
}

func CancelRun(config JobConfig, runId int) error {
	var url = fmt.Sprintf("%s/api/v2/accounts/%s/runs/%d/cancel", config.accessURL(), config.AccountID, runId)
	var response, err = req(config, "POST", url, nil)
	if err != nil {
		return err
	}

	if !response.Status.IsSuccess {
		return fmt.Errorf("%s", response.Status.UserMessage)
	}

	return nil
}

func req(config JobConfig, method, url string, body io.Reader) (*Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", config.APIKey))
	req.Header.Add("Content-Type", contentType)
	req.Header.Add("Accept", contentType)
	req.Header.Add("User-Agent", userAgent)

	var httpClient = http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}

	var respBuf = new(strings.Builder)
	_, err = io.Copy(respBuf, resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	var respBody = respBuf.String()

	log.WithFields(log.Fields{
		"url":        url,
		"resp":       respBody,
		"respStatus": resp.StatusCode,
	}).Debug("dbt request")

	var response Response
	if err := json.Unmarshal([]byte(respBody), &response); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	return &response, nil
}

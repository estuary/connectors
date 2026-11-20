package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	maxWriteAttempts = 5
	maxRetryDelay    = 30 * time.Second
)

// InfluxDB writes the rest of a batch when it drops points older than the
// bucket's retention period, and reports the drop as HTTP 422.
var retentionDropped = regexp.MustCompile(`(?:^|: )partial write: points beyond retention policy dropped=(\d+)$`)

type apiError struct {
	status  int
	message string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.status, e.message)
}

type client struct {
	baseURL    *url.URL
	config     config
	httpClient *http.Client
}

func (c config) client(httpClient *http.Client) (*client, error) {
	u, err := url.Parse(c.URL)
	if err != nil {
		return nil, fmt.Errorf("parsing InfluxDB URL: %w", err)
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &client{baseURL: u, config: c, httpClient: httpClient}, nil
}

func (c *client) checkBucket(ctx context.Context) error {
	req, err := c.newRequest(ctx, http.MethodGet, "buckets", "", nil, url.Values{"org": {c.config.Org}, "name": {c.config.Bucket}})
	if err != nil {
		return fmt.Errorf("creating bucket request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("checking InfluxDB bucket: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("reading bucket response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		message := responseMessage(body)
		if resp.StatusCode == http.StatusUnauthorized {
			return fmt.Errorf("InfluxDB authentication failed (HTTP 401): check your API token: %s", message)
		}
		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("checking InfluxDB bucket: HTTP 404: %s; the bucket must already exist in organization %q", message, c.config.Org)
		}
		return fmt.Errorf("checking InfluxDB bucket: HTTP %d: %s", resp.StatusCode, message)
	}

	var result struct {
		Buckets []struct {
			Name string `json:"name"`
		} `json:"buckets"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("decoding bucket response: %w", err)
	}
	for _, bucket := range result.Buckets {
		if bucket.Name == c.config.Bucket {
			return nil
		}
	}
	return fmt.Errorf("bucket %q was not found in organization %q; the bucket must already exist", c.config.Bucket, c.config.Org)
}

// writePoints returns the number of points InfluxDB dropped for being older
// than the bucket's retention period.
func (c *client) writePoints(ctx context.Context, points string) (int, error) {
	err := c.post(ctx, "write", "text/plain; charset=utf-8", points, url.Values{
		"org": {c.config.Org}, "bucket": {c.config.Bucket}, "precision": {"ns"},
	})
	var apiErr *apiError
	if errors.As(err, &apiErr) && apiErr.status == http.StatusUnprocessableEntity && strings.Count(apiErr.message, "partial write") == 1 {
		if match := retentionDropped.FindStringSubmatch(apiErr.message); match != nil {
			if dropped, err := strconv.Atoi(match[1]); err == nil {
				return dropped, nil
			}
		}
	}
	if err != nil {
		return 0, fmt.Errorf("writing InfluxDB points: %w", err)
	}
	return 0, nil
}

func (c *client) deletePoints(ctx context.Context, request deleteRequest) error {
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encoding InfluxDB delete request: %w", err)
	}
	if err := c.post(ctx, "delete", "application/json", string(body), url.Values{
		"org": {c.config.Org}, "bucket": {c.config.Bucket},
	}); err != nil {
		return fmt.Errorf("deleting InfluxDB points: %w", err)
	}
	return nil
}

func (c *client) newRequest(ctx context.Context, method, endpoint, contentType string, body io.Reader, query url.Values) (*http.Request, error) {
	u := *c.baseURL
	u.Path = strings.TrimRight(u.Path, "/") + "/api/v2/" + endpoint
	u.RawPath = ""
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Token "+c.config.Token)
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return req, nil
}

func (c *client) post(ctx context.Context, endpoint, contentType, body string, query url.Values) error {
	for attempt := 0; ; attempt++ {
		req, err := c.newRequest(ctx, http.MethodPost, endpoint, contentType, strings.NewReader(body), query)
		if err != nil {
			return fmt.Errorf("creating request: %w", err)
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return err
		}
		responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("reading response: %w", err)
		}
		if resp.StatusCode == http.StatusNoContent {
			return nil
		}
		if (resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusServiceUnavailable) || attempt+1 == maxWriteAttempts {
			return &apiError{status: resp.StatusCode, message: responseMessage(responseBody)}
		}

		timer := time.NewTimer(retryDelay(resp.Header.Get("Retry-After"), attempt, time.Now()))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func retryDelay(retryAfter string, attempt int, now time.Time) time.Duration {
	if seconds, err := strconv.ParseInt(strings.TrimSpace(retryAfter), 10, 64); err == nil && seconds >= 0 {
		if seconds >= int64(maxRetryDelay/time.Second) {
			return maxRetryDelay
		}
		return time.Duration(seconds) * time.Second
	}
	if deadline, err := http.ParseTime(retryAfter); err == nil {
		return min(maxRetryDelay, max(0, deadline.Sub(now)))
	}
	return min(maxRetryDelay, time.Second<<attempt)
}

func responseMessage(body []byte) string {
	var apiError struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &apiError) == nil && apiError.Message != "" {
		return apiError.Message
	}
	return strings.TrimSpace(string(body))
}

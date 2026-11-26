package connector

import (
	"context"
	"crypto/tls"
	"crypto/x509"
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

	"github.com/estuary/connectors/go/retry"
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
	retry      retry.Policy
}

func (c config) client(httpClient *http.Client) (*client, error) {
	u, err := url.Parse(c.URL)
	if err != nil {
		return nil, fmt.Errorf("parsing InfluxDB URL: %w", err)
	}
	if httpClient == nil {
		tlsConfig, err := c.Advanced.tlsConfig()
		if err != nil {
			return nil, err
		}
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConfig
		httpClient = &http.Client{Timeout: 30 * time.Second, Transport: transport}
	}
	return &client{baseURL: u, config: c, httpClient: httpClient, retry: retry.Policy{
		Classify: func(err error) retry.Outcome {
			var apiErr *apiError
			if errors.As(err, &apiErr) {
				return retry.HTTPStatus(apiErr.status)
			}
			return retry.Network(err)
		},
		// Repeating the same points merges identical values at fixed timestamps;
		// repeating a delete uses the same predicate and time range.
		RetryAmbiguous: true,
		MaxAttempts:    maxWriteAttempts,
		InitialBackoff: time.Second,
		MaxBackoff:     maxRetryDelay,
		Jitter:         0.2,
	}}, nil
}

func (c advancedConfig) tlsConfig() (*tls.Config, error) {
	if c.SSLCACertificate != "" && c.SSLSkipVerify {
		return nil, fmt.Errorf("provide an SSL CA Certificate or skip verification, not both")
	}
	var roots *x509.CertPool
	if c.SSLCACertificate != "" {
		roots, _ = x509.SystemCertPool()
		if roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM([]byte(c.SSLCACertificate)) {
			return nil, fmt.Errorf("'advanced.ssl_ca_certificate' must contain a PEM-encoded CA certificate")
		}
	}
	return &tls.Config{RootCAs: roots, InsecureSkipVerify: c.SSLSkipVerify}, nil
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
	req.Header.Set("Authorization", "Token "+c.config.Credentials.AccessToken)
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return req, nil
}

func (c *client) post(ctx context.Context, endpoint, contentType, body string, query url.Values) error {
	return c.retry.Do(ctx, "InfluxDB "+endpoint, func(ctx context.Context) error {
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
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return fmt.Errorf("reading response: %w", err)
		}
		if resp.StatusCode == http.StatusNoContent {
			return nil
		}
		err = &apiError{status: resp.StatusCode, message: responseMessage(responseBody)}
		if delay, ok := retry.ParseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
			// An overflowing seconds value can wrap to a negative duration.
			if delay < 0 {
				delay = maxRetryDelay
			}
			err = retry.WithDelay(err, min(delay, maxRetryDelay))
		}
		return err
	})
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

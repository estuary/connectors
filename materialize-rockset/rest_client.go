package materialize_rockset

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	headerUserAgent   = "estuary-test"
	headerAccept      = "application/json"
	headerContentType = "application/json"
	baseUrl           = "https://api.rs2.usw2.rockset.com"
	validateEndpoint  = "/v1/orgs/self/users/self"
)

type client struct {
	client      *http.Client
	baseURL     *url.URL
	dumpRequest bool
	apiKey      string
}

var ErrNotFound = errors.New("not found")

type StatusError struct {
	statusCode int
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("unexpected http status: %d", e.statusCode)
}

func (e *StatusError) NotFound() bool {
	return e.statusCode == 404
}

func NewClient(apiKey string, verboseLogging bool) (*client, error) {
	baseURL, err := url.Parse(baseUrl)
	if err != nil {
		return nil, fmt.Errorf("parsing rockset url: %w", err)
	}
	// Create a client
	c := &client{
		client:      &http.Client{},
		baseURL:     baseURL,
		dumpRequest: verboseLogging,
		apiKey:      fmt.Sprintf("ApiKey %s", apiKey),
	}
	// Validate the connection
	err = c.CheckConnection(context.Background())
	if err != nil {
		return nil, err
	}
	return c, nil
}

// makeRequest allows full control of the API request
func (c *client) makeRequest(ctx context.Context, method string, urlStr string, rawBody interface{}, outBody interface{}) (*http.Response, error) {
	// Build the full url
	rel, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	apiURL := c.baseURL.ResolveReference(rel)

	var req *http.Request

	// Setup the reader for the request
	if rawBody != nil {
		body, merr := json.Marshal(rawBody)
		if merr != nil {
			return nil, fmt.Errorf("marshalling request body: %w", err)
		}

		reader := bytes.NewReader(body)
		req, err = http.NewRequest(method, apiURL.String(), reader)
	} else {
		req, err = http.NewRequest(method, apiURL.String(), nil)
	}
	if err != nil {
		return nil, fmt.Errorf("creating http.Request: %w", err)
	}
	req = req.WithContext(ctx)

	// Headers
	req.Header.Add("Accept", headerAccept)
	req.Header.Add("Content-Type", headerContentType)

	if c.dumpRequest {
		requestDump, err := httputil.DumpRequest(req, false)
		if err == nil {
			log.Infof("request: %v", string(requestDump))
		}
	}
	// Add Authorization header after logging the request, to ensure we don't log someone's api key.
	req.Header.Add("Authorization", c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}

	if c.dumpRequest && (resp.StatusCode < 200 || resp.StatusCode >= 300) {
		responseDump, err := httputil.DumpResponse(resp, false)
		if err == nil {
			log.Infof("response: %v", string(responseDump))
		}
	}

	if outBody != nil {
		defer resp.Body.Close()
		err = json.NewDecoder(resp.Body).Decode(outBody)
		if err != nil {
			return nil, fmt.Errorf("decoding response: %w", err)
		}
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &StatusError{statusCode: resp.StatusCode}
	}

	return resp, nil
}

// CheckConnection checks the API client is functioning
func (c *client) CheckConnection(ctx context.Context) error {
	_, err := c.makeRequest(ctx, http.MethodGet, validateEndpoint, nil, nil)
	if err != nil {
		return fmt.Errorf("making request: %w", err)
	}
	return nil
}

type workspaceWrapper struct {
	Data Workspace `json:"data"`
}

type Workspace struct {
	CreatedAt       time.Time `json:"created_at"`
	CreatedBy       string    `json:"created_by"`
	Name            string    `json:"name"`
	Description     string    `json:"description"`
	CollectionCount int       `json:"collection_count"`
}

type CreateWorkspace struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

func (c *client) GetWorkspace(ctx context.Context, name string) (*Workspace, error) {
	var data workspaceWrapper
	url := fmt.Sprintf("/v1/orgs/self/ws/%s", name)
	_, err := c.makeRequest(ctx, http.MethodGet, url, nil, &data)
	if err != nil {
		return nil, err
	}

	return &data.Data, nil
}

// CreateWorkspace
func (c *client) CreateWorkspace(ctx context.Context, workspace *CreateWorkspace) (*Workspace, error) {
	var data workspaceWrapper
	url := "/v1/orgs/self/ws"
	_, err := c.makeRequest(ctx, http.MethodPost, url, workspace, &data)
	if err != nil {
		return nil, err
	}
	return &data.Data, nil
}

func (c *client) DestroyWorkspace(ctx context.Context, workspace string) error {
	url := fmt.Sprintf("/v1/orgs/self/ws/%s", workspace)
	_, err := c.makeRequest(ctx, http.MethodDelete, url, nil, nil)
	return err
}

type collectionWrapper struct {
	Data Collection `json:"data"`
}

type Collection struct {
	CreatedAt   time.Time             `json:"created_at"`
	CreatedBy   string                `json:"created_by"`
	Name        string                `json:"name"`
	Description string                `json:"description"`
	Workspace   string                `json:"workspace"`
	Status      string                `json:"status"`
	Sources     []GetCollectionSource `json:"sources,omitempty"`
}

func (c *Collection) GetCloudStorageSource(integration string) *CloudStorageSource {
	if source := c.GetIntegrationSource(integration); source != nil {
		return source.S3
	}
	return nil
}

func (c *Collection) GetIntegrationSource(integration string) *GetCollectionSource {
	for _, source := range c.Sources {
		if source.IntegrationName == integration {
			return &source
		}
	}
	return nil
}

type CloudStorageSource struct {
	ObjectCountDownloaded int64 `json:"object_count_downloaded"`
	ObjectCountTotal      int64 `json:"object_count_total"`
	ObjectBytesTotal      int64 `json:"object_bytes_total"`
}

type GetCollectionSource struct {
	IntegrationName string              `json:"integration_name"`
	S3              *CloudStorageSource `json:"s3,omitempty"`
}

type S3Integration struct {
	Bucket  string `json:"bucket"`
	Region  string `json:"region,omitempty"`
	Prefix  string `json:"prefix,omitempty"`
	Pattern string `json:"pattern,omitempty"`
}

type CreateCollectionSource struct {
	IntegrationName string         `json:"integration_name"`
	S3              *S3Integration `json:"s3,omitempty"`
}

type CreateCollection struct {
	Name        string                   `json:"name"`
	Description string                   `json:"description"`
	Sources     []CreateCollectionSource `json:"sources,omitempty"`
}

// GetCollection
func (c *client) GetCollection(ctx context.Context, workspace string, collection string) (*Collection, error) {
	var data collectionWrapper
	url := fmt.Sprintf("/v1/orgs/self/ws/%s/collections/%s", workspace, collection)
	_, err := c.makeRequest(ctx, http.MethodGet, url, nil, &data)
	if err != nil {
		return nil, err
	}
	return &data.Data, nil
}

// CreateCollection
func (c *client) CreateCollection(ctx context.Context, workspace string, collection *CreateCollection) (*Collection, error) {
	var data collectionWrapper
	url := fmt.Sprintf("/v1/orgs/self/ws/%s/collections", workspace)
	_, err := c.makeRequest(ctx, http.MethodPost, url, collection, &data)
	if err != nil {
		return nil, err
	}
	return &data.Data, nil
}

func (c *client) DestroyCollection(ctx context.Context, workspace string, collection string) error {
	url := fmt.Sprintf("/v1/orgs/self/ws/%s/collections/%s", workspace, collection)
	_, err := c.makeRequest(ctx, http.MethodDelete, url, nil, nil)
	if err != nil {
		return err
	}

	time.Sleep(time.Second * 2)
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for collection `%s/%s` to be deleted: %w", workspace, collection, err)
		default:
			_, err := c.GetCollection(ctx, workspace, collection)
			if se, ok := err.(*StatusError); ok && se.NotFound() {
				return nil
			} else {
				// Not deleted yet, so wait a couple of seconds then we'll try again.
				time.Sleep(time.Second * 2)
			}
		}
	}
}

type docsWrapper struct {
	Data []json.RawMessage `json:"data"`
}

func (c *client) AddDocuments(ctx context.Context, workspace string, collection string, documents []json.RawMessage) error {
	url := fmt.Sprintf("/v1/orgs/self/ws/%s/collections/%s/docs", workspace, collection)
	_, err := c.makeRequest(ctx, http.MethodPost, url, docsWrapper{Data: documents}, nil)
	if err != nil {
		return err
	}
	return nil
}

func (c *client) DeleteDocuments(ctx context.Context, workspace string, collection string, documents []json.RawMessage) error {
	url := fmt.Sprintf("/v1/orgs/self/ws/%s/collections/%s/docs", workspace, collection)
	_, err := c.makeRequest(ctx, http.MethodDelete, url, docsWrapper{Data: documents}, nil)
	if err != nil {
		return err
	}
	return nil
}

package materialize_rockset

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
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

func NewClient(apiKey string, verboseLogging bool) (*client, error) {
	baseURL, err := url.Parse(baseUrl)
	if err != nil {
		return nil, fmt.Errorf("could not parse rockset url: %w", err)
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
func (c *client) makeRequest(ctx context.Context, method string, urlStr string, body []byte) (*http.Response, error) {
	// Build the full url
	rel, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	apiURL := c.baseURL.ResolveReference(rel)

	var contentLength = len(body)

	var req *http.Request

	// Setup the reader for the request
	if body != nil {
		reader := bytes.NewReader(body)
		req, err = http.NewRequest(method, apiURL.String(), reader)
	} else {
		req, err = http.NewRequest(method, apiURL.String(), nil)
	}
	if err != nil {
		return nil, fmt.Errorf("could not setup request: %v", err)
	}
	req = req.WithContext(ctx)

	// Headers
	req.Header.Add("Accept", headerAccept)
	req.Header.Add("Authorization", c.apiKey)
	req.Header.Add("Content-Length", fmt.Sprintf("%d", contentLength))
	req.Header.Add("Content-Type", headerContentType)
	req.Header.Add("User-Agent", headerUserAgent)

	// Debug dump
	if c.dumpRequest {
		requestDump, err := httputil.DumpRequest(req, false)
		if err == nil {
			log.Printf("request: %v", string(requestDump))
		}
	}

	// Do the request
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request error: %w", err)
	}

	// Debug dump
	if c.dumpRequest && (resp.StatusCode < 200 || resp.StatusCode >= 300) {
		responseDump, err := httputil.DumpResponse(resp, true)
		if err == nil {
			log.Printf("response: %v", string(responseDump))
		}
	}

	return resp, err
}

// CheckConnection checks the API client is functioning
func (c *client) CheckConnection(ctx context.Context) error {
	resp, err := c.makeRequest(ctx, http.MethodGet, validateEndpoint, nil)
	if err != nil {
		return fmt.Errorf("could not connect to rockset: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("could not validate connection to rockset. status: %d", resp.StatusCode)
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
	url := fmt.Sprintf("/v1/orgs/self/ws/%s", name)
	resp, err := c.makeRequest(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("could not make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed status %d", resp.StatusCode)
	}
	var data workspaceWrapper
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return nil, fmt.Errorf("could not decode response: %w", err)
	}
	return &data.Data, nil
}

// CreateWorkspace
func (c *client) CreateWorkspace(ctx context.Context, workspace *CreateWorkspace) (*Workspace, error) {
	body, err := json.Marshal(workspace)
	if err != nil {
		return nil, fmt.Errorf("marshal: %v", err)
	}
	url := "/v1/orgs/self/ws"
	resp, err := c.makeRequest(ctx, http.MethodPost, url, body)
	if err != nil {
		return nil, fmt.Errorf("could not make request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed status %d", resp.StatusCode)
	}
	var data workspaceWrapper
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return nil, fmt.Errorf("could not decode response: %v", err)
	}
	log.Printf("successfully created workspace")
	return &data.Data, nil
}

func (c *client) DestroyWorkspace(ctx context.Context, workspace string) error {
	url := fmt.Sprintf("/v1/orgs/self/ws/%s", workspace)
	resp, err := c.makeRequest(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("could not make request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("request failed status %d", resp.StatusCode)
	}
	return nil
}

type collectionWrapper struct {
	Data Collection `json:"data"`
}

type Collection struct {
	CreatedAt   time.Time `json:"created_at"`
	CreatedBy   string    `json:"created_by"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Workspace   string    `json:"workspace"`
	Status      string    `json:"status"`
}

type CreateCollection struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// GetCollection
func (c *client) GetCollection(ctx context.Context, workspace string, collection string) (*Collection, error) {
	url := fmt.Sprintf("/v1/orgs/self/ws/%s/collections/%s", workspace, collection)
	resp, err := c.makeRequest(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("could not make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	} else if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed status %d", resp.StatusCode)
	}
	var data collectionWrapper
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return nil, fmt.Errorf("could not decode response: %w", err)
	}
	return &data.Data, nil
}

// CreateCollection
func (c *client) CreateCollection(ctx context.Context, workspace string, collection *CreateCollection) (*Collection, error) {
	body, err := json.Marshal(collection)
	if err != nil {
		return nil, fmt.Errorf("marshal: %v", err)
	}
	url := fmt.Sprintf("/v1/orgs/self/ws/%s/collections", workspace)
	resp, err := c.makeRequest(ctx, http.MethodPost, url, body)
	if err != nil {
		return nil, fmt.Errorf("could not make request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request failed status %d", resp.StatusCode)
	}
	var data collectionWrapper
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return nil, fmt.Errorf("could not decode response: %v", err)
	}
	return &data.Data, nil
}

func (c *client) DestroyCollection(ctx context.Context, workspace string, collection string) error {
	url := fmt.Sprintf("/v1/orgs/self/ws/%s/collections/%s", workspace, collection)
	resp, err := c.makeRequest(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("could not make request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("request failed status %d", resp.StatusCode)
	}
	return nil
}

type docsWrapper struct {
	Data []json.RawMessage `json:"data"`
}

func (c *client) AddDocuments(ctx context.Context, workspace string, collection string, documents []json.RawMessage) error {
	body, err := json.Marshal(docsWrapper{Data: documents})
	if err != nil {
		return fmt.Errorf("marshal: %v", err)
	}
	url := fmt.Sprintf("/v1/orgs/self/ws/%s/collections/%s/docs", workspace, collection)
	resp, err := c.makeRequest(ctx, http.MethodPost, url, body)
	if err != nil {
		return fmt.Errorf("could not make request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("request failed status %d", resp.StatusCode)
	}
	return nil
}

func (c *client) DeleteDocuments(ctx context.Context, workspace string, collection string, documents []json.RawMessage) error {
	body, err := json.Marshal(docsWrapper{Data: documents})
	if err != nil {
		return fmt.Errorf("marshal: %v", err)
	}
	url := fmt.Sprintf("/v1/orgs/self/ws/%s/collections/%s/docs", workspace, collection)
	resp, err := c.makeRequest(ctx, http.MethodDelete, url, body)
	if err != nil {
		return fmt.Errorf("could not make request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("request failed status %d", resp.StatusCode)
	}
	return nil
}

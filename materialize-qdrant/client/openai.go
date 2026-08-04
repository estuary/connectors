package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	openAIBaseURL    = "https://api.openai.com/v1"
	maxRetries       = 10
	initialRetryWait = 200 * time.Millisecond
	maxRetryWait     = 60 * time.Second
)

type embeddingsRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embeddingsResponse struct {
	Data []Embedding `json:"data"`
}

type Embedding struct {
	Embedding []float32 `json:"embedding"`
	Index     int       `json:"index"`
}

type OpenAIClient struct {
	httpClient     *http.Client
	embeddingModel string
	apiKey         string
	org            string
}

func NewOpenAIClient(embeddingModel string, org string, apiKey string) *OpenAIClient {
	return &OpenAIClient{
		httpClient:     http.DefaultClient,
		embeddingModel: embeddingModel,
		apiKey:         apiKey,
		org:            org,
	}
}

func (c *OpenAIClient) CreateEmbeddings(ctx context.Context, input []string) ([]Embedding, error) {
	requestBody, err := json.Marshal(embeddingsRequest{Model: c.embeddingModel, Input: input})
	if err != nil {
		return nil, fmt.Errorf("encoding OpenAI embeddings request: %w", err)
	}
	response, err := withRetry(ctx, func() (*http.Response, error) {
		req, err := c.newRequest(ctx, http.MethodPost, "/embeddings", bytes.NewReader(requestBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		return c.httpClient.Do(req)
	})
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, decodeOpenAIError(response)
	}
	var result embeddingsResponse
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding OpenAI embeddings response: %w", err)
	}
	return result.Data, nil
}

func (c *OpenAIClient) VerifyModelExists(ctx context.Context) error {
	req, err := c.newRequest(ctx, http.MethodGet, "/models/"+url.PathEscape(c.embeddingModel), nil)
	if err != nil {
		return err
	}
	response, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("requesting OpenAI model: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("verifying OpenAI embedding model %s: %w", c.embeddingModel, decodeOpenAIError(response))
	}
	return nil
}

func (c *OpenAIClient) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, openAIBaseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	if c.org != "" {
		req.Header.Set("OpenAI-Organization", c.org)
	}
	return req, nil
}

func decodeOpenAIError(res *http.Response) error {
	var body struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err == nil && body.Error.Message != "" {
		return fmt.Errorf("OpenAI request failed (%s): %s", res.Status, body.Error.Message)
	}
	return fmt.Errorf("OpenAI request failed with status %s", res.Status)
}

func withRetry(ctx context.Context, request func() (*http.Response, error)) (*http.Response, error) {
	wait := initialRetryWait
	for attempt := 0; ; attempt++ {
		response, err := request()
		if err != nil {
			return nil, fmt.Errorf("OpenAI request: %w", err)
		}
		if !retryableStatus(response.StatusCode) || attempt == maxRetries {
			return response, nil
		}
		response.Body.Close()

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		if wait < maxRetryWait/2 {
			wait *= 2
		} else {
			wait = maxRetryWait
		}
	}
}

func retryableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500 && status <= 599
}

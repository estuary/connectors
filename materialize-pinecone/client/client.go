package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

type OpenAIEmbeddingsRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type OpenAIEmbeddingsResponse struct {
	Object string      `json:"object"`
	Data   []Embedding `json:"data"`
	Model  string      `json:"model"`
	Usage  usage       `json:"usage"`
}

type Embedding struct {
	Object    string    `json:"object"`
	Embedding []float64 `json:"embedding"`
	Index     int       `json:"index"`
}

type usage struct {
	PromptTokens int `json:"prompt_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

type OpenAiClient struct {
	http           *http.Client
	embeddingModel string
	apiKey         string
	org            string
}

func NewOpenAiClient(embeddingModel string, org string, apiKey string) *OpenAiClient {
	return &OpenAiClient{
		http:           http.DefaultClient,
		embeddingModel: embeddingModel,
		org:            org,
		apiKey:         apiKey,
	}
}

type openAiEmbeddingsError struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

func (c *OpenAiClient) CreateEmbeddings(ctx context.Context, input []string) ([]Embedding, error) {
	res, err := withRetry(ctx, func() (*http.Response, error) {
		body := new(bytes.Buffer)
		if err := json.NewEncoder(body).Encode(&OpenAIEmbeddingsRequest{
			Model: c.embeddingModel,
			Input: input,
		}); err != nil {
			return nil, err
		}

		req, err := http.NewRequestWithContext(ctx, "POST", "https://api.openai.com/v1/embeddings", body)
		if err != nil {
			return nil, err
		}
		req.Header.Set("accept", "application/json")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))
		if c.org != "" {
			req.Header.Set("OpenAI-Organization", c.org)
		}

		return c.http.Do(req)
	})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		var errorBody openAiEmbeddingsError
		if err := json.NewDecoder(res.Body).Decode(&errorBody); err != nil {
			log.WithField("error", err).Warn("could not decode error response body")
		} else if errorBody.Error.Message != "" {
			return nil, fmt.Errorf("creating embeddings failed (%s): %s", res.Status, errorBody.Error.Message)
		} else {
			log.WithField("errorBody", errorBody).Warn("errorBody error message was empty")
		}

		return nil, fmt.Errorf("OpenAiClient CreateEmbeddings unexpected status: %s", res.Status)
	}

	embeddings := OpenAIEmbeddingsResponse{}
	if err := json.NewDecoder(res.Body).Decode(&embeddings); err != nil {
		return nil, err
	}

	return embeddings.Data, nil
}

// This ping method functions as a means to verify connectivity with the OpenAI API via a request to
// the /models endpoint.
func (c *OpenAiClient) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.openai.com/v1/models", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))
	if c.org != "" {
		req.Header.Set("OpenAI-Organization", c.org)
	}

	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("OpenAiClient Ping unexpected status: %s", res.Status)
	}

	return nil
}

type PineconeUpsertRequest struct {
	Vectors   []Vector `json:"vectors"`
	Namespace string   `json:"namespace"`
}

type Vector struct {
	Id       string                 `json:"id"`
	Values   []float64              `json:"values"`
	Metadata map[string]interface{} `json:"metadata"`
}

type PineconeUpsertResponse struct {
	UpsertedCount int `json:"upsertedCount"`
}

type PineconeIndexStatsResponse struct {
	Namespaces    map[string]interface{} `json:"namespaces"`
	Dimension     int                    `json:"dimension"`
	IndexFullness float64                `json:"index_fullness"`
}

type PineconeIndexDescribeResponse struct {
	Database struct {
		MetadataConfig struct {
			Indexed []string `json:"indexed"`
		} `json:"metadata_config"`
	} `json:"database"`
}

type whoamiResponse struct {
	ProjectName string `json:"project_name"`
}

type pineconeUpsertError struct {
	Code    int      `json:"code"`
	Message string   `json:"message"`
	Details []string `json:"details"`
}

type PineconeClient struct {
	http        *http.Client
	index       string
	projectName string
	environment string
	apiKey      string
}

func NewPineconeClient(ctx context.Context, index string, environment string, apiKey string) (*PineconeClient, error) {
	c := &PineconeClient{
		http:        http.DefaultClient,
		index:       index,
		environment: environment,
		apiKey:      apiKey,
	}

	whoami, err := c.whoami(ctx)
	if err != nil {
		return nil, err
	}

	c.projectName = whoami.ProjectName

	return c, nil
}

func (c *PineconeClient) baseUrl() string {
	return fmt.Sprintf("https://%s-%s.svc.%s.pinecone.io", c.index, c.projectName, c.environment)
}

func (c *PineconeClient) DescribeIndexStats(ctx context.Context) (PineconeIndexStatsResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", c.baseUrl()+"/describe_index_stats", nil)
	if err != nil {
		return PineconeIndexStatsResponse{}, err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Api-Key", c.apiKey)

	res, err := c.http.Do(req)
	if err != nil {
		return PineconeIndexStatsResponse{}, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return PineconeIndexStatsResponse{}, fmt.Errorf("PineconeClient DescribeIndexStats unexpected status: %s", res.Status)
	}

	indexResponse := PineconeIndexStatsResponse{}
	if err := json.NewDecoder(res.Body).Decode(&indexResponse); err != nil {
		return PineconeIndexStatsResponse{}, err
	}

	return indexResponse, nil
}

func (c *PineconeClient) DescribeIndex(ctx context.Context) (PineconeIndexDescribeResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("https://controller.%s.pinecone.io/databases/%s", c.environment, c.index), nil)
	if err != nil {
		return PineconeIndexDescribeResponse{}, err
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Api-Key", c.apiKey)

	res, err := c.http.Do(req)
	if err != nil {
		return PineconeIndexDescribeResponse{}, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return PineconeIndexDescribeResponse{}, fmt.Errorf("PineconeClient DescribeIndex unexpected status: %s", res.Status)
	}

	indexResponse := PineconeIndexDescribeResponse{}
	if err := json.NewDecoder(res.Body).Decode(&indexResponse); err != nil {
		return PineconeIndexDescribeResponse{}, err
	}

	return indexResponse, nil
}

func (c *PineconeClient) Upsert(ctx context.Context, req PineconeUpsertRequest) error {
	res, err := withRetry(ctx, func() (*http.Response, error) {
		body := new(bytes.Buffer)
		if err := json.NewEncoder(body).Encode(&req); err != nil {
			return nil, err
		}

		req, err := http.NewRequestWithContext(ctx, "POST", c.baseUrl()+"/vectors/upsert", body)
		if err != nil {
			return nil, err
		}
		req.Header.Set("accept", "application/json")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Api-Key", c.apiKey)

		return c.http.Do(req)
	})
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		var errorBody pineconeUpsertError
		if err := json.NewDecoder(res.Body).Decode(&errorBody); err != nil {
			log.WithField("error", err).Warn("could not decode error response body")
		} else if errorBody.Message != "" {
			return fmt.Errorf("pinecone vector upsert failed (%s): %s", res.Status, errorBody.Message)
		} else {
			log.WithField("errorBody", errorBody).Warn("errorBody error message was empty")
		}

		return fmt.Errorf("PineconeClient Upsert unexpected status: %s", res.Status)
	}

	upsertResponse := PineconeUpsertResponse{}
	if err := json.NewDecoder(res.Body).Decode(&upsertResponse); err != nil {
		return err
	}

	if len(req.Vectors) != upsertResponse.UpsertedCount {
		return fmt.Errorf("upserted unexpected vector count: %d expected vs %d upserted", len(req.Vectors), upsertResponse.UpsertedCount)
	}

	return nil
}

func (c *PineconeClient) whoami(ctx context.Context) (whoamiResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("https://controller.%s.pinecone.io/actions/whoami", c.environment), nil)
	if err != nil {
		return whoamiResponse{}, err
	}
	req.Header.Set("Api-Key", c.apiKey)
	req.Header.Set("accept", "application/json")

	res, err := c.http.Do(req)
	if err != nil {
		return whoamiResponse{}, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return whoamiResponse{}, fmt.Errorf("PineconeClient whoami unexpected status: %s", res.Status)
	}

	whoami := whoamiResponse{}
	if err := json.NewDecoder(res.Body).Decode(&whoami); err != nil {
		return whoamiResponse{}, err
	}

	return whoami, nil
}

var (
	maxRetries             = 10
	initialBackoff float64 = 200 // Milliseconds
	maxBackoff             = time.Duration(60 * time.Second)
	retryableCodes         = []int{http.StatusTooManyRequests, http.StatusInternalServerError}
)

func withRetry(ctx context.Context, fn func() (*http.Response, error)) (*http.Response, error) {
	n := 0
	backoff := initialBackoff

	for {
		n++

		res, err := fn()
		if err != nil {
			return nil, fmt.Errorf("withRetry: %w", err)
		}

		if containsCode(retryableCodes, res.StatusCode) {
			if n > maxRetries {
				log.WithFields(log.Fields{
					"host":       res.Request.URL.Host,
					"path":       res.Request.URL.Path,
					"attempts":   n,
					"lastCode":   res.StatusCode,
					"lastStatus": res.Status,
				}).Warn("exceeded retry limit")
				return res, nil
			}

			res.Body.Close()
			backoff *= math.Pow(2, 1+rand.Float64())
			delay := time.Duration(backoff * float64(time.Millisecond))
			if delay > maxBackoff {
				delay = maxBackoff
			}

			log.WithFields(log.Fields{
				"host":       res.Request.URL.Host,
				"path":       res.Request.URL.Path,
				"attempts":   n,
				"lastCode":   res.StatusCode,
				"lastStatus": res.Status,
				"delay":      delay.String(),
			}).Info("waiting to retry request on retryable error")

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}

		return res, nil
	}
}

func containsCode(src []int, in int) bool {
	for _, c := range src {
		if c == in {
			return true
		}
	}

	return false
}

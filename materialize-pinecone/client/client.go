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
	Embedding []float32 `json:"embedding"`
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

func (c *OpenAiClient) VerifyModelExists(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("https://api.openai.com/v1/models/%s", c.embeddingModel), nil)
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
		var errorBody openAiEmbeddingsError
		if err := json.NewDecoder(res.Body).Decode(&errorBody); err != nil {
			log.WithField("error", err).Warn("could not decode error response body")
		} else if errorBody.Error.Message != "" {
			return fmt.Errorf("could not verify OpenAI embeddings model %s exists (%s): %s", c.embeddingModel, res.Status, errorBody.Error.Message)
		} else {
			log.WithField("errorBody", errorBody).Warn("errorBody error message was empty")
		}

		return fmt.Errorf("OpenAiClient VerifyModelExists unexpected status: %s", res.Status)
	}

	return nil
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

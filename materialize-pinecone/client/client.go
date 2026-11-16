package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

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

// StatusError is returned when OpenAI responds with a non-200 status.
type StatusError struct {
	StatusCode int
	msg        string
}

func (e *StatusError) Error() string { return e.msg }

func (c *OpenAiClient) CreateEmbeddings(ctx context.Context, input []string) ([]Embedding, error) {
	body, err := json.Marshal(&OpenAIEmbeddingsRequest{
		Model: c.embeddingModel,
		Input: input,
	})
	if err != nil {
		return nil, err
	}

	var embeddings OpenAIEmbeddingsResponse
	if err := embeddingsRetry.Do(ctx, "create embeddings", func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, "POST", "https://api.openai.com/v1/embeddings", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("accept", "application/json")
		req.Header.Set("Content-Type", "application/json")
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
				return &StatusError{res.StatusCode, fmt.Sprintf("creating embeddings failed (%s): %s", res.Status, errorBody.Error.Message)}
			} else {
				log.WithField("errorBody", errorBody).Warn("errorBody error message was empty")
			}

			return &StatusError{res.StatusCode, fmt.Sprintf("OpenAiClient CreateEmbeddings unexpected status: %s", res.Status)}
		}

		return json.NewDecoder(res.Body).Decode(&embeddings)
	}); err != nil {
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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type client struct {
	es *elasticsearch.Client
}

type createIndexParams struct {
	Settings indexSettings `json:"settings,omitempty"`
	Mappings indexMappings `json:"mappings"`
}

type indexSettings struct {
	Shards   *int `json:"number_of_shards,omitempty"`
	Replicas *int `json:"number_of_replicas,omitempty"`
}

type indexMappings struct {
	Properties map[string]mappedProperty `json:"properties"`
}

// createIndex creates a new index and sets its mappings per indexProps if it doesn't already exist.
func (c *client) createIndex(ctx context.Context, index string, shards *int, replicas *int, indexProps map[string]mappedProperty) error {
	existResp, err := c.es.Indices.Exists(
		[]string{index},
		c.es.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("checking if index exists: %w", err)
	}
	defer existResp.Body.Close()

	if existResp.StatusCode == http.StatusOK {
		return nil
	} else if existResp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("index exists unexpected status code: %d", existResp.StatusCode)
	}

	// Index does not exist, create a new one.
	params := createIndexParams{
		Settings: indexSettings{
			Shards:   shards,
			Replicas: replicas,
		},
		Mappings: indexMappings{
			Properties: indexProps,
		},
	}

	createResp, err := c.es.Indices.Create(
		index,
		c.es.Indices.Create.WithContext(ctx),
		c.es.Indices.Create.WithBody(esutil.NewJSONReader(params)),
	)
	if err != nil {
		return fmt.Errorf("createIndex: %w", err)
	}
	defer createResp.Body.Close()
	if createResp.IsError() {
		return fmt.Errorf("createIndex error response [%s] %s", createResp.Status(), createResp.String())
	}

	return nil
}

// deleteIndex deletes an index with the provided name.
func (c *client) deleteIndex(ctx context.Context, index string) error {
	res, err := c.es.Indices.Delete(
		[]string{index},
		c.es.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("deleting existing index: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("delete index error response [%s] %s", res.Status(), res.String())
	}

	return nil
}

func (c *client) addMappingToIndex(ctx context.Context, index string, field string, prop mappedProperty) error {
	res, err := c.es.Indices.PutMapping(
		[]string{index},
		esutil.NewJSONReader(map[string]map[string]mappedProperty{"properties": {field: prop}}),
		c.es.Indices.PutMapping.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("addMappingToIndex: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("addMappingToIndex error response [%s] %s", res.Status(), res.String())
	}

	return nil
}

func (c *client) isServerless(ctx context.Context) (bool, error) {
	res, err := c.es.Info(c.es.Info.WithContext(ctx))
	if err != nil {
		return false, fmt.Errorf("getting serverless status: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return false, fmt.Errorf("getting serverless status error response [%s] %s", res.Status(), res.String())
	}

	type infoResponse struct {
		Version struct {
			BuildFlavor string `json:"build_flavor"`
		} `json:"version"`
	}

	var info infoResponse
	if err := json.NewDecoder(res.Body).Decode(&info); err != nil {
		return false, fmt.Errorf("decoding serverless status response: %w", err)
	}

	return strings.EqualFold(info.Version.BuildFlavor, "serverless"), nil
}

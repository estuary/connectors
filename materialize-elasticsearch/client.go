package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
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
	Properties map[string]property `json:"properties"`
}

// createIndex creates a new index and sets its mappings per indexProps if it doesn't already exist.
func (c *client) createIndex(ctx context.Context, index string, shards *int, replicas *int, indexProps map[string]property) error {
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

func (c *client) addMappingToIndex(ctx context.Context, index string, field string, prop property) error {
	res, err := c.es.Indices.PutMapping(
		[]string{index},
		esutil.NewJSONReader(map[string]map[string]property{"properties": {field: prop}}),
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

type indexMetaResponse struct {
	Mappings struct {
		Properties map[string]property `json:"properties"`
	} `json:"mappings"`
}

func (c *client) populateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema) error {
	res, err := c.es.Indices.Get(
		[]string{"*"}, // Get info for all indices the connector has access to.
		c.es.Indices.Get.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("getting index metadata: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("getting index metadata error response [%s] %s", res.Status(), res.String())
	}

	var indexMeta map[string]indexMetaResponse
	if err := json.NewDecoder(res.Body).Decode(&indexMeta); err != nil {
		return fmt.Errorf("decoding index metadata response: %w", err)
	}

	for index, meta := range indexMeta {
		res := is.PushResource(index)
		for field, prop := range meta.Mappings.Properties {
			res.PushField(boilerplate.ExistingField{
				Name:               field,
				Nullable:           true,
				Type:               string(prop.Type),
				CharacterMaxLength: 0,
				Format:             prop.Format,
			})
		}
	}

	return nil
}

// serverInfo describes the cluster on the other end of the connection, as
// reported by the root info endpoint. It is used to distinguish Elasticsearch
// from OpenSearch (an API-compatible fork), and Elastic Cloud serverless from
// self-managed clusters.
type serverInfo struct {
	// Distribution is "opensearch" for OpenSearch clusters and empty (the field
	// is absent) for Elasticsearch.
	Distribution string
	// BuildFlavor is "serverless" for Elastic Cloud serverless clusters.
	BuildFlavor string
}

func (i serverInfo) isOpenSearch() bool {
	return strings.EqualFold(i.Distribution, "opensearch")
}

func (i serverInfo) isServerless() bool {
	return strings.EqualFold(i.BuildFlavor, "serverless")
}

func (c *client) info(ctx context.Context) (serverInfo, error) {
	res, err := c.es.Info(c.es.Info.WithContext(ctx))
	if err != nil {
		return serverInfo{}, fmt.Errorf("getting server info: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return serverInfo{}, fmt.Errorf("getting server info error response [%s] %s", res.Status(), res.String())
	}

	type infoResponse struct {
		Version struct {
			Distribution string `json:"distribution"`
			BuildFlavor  string `json:"build_flavor"`
		} `json:"version"`
	}

	var info infoResponse
	if err := json.NewDecoder(res.Body).Decode(&info); err != nil {
		return serverInfo{}, fmt.Errorf("decoding server info response: %w", err)
	}

	return serverInfo{
		Distribution: info.Version.Distribution,
		BuildFlavor:  info.Version.BuildFlavor,
	}, nil
}

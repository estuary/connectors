package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/tidwall/gjson"
)

const (
	defaultFlowMaterializations = ".flow_materializations_v2"
)

type client struct {
	es *elasticsearch.Client
}

func (c config) toClient() (*client, error) {
	es, err := elasticsearch.NewClient(
		elasticsearch.Config{
			Addresses: []string{c.Endpoint},
			Username:  c.Credentials.Username,
			Password:  c.Credentials.Password,
			APIKey:    c.Credentials.ApiKey,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	return &client{es: es}, nil
}

func (c *client) createMetaIndex(ctx context.Context) error {
	props := map[string]property{
		"version":   {Type: elasticTypeKeyword},
		"specBytes": {Type: elasticTypeBinary},
	}

	return c.createIndex(ctx, defaultFlowMaterializations, 1, 0, props)
}

func (c *client) putSpec(ctx context.Context, spec *pf.MaterializationSpec, version string) error {
	specBytes, err := spec.Marshal()
	if err != nil {
		return fmt.Errorf("marshalling spec: %w", err)
	}

	res, err := c.es.Index(
		defaultFlowMaterializations,
		esutil.NewJSONReader(map[string]string{
			"specBytes": base64.StdEncoding.EncodeToString(specBytes),
			"version":   version,
		}),
		c.es.Index.WithContext(ctx),
		c.es.Index.WithRefresh("true"),
		c.es.Index.WithDocumentID(url.PathEscape(spec.Name.String())),
	)
	if err != nil {
		return fmt.Errorf("putSpec: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("putSpec error response [%s] %s", res.Status(), res.String())
	}

	return nil
}

func (c *client) getSpec(ctx context.Context, materialization pf.Materialization) (*pf.MaterializationSpec, error) {
	res, err := c.es.Get(
		defaultFlowMaterializations,
		url.PathEscape(string(materialization)),
		c.es.Get.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("getSpec spec: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return nil, nil
	} else if res.IsError() {
		return nil, fmt.Errorf("getSpec error response [%s] %s", res.Status(), res.String())
	}

	var spec pf.MaterializationSpec

	if jsonBytes, err := io.ReadAll(res.Body); err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	} else if loc := gjson.GetBytes(jsonBytes, "_source.specBytes"); !loc.Exists() {
		return nil, fmt.Errorf("malformed response: '_source.specBytes' does not exist")
	} else if specBytes, err := base64.StdEncoding.DecodeString(loc.String()); err != nil {
		return nil, fmt.Errorf("base64.Decode: %w", err)
	} else if err := spec.Unmarshal(specBytes); err != nil {
		return nil, fmt.Errorf("spec.Unmarshal: %w", err)
	} else if err := spec.Validate(); err != nil {
		return nil, fmt.Errorf("validating spec: %w", err)
	}

	return &spec, nil
}

type createIndexParams struct {
	Settings createIndexSettings `json:"settings"`
	Mappings indexMappings       `json:"mappings"`
}

type createIndexSettings struct {
	Shards   int `json:"number_of_shards"`
	Replicas int `json:"number_of_replicas"`
}

type indexMappings struct {
	Properties map[string]property `json:"properties"`
}

// createIndex creates a new index and sets its mappings per indexProps if it doesn't already exist.
func (c *client) createIndex(ctx context.Context, index string, shards int, replicas int, indexProps map[string]property) error {
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
		Settings: createIndexSettings{
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
		c.es.Indices.Create.WithWaitForActiveShards("all"),
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

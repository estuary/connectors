package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/tidwall/gjson"
)

const (
	defaultFlowMaterializations = "flow_materializations_v2"
)

type client struct {
	es *elasticsearch.Client
}

func (c *client) createMetaIndex(ctx context.Context, replicas *int) error {
	props := map[string]property{
		"version": {Type: elasticTypeKeyword, Index: boolPtr(false)},
		// Binary mappings are never indexed, and to specify index: false on such a mapping results
		// in an error.
		"specBytes": {Type: elasticTypeBinary},
	}

	// The meta index will always be created with the default number of shards.
	// Long term we plan to not require a meta index at all.
	return c.createIndex(ctx, defaultFlowMaterializations, nil, replicas, props)
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

func (c *client) infoSchema(ctx context.Context) (*boilerplate.InfoSchema, error) {
	res, err := c.es.Indices.Get(
		[]string{"*"}, // Get info for all indices the connector has access to.
		c.es.Indices.Get.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("getting index metadata: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return nil, fmt.Errorf("getting index metadata error response [%s] %s", res.Status(), res.String())
	}

	is := boilerplate.NewInfoSchema(
		func(rp []string) []string {
			// Pass-through the index name as-is, since it is the only component of the resource
			// path and the required transformations are assumed to already be done as part of the
			// Validate response.
			return rp
		},
		translateField,
	)

	var indexMeta map[string]indexMetaResponse
	if err := json.NewDecoder(res.Body).Decode(&indexMeta); err != nil {
		return nil, fmt.Errorf("decoding index metadata response: %w", err)
	}

	for index, meta := range indexMeta {
		is.PushResource(index)

		for field, prop := range meta.Mappings.Properties {
			is.PushField(boilerplate.EndpointField{
				Name:               field,
				Nullable:           true,
				Type:               string(prop.Type),
				CharacterMaxLength: 0,
			}, index)
		}
	}

	return is, nil
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

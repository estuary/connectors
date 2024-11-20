package main

import (
	"encoding/json"
	"fmt"

	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/invopop/jsonschema"
)

const (
	metaProperty   = "_meta"
	sourceProperty = "source"
	sequenceNumber = "sequence_number"
	partitionKey   = "partition_key"
	streamSource   = "stream"
	shardSource    = "shard"
)

// Provides a default schema to use for collections.
var minimalSchema = &jsonschema.Schema{
	Type:                 "object",
	Required:             []string{metaProperty},
	AdditionalProperties: nil,
	Extras: map[string]any{
		"x-infer-schema": true,

		"properties": map[string]*jsonschema.Schema{
			metaProperty: {
				Type:     "object",
				Required: []string{sequenceNumber, partitionKey},
				Extras: map[string]any{
					"properties": map[string]*jsonschema.Schema{
						sequenceNumber: {Type: "string"},
						partitionKey:   {Type: "string"},
						sourceProperty: {
							Type:     "object",
							Required: []string{shardSource, streamSource},
							Extras: map[string]any{
								"properties": map[string]*jsonschema.Schema{
									streamSource: {Type: "string"},
									shardSource:  {Type: "string"},
								},
							},
						},
					},
				},
			},
		},
	},
}

func discoverStreams(streams []kinesisStream) ([]*pc.Response_Discovered_Binding, error) {
	var out = make([]*pc.Response_Discovered_Binding, 0, len(streams))

	bs, err := json.Marshal(minimalSchema)
	if err != nil {
		panic(fmt.Errorf("error generating schema: %v", err))
	}

	for _, s := range streams {
		resourceJSON, err := json.Marshal(resource{Stream: s.name})
		if err != nil {
			return nil, fmt.Errorf("serializing resource json: %w", err)
		}
		out = append(out, &pc.Response_Discovered_Binding{
			RecommendedName:    s.name,
			DocumentSchemaJson: bs,
			ResourceConfigJson: resourceJSON,
			Key:                []string{fmt.Sprintf("/%s/%s", metaProperty, sequenceNumber), fmt.Sprintf("/%s/%s", metaProperty, partitionKey)},
		})
	}

	return out, nil
}

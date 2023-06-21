package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/service/kinesis"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
)

const (
	metaProperty   = "_meta"
	sequenceNumber = "sequence_number"
	partitionKey   = "partition_key"
)

// Provides a default schema to use for collections.
var minimalSchema = &jsonschema.Schema{
	Type:                 "object",
	Required:             []string{metaProperty},
	AdditionalProperties: nil,
	Extras: map[string]interface{}{
		"x-infer-schema": true,

		"properties": map[string]*jsonschema.Schema{
			metaProperty: {
				Type:     "object",
				Required: []string{sequenceNumber, partitionKey},
				Extras: map[string]interface{}{
					"properties": map[string]*jsonschema.Schema{
						sequenceNumber: {
							Type: "string",
						},
						partitionKey: {
							Type: "string",
						},
					},
				},
			},
		},
	},
}

func discoverStreams(ctx context.Context, client *kinesis.Kinesis, streamNames []string) []*pc.Response_Discovered_Binding {
	var streams = make([]*pc.Response_Discovered_Binding, len(streamNames))

	// Marshal schema to JSON
	bs, err := json.Marshal(minimalSchema)
	if err != nil {
		panic(fmt.Errorf("error generating schema: %v", err))
	}

	for i, name := range streamNames {
		resourceJSON, err := json.Marshal(resource{Stream: name})
		if err != nil {
			panic(fmt.Errorf("serializing resource json: %w", err))
		}
		streams[i] = &pc.Response_Discovered_Binding{
			RecommendedName:    pf.Collection(name),
			DocumentSchemaJson: json.RawMessage(bs),
			ResourceConfigJson: resourceJSON,
			Key:                []string{fmt.Sprintf("/%s/%s", metaProperty, sequenceNumber), fmt.Sprintf("/%s/%s", metaProperty, partitionKey)},
		}
	}

	return streams
}

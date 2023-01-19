package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/invopop/jsonschema"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/estuary/flow/go/protocols/airbyte"
)

const (
	metaProperty  = "_meta"
	sequenceNumber = "sequence_number"
	partitionKey = "partition_key"
)

// Provides a default schema to use for collections.
var minimalSchema = &jsonschema.Schema{
		Type:                 "object",
		Required:             []string{metaProperty},
		AdditionalProperties: nil,
		Extras: map[string]interface{}{
			"x-infer-schema": true,

			"properties": map[string]*jsonschema.Schema{
				metaProperty: &jsonschema.Schema{
					Type: "object",
					Required: []string{sequenceNumber, partitionKey},
					Extras: map[string]interface{}{
						"properties": map[string]*jsonschema.Schema{
							sequenceNumber: &jsonschema.Schema{
								Type: "string",
							},
							partitionKey: &jsonschema.Schema{
								Type: "string",
							},
						},
					},
				},
			},
		},
}

func discoverStreams(ctx context.Context, client *kinesis.Kinesis, streamNames []string) []airbyte.Stream {
	var streams = make([]airbyte.Stream, len(streamNames))

	// Marshal schema to JSON
	bs, err := json.Marshal(minimalSchema)
	if err != nil {
		panic(fmt.Errorf("error generating schema: %v", err))
	}

	for i, name := range streamNames {
		streams[i] = airbyte.Stream{
			Name:                name,
			JSONSchema:          json.RawMessage(bs),
			SupportedSyncModes:  []airbyte.SyncMode{airbyte.SyncModeIncremental},
			SourceDefinedPrimaryKey: [][]string{{metaProperty, sequenceNumber}, {metaProperty, partitionKey}},
			SourceDefinedCursor: true,
		}
	}

	return streams
}

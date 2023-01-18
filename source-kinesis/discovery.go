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
)

// Provides a default schema to use for collections.
var minimalSchema = &jsonschema.Schema{
		Type:                 "object",
		Required:             []string{metaProperty},
		AdditionalProperties: nil,
		Extras: map[string]interface{}{
			"x-infer-schema": true,
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
			SourceDefinedCursor: true,
		}
	}

	return streams
}

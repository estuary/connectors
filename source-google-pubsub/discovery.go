package main

import (
	"context"
	"encoding/json"
	"fmt"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	"google.golang.org/api/iterator"
)

var minimalSchema = generateMinimalSchema()

func (driver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	client, err := cfg.client(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting client from config: %w", err)
	}

	discoveredTopics := []string{}
	topicIter := client.Topics(ctx)
	for {
		topic, err := topicIter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, fmt.Errorf("iterating over topics: %w", err)
		}

		discoveredTopics = append(discoveredTopics, topic.ID())
	}

	bindings := make([]*pc.Response_Discovered_Binding, 0, len(discoveredTopics))

	for _, topic := range discoveredTopics {
		resourceJson, err := json.Marshal(resource{Topic: topic})
		if err != nil {
			return nil, fmt.Errorf("marshalling resource: %w", err)
		}

		bindings = append(bindings, &pc.Response_Discovered_Binding{
			RecommendedName:    topic,
			ResourceConfigJson: resourceJson,
			DocumentSchemaJson: minimalSchema,
			Key:                []string{"/_meta/id"},
		})
	}

	return &pc.Response_Discovered{Bindings: bindings}, nil
}

type documentMetadata struct {
	ID          string            `json:"id"`
	Topic       string            `json:"topic"`
	Subcription string            `json:"subscription"`
	PublishTime string            `json:"publishTime,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	OrderingKey string            `json:"orderingKey,omitempty"`
}

func generateMinimalSchema() json.RawMessage {
	var schema = &jsonschema.Schema{
		Type:     "object",
		Required: []string{"_meta"},
		Extras: map[string]interface{}{
			"properties": map[string]*jsonschema.Schema{
				"_meta": {
					Type:     "object",
					Required: []string{"id", "subscription"},
					Extras: map[string]interface{}{
						"properties": map[string]*jsonschema.Schema{
							"id": {
								Type:  "string",
								Title: "ID of the message",
							},
							"topic": {
								Type:  "string",
								Title: "Topic the message was published to",
							},
							"subscription": {
								Type:  "string",
								Title: "Subscription the message was read from",
							},
							"publishTime": {
								Type:   "string",
								Format: "date-time",
								Title:  "Time the message was published",
							},
							"attributes": {
								Type:  "object",
								Title: "The key-value pairs this message is labelled with",
							},
							"orderingKey": {
								Type:  "string",
								Title: "The ordering key used for the message",
							},
						},
					},
				},
			},
			"x-infer-schema": true,
		},
	}

	bs, err := json.Marshal(schema)
	if err != nil {
		panic(fmt.Errorf("error generating schema: %v", err))
	}
	return json.RawMessage(bs)
}

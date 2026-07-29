package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
)

var (
	standardSchema = generateMinimalSchema(false)
	fifoSchema     = generateMinimalSchema(true)
)

func (driver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	client, err := buildClient(ctx, &cfg, 0)
	if err != nil {
		return nil, err
	}

	var queueURLs []string
	paginator := sqs.NewListQueuesPaginator(client, &sqs.ListQueuesInput{MaxResults: aws.Int32(1000)})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing queues: %w", err)
		}
		queueURLs = append(queueURLs, page.QueueUrls...)
	}
	sort.Strings(queueURLs)

	bindings := make([]*pc.Response_Discovered_Binding, 0, len(queueURLs))
	for _, queueURL := range queueURLs {
		name, _, err := parseQueueURL(queueURL)
		if err != nil {
			return nil, fmt.Errorf("parsing discovered queue URL: %w", err)
		}

		resourceJSON, err := json.Marshal(resource{QueueURL: queueURL})
		if err != nil {
			return nil, fmt.Errorf("marshalling resource: %w", err)
		}

		binding := &pc.Response_Discovered_Binding{
			RecommendedName:    name,
			ResourceConfigJson: resourceJSON,
		}
		if isFifoQueueName(name) {
			binding.DocumentSchemaJson = fifoSchema
			// For FIFO queues the (group, sequence number) pair is unique
			// and stable across redeliveries, and tables keyed on it
			// cluster by group in queue order.
			binding.Key = []string{"/_meta/messageGroupId", "/_meta/sequenceNumber"}
		} else {
			binding.DocumentSchemaJson = standardSchema
			binding.Key = []string{"/_meta/messageId"}
		}
		bindings = append(bindings, binding)
	}

	return &pc.Response_Discovered{Bindings: bindings}, nil
}

func generateMinimalSchema(fifo bool) json.RawMessage {
	metaProperties := map[string]*jsonschema.Schema{
		"queueUrl": {
			Type:  "string",
			Title: "URL of the queue the message was received from",
		},
		"messageId": {
			Type:  "string",
			Title: "SQS-assigned unique ID of the message",
		},
		"sentTimestamp": {
			Type:   "string",
			Format: "date-time",
			Title:  "Time the message was sent to the queue",
		},
		"approximateReceiveCount": {
			Type:  "integer",
			Title: "Number of times the message has been received without being deleted",
		},
		"messageAttributes": {
			Type:  "object",
			Title: "Message attributes attached by the producer",
		},
	}
	metaRequired := []string{"messageId"}

	if fifo {
		metaProperties["messageGroupId"] = &jsonschema.Schema{
			Type:  "string",
			Title: "Message group this message belongs to",
		}
		metaProperties["sequenceNumber"] = &jsonschema.Schema{
			Type:  "string",
			Title: "SQS-assigned sequence number, strictly increasing within the message group",
		}
		metaProperties["deduplicationId"] = &jsonschema.Schema{
			Type:  "string",
			Title: "Producer-supplied or content-based deduplication ID",
		}
		metaRequired = []string{"messageId", "messageGroupId", "sequenceNumber"}
	}

	schema := &jsonschema.Schema{
		Type:     "object",
		Required: []string{"_meta"},
		Extras: map[string]any{
			"properties": map[string]*jsonschema.Schema{
				"_meta": {
					Type:     "object",
					Required: metaRequired,
					Extras: map[string]any{
						"properties": metaProperties,
					},
				},
			},
			"x-infer-schema": true,
		},
	}

	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		panic(fmt.Errorf("error generating schema: %v", err))
	}
	return json.RawMessage(schemaJSON)
}

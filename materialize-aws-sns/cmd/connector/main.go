package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	connector "github.com/estuary/connectors/materialize-aws-sns"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: connector <config.json>")
	}

	configFile := os.Args[1]
	configData, err := os.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	config, err := connector.ResolveEndpointConfig(json.RawMessage(configData))
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	ctx := context.Background()
	client, err := config.Client(ctx)
	if err != nil {
		log.Fatalf("Failed to create SNS client: %v", err)
	}

	// Example usage
	bindings := []*connector.TopicBinding{
		{
			Identifier: "example-binding",
			TopicName:  "example-topic",
		},
	}

	transactor := connector.NewTransactor(config, client, bindings)

	// Example document
	documents := []map[string]interface{}{
		{
			"_meta": map[string]interface{}{
				"op":        "c",
				"namespace": "example.collection",
				"source":    "example-source",
				"uuid":      "example-uuid-123",
			},
			"id":   1,
			"name": "example-record",
		},
	}

	err = transactor.ProcessDocuments(ctx, documents)
	if err != nil {
		log.Fatalf("Failed to process documents: %v", err)
	}

	fmt.Println("Successfully processed documents")
}


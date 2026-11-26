package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
)

var (
	queueSchema  = generateMinimalSchema(false)
	streamSchema = generateMinimalSchema(true)
)

type queueInfo struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Exclusive  bool   `json:"exclusive"`
	AutoDelete bool   `json:"auto_delete"`
	Arguments  struct {
		QueueType string `json:"x-queue-type"`
	} `json:"arguments"`
}

func (d driver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}
	cfg.SetDefaults()
	if cfg.Username == "" || cfg.Password == "" {
		return nil, fmt.Errorf("management API discovery requires 'username' and 'password', including with EXTERNAL AMQP authentication")
	}

	client := d.httpClient
	if client == nil {
		var err error
		client, err = buildHTTPClient(&cfg)
		if err != nil {
			return nil, err
		}
		defer client.CloseIdleConnections()
	}
	queues, err := listQueues(ctx, client, &cfg)
	if err != nil {
		return nil, fmt.Errorf("listing queues: %w", err)
	}
	sort.Slice(queues, func(i, j int) bool { return queues[i].Name < queues[j].Name })

	bindings := make([]*pc.Response_Discovered_Binding, 0, len(queues))
	for _, queue := range queues {
		if queue.Name == "" {
			return nil, fmt.Errorf("management API returned a queue without a name")
		}
		if strings.HasPrefix(queue.Name, "amq.gen-") && (queue.Exclusive || queue.AutoDelete) {
			continue
		}

		queueType := queue.Type
		if queueType == "" {
			queueType = queue.Arguments.QueueType
		}
		if queueType == "" {
			queueType = "classic"
		}
		switch queueType {
		case "classic", "quorum", "stream":
		default:
			continue
		}
		resourceJSON, err := json.Marshal(resource{Queue: queue.Name, QueueType: queueType})
		if err != nil {
			return nil, fmt.Errorf("marshalling resource: %w", err)
		}

		schema := queueSchema
		if queueType == "stream" {
			schema = streamSchema
		}
		bindings = append(bindings, &pc.Response_Discovered_Binding{
			RecommendedName:    queue.Name,
			ResourceConfigJson: resourceJSON,
			DocumentSchemaJson: schema,
			Key:                []string{"/_meta/id"},
		})
	}

	return &pc.Response_Discovered{Bindings: bindings}, nil
}

func listQueues(ctx context.Context, client *http.Client, cfg *config) ([]queueInfo, error) {
	scheme := "http"
	if cfg.TLS {
		scheme = "https"
	}
	endpoint := url.URL{
		Scheme:  scheme,
		Host:    net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.ManagementPort)),
		Path:    "/api/queues/" + cfg.VirtualHost,
		RawPath: "/api/queues/" + url.PathEscape(cfg.VirtualHost),
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating management API request: %w", err)
	}
	req.SetBasicAuth(cfg.Username, cfg.Password)
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting management API: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("management API returned HTTP %d (%s)", resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	var queues []queueInfo
	if err := json.NewDecoder(resp.Body).Decode(&queues); err != nil {
		return nil, fmt.Errorf("decoding management API queues: %w", err)
	}
	return queues, nil
}

func generateMinimalSchema(stream bool) json.RawMessage {
	metaProperties := map[string]*jsonschema.Schema{
		"queue": {
			Type:  "string",
			Title: "Queue the message was received from",
		},
		"id": {
			Type:  "string",
			Title: "Unique ID of the message",
		},
		"exchange": {
			Type:  "string",
			Title: "Exchange the message was published to",
		},
		"routingKey": {
			Type:  "string",
			Title: "Routing key of the message",
		},
		"timestamp": {
			Type:   "string",
			Format: "date-time",
			Title:  "Timestamp provided by the producer",
		},
		"messageId": {
			Type:  "string",
			Title: "Message ID provided by the producer",
		},
		"contentType": {
			Type:  "string",
			Title: "Content type provided by the producer",
		},
		"contentEncoding": {
			Type:  "string",
			Title: "Content encoding provided by the producer",
		},
		"correlationId": {
			Type:  "string",
			Title: "Correlation ID provided by the producer",
		},
		"replyTo": {
			Type:  "string",
			Title: "Reply destination provided by the producer",
		},
		"type": {
			Type:  "string",
			Title: "Message type provided by the producer",
		},
		"appId": {
			Type:  "string",
			Title: "Application ID provided by the producer",
		},
		"userId": {
			Type:  "string",
			Title: "User ID provided by the producer",
		},
		"priority": {
			Type:  "integer",
			Title: "Message priority",
		},
		"deliveryMode": {
			Type:  "integer",
			Title: "Message delivery mode",
		},
		"expiration": {
			Type:  "string",
			Title: "Message expiration in milliseconds provided by the producer",
		},
		"redelivered": {
			Type:  "boolean",
			Title: "Whether the message was redelivered",
		},
		"headers": {
			Type:  "object",
			Title: "Headers attached by the producer",
		},
	}
	metaRequired := []string{"queue", "id", "exchange", "routingKey", "redelivered"}
	if stream {
		metaProperties["offset"] = &jsonschema.Schema{
			Type:  "integer",
			Title: "Offset of the message in the stream",
		}
		metaRequired = append(metaRequired, "offset")
	}

	schema := &jsonschema.Schema{
		Type:     "object",
		Required: []string{"_meta"},
		Extras: map[string]any{
			"properties": map[string]*jsonschema.Schema{
				"_meta": {
					Type:     "object",
					Required: metaRequired,
					Extras:   map[string]any{"properties": metaProperties},
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

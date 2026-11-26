package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	amqp "github.com/rabbitmq/amqp091-go"
)

type driver struct {
	httpClient *http.Client
	connect    func(context.Context, *config) (*amqp.Connection, error)
}

func (driver) Spec(context.Context, *pc.Request_Spec) (*pc.Response_Spec, error) {
	endpoint := schemagen.GenerateSchema("Source RabbitMQ Spec", &config{})
	endpoint.If = &jsonschema.Schema{
		Required: []string{"authMechanism"},
		Extras:   map[string]any{"properties": map[string]any{"authMechanism": map[string]any{"const": "EXTERNAL"}}},
	}
	endpoint.Then = &jsonschema.Schema{
		Required: []string{"tls", "tlsConfig"},
		Extras: map[string]any{"properties": map[string]any{
			"tls":       map[string]any{"const": true},
			"tlsConfig": map[string]any{"required": []string{"ssl_client_cert", "ssl_client_key"}},
		}},
	}
	endpoint.Else = &jsonschema.Schema{Required: []string{"username", "password"}}
	endpointSchema, err := endpoint.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}
	resourceSchema, err := schemagen.GenerateSchema("Resource", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/source-rabbitmq",
		ResourcePathPointers:     []string{"/queue"},
	}, nil
}

func (d driver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}
	cfg.SetDefaults()

	var resources []resource
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		resources = append(resources, res)
	}

	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	connect := d.connect
	if connect == nil {
		connect = buildClient
	}
	client, err := connect(connectCtx, &cfg)
	if err != nil {
		if connectCtx.Err() != nil {
			return nil, fmt.Errorf("connecting to RabbitMQ: %w", connectCtx.Err())
		}
		return nil, err
	}
	defer func() { client.CloseDeadline(time.Now().Add(5 * time.Second)) }()
	stop := context.AfterFunc(connectCtx, func() { client.CloseDeadline(time.Now()) })

	channel, err := client.Channel()
	stopped := stop()
	cancel()
	if !stopped {
		return nil, fmt.Errorf("opening AMQP channel: %w", connectCtx.Err())
	}
	if err != nil {
		return nil, fmt.Errorf("opening AMQP channel: %w", err)
	}

	out := make([]*pc.Response_Validated_Binding, 0, len(resources))
	for _, res := range resources {
		queueCtx, cancel := context.WithTimeout(ctx, connectTimeout)
		stop := context.AfterFunc(queueCtx, func() { client.CloseDeadline(time.Now()) })
		// Passive declarations check existence without creating queues or
		// consuming messages. RabbitMQ ignores the other queue properties.
		_, err := channel.QueueDeclarePassive(res.Queue, false, false, false, false, nil)
		stopped := stop()
		cancel()
		if !stopped {
			return nil, fmt.Errorf("validating queue %q: %w", res.Queue, queueCtx.Err())
		}
		if err != nil {
			return nil, fmt.Errorf("queue %q is not accessible: %w", res.Queue, err)
		}
		out = append(out, &pc.Response_Validated_Binding{ResourcePath: []string{res.Queue}})
	}

	return &pc.Response_Validated{Bindings: out}, nil
}

// Apply is a no-op since the connector owns no server-side resources.
func (driver) Apply(context.Context, *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{}, nil
}

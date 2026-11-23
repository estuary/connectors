package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	amqp "github.com/rabbitmq/amqp091-go"
)

type config struct {
	Host        string `json:"host" jsonschema:"title=Host,description=Hostname or IP address of your RabbitMQ server without a scheme or port.,minLength=1"`
	Port        int    `json:"port,omitempty" jsonschema:"title=Port,description=AMQP port of your RabbitMQ server. For TLS use the server's TLS port (typically 5671).,default=5672,minimum=1,maximum=65535"`
	VirtualHost string `json:"virtualHost,omitempty" jsonschema:"title=Virtual Host,description=RabbitMQ virtual host containing your exchanges.,default=/,minLength=1"`
	Username    string `json:"username" jsonschema:"title=Username,description=Username for authenticating with your RabbitMQ server.,minLength=1"`
	Password    string `json:"password" jsonschema:"title=Password,description=Password for authenticating with your RabbitMQ server.,minLength=1" jsonschema_extras:"secret=true"`
	TLS         bool   `json:"tls,omitempty" jsonschema:"title=Use TLS,description=Connect to your RabbitMQ server using TLS and system CA certificates. Does not change the port; typically set port to 5671.,default=false"`
}

func (c config) Validate() error {
	for _, field := range []struct{ name, value string }{
		{"host", c.Host},
		{"virtualHost", c.VirtualHost},
		{"username", c.Username},
		{"password", c.Password},
	} {
		if field.value == "" {
			return fmt.Errorf("missing '%s'", field.name)
		}
	}
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("'port' must be between 1 and 65535")
	}
	return nil
}

func (c config) address() string {
	scheme := "amqp"
	if c.TLS {
		scheme = "amqps"
	}
	u := url.URL{
		Scheme:  scheme,
		Host:    net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		Path:    "/" + c.VirtualHost,
		RawPath: "/" + url.PathEscape(c.VirtualHost),
	}
	return u.String()
}

func (c config) client(ctx context.Context) (*amqp.Connection, error) {
	client, err := amqp.DialConfig(c.address(), amqp.Config{
		SASL: []amqp.Authentication{&amqp.PlainAuth{Username: c.Username, Password: c.Password}},
		Dial: func(network, address string) (net.Conn, error) {
			dialer := net.Dialer{Timeout: 30 * time.Second}
			conn, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			// Bound TLS and AMQP handshaking as well as the initial connection. The
			// AMQP client clears this deadline when the handshake completes.
			deadline := time.Now().Add(30 * time.Second)
			if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
				deadline = d
			}
			if err := conn.SetDeadline(deadline); err != nil {
				conn.Close()
				return nil, err
			}
			return conn, nil
		},
	})
	if err != nil {
		return nil, fmt.Errorf("connecting to RabbitMQ: %w", err)
	}
	return client, nil
}

type resource struct {
	Exchange        string `json:"exchange" jsonschema:"title=Exchange,description=Name of an existing exchange to publish your documents to. Declare the exchange and bind queues before publishing the materialization.,minLength=1,maxLength=255" jsonschema_extras:"x-collection-name=true"`
	ExchangeType    string `json:"exchangeType,omitempty" jsonschema:"title=Exchange Type,description=Expected exchange type. Passive checks only verify existence; the connector does not create or change exchanges.,enum=direct,enum=fanout,enum=topic,default=topic"`
	RoutingKey      string `json:"routingKey,omitempty" jsonschema:"title=Routing Key,description=Routing key used when no scalar value is found at the routing key field.,default=,maxLength=255"`
	RoutingKeyField string `json:"routingKeyField,omitempty" jsonschema:"title=Routing Key Field,description=Optional JSON pointer to a collection projection whose string or number or boolean value becomes the routing key.,format=json-pointer"`
}

func (r resource) Validate() error {
	if r.Exchange == "" {
		return fmt.Errorf("missing 'exchange'")
	}
	if len(r.Exchange) > 255 {
		return fmt.Errorf("'exchange' must be at most 255 bytes")
	}
	switch r.ExchangeType {
	case "direct", "fanout", "topic":
	default:
		return fmt.Errorf("'exchangeType' must be direct, fanout, or topic")
	}
	if len(r.RoutingKey) > 255 {
		return fmt.Errorf("'routingKey' must be at most 255 bytes")
	}
	if err := validatePointer(r.RoutingKeyField); err != nil {
		return fmt.Errorf("invalid 'routingKeyField': %w", err)
	}
	return nil
}

func (r resource) validateBinding(collection pf.CollectionSpec) (*pm.Response_Validated_Binding, error) {
	if r.RoutingKeyField != "" {
		found := false
		for _, p := range collection.Projections {
			if p.Ptr == r.RoutingKeyField {
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("'routingKeyField' %q must be a projection of collection %q", r.RoutingKeyField, collection.Name)
		}
	}

	var constraints []*pm.Response_Validated_ProjectionConstraint
	for _, projection := range collection.Projections {
		var constraint = new(pm.Response_Validated_Constraint)
		switch {
		case projection.IsRootDocumentProjection():
			constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
			constraint.Reason = "The root document must be materialized"
		case r.RoutingKeyField != "" && projection.Ptr == r.RoutingKeyField:
			constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
			constraint.Reason = "The routing key field must be materialized"
		case projection.IsPrimaryKey:
			constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		default:
			constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
			constraint.Reason = "RabbitMQ only materializes the full document"
		}
		constraints = append(constraints, &pm.Response_Validated_ProjectionConstraint{
			Field:      projection.Field,
			Constraint: constraint,
		})
	}

	return &pm.Response_Validated_Binding{
		CaseInsensitiveFields: false,
		ProjectionConstraints: constraints,
		DeltaUpdates:          true,
		ResourcePath:          []string{r.Exchange},
	}, nil
}

func Driver() driver {
	return driver{}
}

type driver struct{}

var _ boilerplate.Connector = &driver{}

func (d driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	endpointSchema, err := schemagen.GenerateSchema("Materialize RabbitMQ Spec", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}
	resourceSchema, err := schemagen.GenerateSchema("RabbitMQ Exchange", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-rabbitmq",
	}, nil
}

func (d driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	cfg, err := resolveEndpointConfig(req.ConfigJson)
	if err != nil {
		return nil, err
	}
	client, err := cfg.client(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	var out []*pm.Response_Validated_Binding
	for _, b := range req.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, err
		}
		binding, err := res.validateBinding(b.Collection)
		if err != nil {
			return nil, err
		}
		if err := checkExchange(client, res); err != nil {
			return nil, err
		}
		out = append(out, binding)
	}
	return &pm.Response_Validated{Bindings: out}, nil
}

// Apply rechecks exchanges without changing them or their queue bindings.
func (d driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}
	cfg, err := resolveEndpointConfig(req.Materialization.ConfigJson)
	if err != nil {
		return nil, err
	}
	client, err := cfg.client(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	for _, b := range req.Materialization.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, err
		}
		if err := checkExchange(client, res); err != nil {
			return nil, err
		}
	}
	return &pm.Response_Applied{}, nil
}

func checkExchange(client *amqp.Connection, res resource) error {
	ch, err := client.Channel()
	if err != nil {
		return fmt.Errorf("opening exchange check channel: %w", err)
	}
	defer ch.Close()
	err = ch.ExchangeDeclarePassive(res.Exchange, res.ExchangeType, true, false, false, false, nil)
	if err == nil {
		return nil
	}
	var amqpErr *amqp.Error
	if errors.As(err, &amqpErr) && amqpErr.Code == amqp.NotFound {
		return fmt.Errorf("exchange %q does not exist: declare it and bind queues before publishing the materialization: %w", res.Exchange, err)
	}
	return fmt.Errorf("checking exchange %q: %w", res.Exchange, err)
}

func (d driver) NewTransactor(ctx context.Context, open pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	if err := boilerplate.RequireSingleShard(open); err != nil {
		return nil, nil, nil, err
	}
	cfg, err := resolveEndpointConfig(open.Materialization.ConfigJson)
	if err != nil {
		return nil, nil, nil, err
	}
	var bindings []*exchangeBinding
	for _, b := range open.Materialization.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, nil, nil, err
		}
		if _, err := res.validateBinding(b.Collection); err != nil {
			return nil, nil, nil, err
		}
		binding, err := newExchangeBinding(b, res)
		if err != nil {
			return nil, nil, nil, err
		}
		bindings = append(bindings, binding)
	}

	client, err := cfg.client(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	ch, err := client.Channel()
	if err != nil {
		client.Close()
		return nil, nil, nil, fmt.Errorf("opening publish channel: %w", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, publishWindow))
	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	if err := ch.Confirm(false); err != nil {
		client.Close()
		return nil, nil, nil, fmt.Errorf("enabling publisher confirms: %w", err)
	}
	return &transactor{
		client:   client,
		channel:  ch,
		confirms: confirms,
		closed:   closed,
		bindings: bindings,
		be:       be,
	}, &pm.Response_Opened{}, nil, nil
}

func newExchangeBinding(spec *pf.MaterializationSpec_Binding, res resource) (*exchangeBinding, error) {
	b := &exchangeBinding{
		path: spec.ResourcePath, exchange: res.Exchange, routingKey: res.RoutingKey,
		routingKeyIndex: -1,
	}
	if res.RoutingKeyField == "" {
		return b, nil
	}
	for _, fields := range []struct {
		names []string
		key   bool
	}{{spec.FieldSelection.Keys, true}, {spec.FieldSelection.Values, false}} {
		for i, field := range fields.names {
			if p := spec.Collection.GetProjection(field); p != nil && p.Ptr == res.RoutingKeyField {
				b.routingKeyIndex, b.routingKeyIsKey = i, fields.key
				return b, nil
			}
		}
	}
	return nil, fmt.Errorf("'routingKeyField' %q must be selected for exchange %q", res.RoutingKeyField, res.Exchange)
}

func validatePointer(ptr string) error {
	if ptr == "" {
		return nil
	}
	if !strings.HasPrefix(ptr, "/") {
		return fmt.Errorf("JSON pointer must start with '/'")
	}
	for i := 0; i < len(ptr); i++ {
		if ptr[i] == '~' {
			if i+1 == len(ptr) || (ptr[i+1] != '0' && ptr[i+1] != '1') {
				return fmt.Errorf("JSON pointer contains an invalid '~' escape")
			}
			i++
		}
	}
	return nil
}

func resolveEndpointConfig(specJson json.RawMessage) (config, error) {
	cfg := config{Port: 5672, VirtualHost: "/"}
	if err := pf.UnmarshalStrict(specJson, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing RabbitMQ endpoint config: %w", err)
	}
	return cfg, nil
}

func resolveResourceConfig(specJson json.RawMessage) (resource, error) {
	res := resource{ExchangeType: "topic"}
	if err := pf.UnmarshalStrict(specJson, &res); err != nil {
		return res, fmt.Errorf("parsing RabbitMQ resource config: %w", err)
	}
	return res, nil
}

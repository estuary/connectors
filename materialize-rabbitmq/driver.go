package connector

import (
	"bytes"
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
	Host        string `json:"host" jsonschema:"title=Host,description=Hostname or IP address of your RabbitMQ server.,minLength=1"`
	Port        int    `json:"port,omitempty" jsonschema:"title=Port,description=AMQP port of your RabbitMQ server.,default=5672,minimum=1,maximum=65535"`
	VirtualHost string `json:"virtualHost,omitempty" jsonschema:"title=Virtual Host,description=RabbitMQ virtual host containing your exchanges.,default=/,minLength=1"`
	Username    string `json:"username" jsonschema:"title=Username,description=Username for authenticating with your RabbitMQ server.,minLength=1"`
	Password    string `json:"password" jsonschema:"title=Password,description=Password for authenticating with your RabbitMQ server.,minLength=1" jsonschema_extras:"secret=true"`
	TLS         bool   `json:"tls,omitempty" jsonschema:"title=Use TLS,description=Connect to your RabbitMQ server using TLS and system CA certificates.,default=false"`
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
	Exchange        string `json:"exchange" jsonschema:"title=Exchange,description=Name of the exchange to publish your documents to.,minLength=1,maxLength=255" jsonschema_extras:"x-collection-name=true"`
	ExchangeType    string `json:"exchangeType,omitempty" jsonschema:"title=Exchange Type,description=Type used when creating a missing exchange.,enum=direct,enum=fanout,enum=topic,default=topic"`
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
	if _, err := parsePointer(r.RoutingKeyField); err != nil {
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

	resourcePath := []string{r.Exchange}
	if r.RoutingKey != "" {
		resourcePath = append(resourcePath, r.RoutingKey)
	}
	return &pm.Response_Validated_Binding{
		CaseInsensitiveFields: false,
		ProjectionConstraints: constraints,
		DeltaUpdates:          true,
		ResourcePath:          resourcePath,
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
	exchangeTypes := make(map[string]string)
	for _, b := range req.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, err
		}
		if err := checkExchangeType(exchangeTypes, res); err != nil {
			return nil, err
		}
		binding, err := res.validateBinding(b.Collection)
		if err != nil {
			return nil, err
		}
		out = append(out, binding)
	}
	return &pm.Response_Validated{Bindings: out}, nil
}

// checkExchangeType rejects bindings that share an exchange but ask for
// different exchange types, since Apply can only create the exchange once.
func checkExchangeType(seen map[string]string, res resource) error {
	if prior, ok := seen[res.Exchange]; ok && prior != res.ExchangeType {
		return fmt.Errorf("bindings for exchange %q set different exchangeType values (%s, %s)", res.Exchange, prior, res.ExchangeType)
	}
	seen[res.Exchange] = res.ExchangeType
	return nil
}

// Apply creates missing exchanges, leaving existing exchanges as-is.
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

	var actions []string
	checked := make(map[string]struct{})
	for _, b := range req.Materialization.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, err
		}
		if _, ok := checked[res.Exchange]; ok {
			continue
		}
		checked[res.Exchange] = struct{}{}
		if created, err := ensureExchange(client, res); err != nil {
			return nil, err
		} else if created {
			actions = append(actions, fmt.Sprintf("created exchange %s", res.Exchange))
		}
	}
	return &pm.Response_Applied{ActionDescription: strings.Join(actions, "\n")}, nil
}

func ensureExchange(client *amqp.Connection, res resource) (bool, error) {
	ch, err := client.Channel()
	if err != nil {
		return false, fmt.Errorf("opening exchange check channel: %w", err)
	}
	err = ch.ExchangeDeclarePassive(res.Exchange, res.ExchangeType, true, false, false, false, nil)
	ch.Close()
	if err == nil {
		return false, nil
	}
	var amqpErr *amqp.Error
	if !errors.As(err, &amqpErr) || amqpErr.Code != amqp.NotFound {
		return false, fmt.Errorf("checking exchange %q: %w", res.Exchange, err)
	}

	// A failed passive declaration closes its channel. Declare the missing
	// exchange on a fresh channel.
	ch, err = client.Channel()
	if err != nil {
		return false, fmt.Errorf("opening exchange creation channel: %w", err)
	}
	defer ch.Close()
	if err := ch.ExchangeDeclare(res.Exchange, res.ExchangeType, true, false, false, false, nil); err != nil {
		return false, fmt.Errorf("creating exchange %q: %w", res.Exchange, err)
	}
	return true, nil
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
		routingKeyPointer, err := parsePointer(res.RoutingKeyField)
		if err != nil {
			return nil, nil, nil, err
		}
		binding := &exchangeBinding{
			path:              b.ResourcePath,
			exchange:          res.Exchange,
			routingKey:        res.RoutingKey,
			routingKeyPointer: routingKeyPointer,
		}
		if err := binding.setCollectionKey(b.Collection); err != nil {
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

func (b *exchangeBinding) setCollectionKey(collection pf.CollectionSpec) error {
	b.keyPointers = make([]jsonPointer, len(collection.Key))
	b.keyDefaults = make([]any, len(collection.Key))
	for i, ptr := range collection.Key {
		keyPointer, err := parsePointer(ptr)
		if err != nil {
			return fmt.Errorf("parsing collection key pointer %q: %w", ptr, err)
		}
		b.keyPointers[i] = keyPointer
		for _, projection := range collection.Projections {
			if projection.Ptr != ptr || len(projection.Inference.DefaultJson) == 0 {
				continue
			}
			decoder := json.NewDecoder(bytes.NewReader(projection.Inference.DefaultJson))
			decoder.UseNumber()
			if err := decoder.Decode(&b.keyDefaults[i]); err != nil {
				return fmt.Errorf("decoding default for collection key pointer %q: %w", ptr, err)
			}
			break
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

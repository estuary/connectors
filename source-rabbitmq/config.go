package main

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

const defaultPrefetchCount = 10

type config struct {
	Host           string    `json:"host" jsonschema:"title=Host,description=Hostname or IP address of your RabbitMQ server.,minLength=1" jsonschema_extras:"order=1"`
	Port           int       `json:"port,omitempty" jsonschema:"title=Port,description=AMQP port of your RabbitMQ server.,default=5672,minimum=1,maximum=65535" jsonschema_extras:"order=2"`
	VirtualHost    string    `json:"virtualHost,omitempty" jsonschema:"title=Virtual Host,description=RabbitMQ virtual host containing your queues.,default=/" jsonschema_extras:"order=3"`
	Username       string    `json:"username,omitempty" jsonschema:"title=Username,description=Username for PLAIN authentication and management API discovery. Optional for EXTERNAL captures.,minLength=1" jsonschema_extras:"order=4"`
	Password       string    `json:"password,omitempty" jsonschema:"title=Password,description=Password for PLAIN authentication and management API discovery. Optional for EXTERNAL captures.,minLength=1" jsonschema_extras:"secret=true,order=5"`
	TLS            bool      `json:"tls,omitempty" jsonschema:"title=TLS,description=Use TLS for AMQP connections and HTTPS for queue discovery.,default=false" jsonschema_extras:"order=6"`
	ManagementPort int       `json:"managementPort,omitempty" jsonschema:"title=Management Port,description=RabbitMQ management API port used for queue discovery.,default=15672,minimum=1,maximum=65535" jsonschema_extras:"order=7"`
	AuthMechanism  string    `json:"authMechanism,omitempty" jsonschema:"title=Authentication Mechanism,description=AMQP SASL authentication mechanism. EXTERNAL uses your TLS client certificate and requires RabbitMQ's rabbitmq_auth_mechanism_ssl plugin.,enum=PLAIN,enum=EXTERNAL,default=PLAIN" jsonschema_extras:"order=8"`
	TLSConfig      tlsConfig `json:"tlsConfig,omitempty" jsonschema:"title=TLS Settings,description=Optional PEM certificates for AMQP and HTTPS discovery. Requires TLS to be enabled." jsonschema_extras:"advanced=true,order=9"`
	PrefetchCount  int       `json:"prefetchCount,omitempty" jsonschema:"title=Prefetch Count,description=Maximum unacknowledged messages per queue. Larger values can improve throughput when commit latency is limiting but increase memory use.,default=10,minimum=1,maximum=65535" jsonschema_extras:"advanced=true,order=10"`
}

type tlsConfig struct {
	SSLServerCA   string `json:"ssl_server_ca,omitempty" jsonschema:"title=SSL Server CA,description=PEM-encoded CA bundle the server certificates must chain to. Omit to trust system certificate authorities." jsonschema_extras:"secret=true,multiline=true,order=1"`
	SSLClientCert string `json:"ssl_client_cert,omitempty" jsonschema:"title=SSL Client Certificate,description=PEM-encoded client certificate chain for mutual TLS." jsonschema_extras:"secret=true,multiline=true,order=2"`
	SSLClientKey  string `json:"ssl_client_key,omitempty" jsonschema:"title=SSL Client Key,description=PEM-encoded private key for the client certificate." jsonschema_extras:"secret=true,multiline=true,order=3"`
}

func (c *config) Validate() error {
	if c.Host == "" {
		return errors.New("missing 'host'")
	}
	switch c.AuthMechanism {
	case "", "PLAIN":
		if c.Username == "" {
			return errors.New("missing 'username'")
		}
		if c.Password == "" {
			return errors.New("missing 'password'")
		}
	case "EXTERNAL":
		if !c.TLS || c.TLSConfig.SSLClientCert == "" || c.TLSConfig.SSLClientKey == "" {
			return errors.New("EXTERNAL authentication requires 'tls' and a client certificate and key in 'tlsConfig'")
		}
	default:
		return fmt.Errorf("invalid 'authMechanism': %q (expected PLAIN or EXTERNAL)", c.AuthMechanism)
	}
	if c.Port < 0 || c.Port > 65535 {
		return fmt.Errorf("invalid 'port': %d (expected 1 to 65535)", c.Port)
	}
	if c.ManagementPort < 0 || c.ManagementPort > 65535 {
		return fmt.Errorf("invalid 'managementPort': %d (expected 1 to 65535)", c.ManagementPort)
	}
	if c.PrefetchCount < 0 || c.PrefetchCount > 65535 {
		return fmt.Errorf("invalid 'prefetchCount': %d (expected 1 to 65535)", c.PrefetchCount)
	}
	_, err := c.buildTLSConfig()
	return err
}

func (c *config) SetDefaults() {
	if c.Port == 0 {
		c.Port = 5672
	}
	if c.VirtualHost == "" {
		c.VirtualHost = "/"
	}
	if c.ManagementPort == 0 {
		c.ManagementPort = 15672
	}
	if c.AuthMechanism == "" {
		c.AuthMechanism = "PLAIN"
	}
	if c.PrefetchCount == 0 {
		c.PrefetchCount = defaultPrefetchCount
	}
}

type resource struct {
	Queue             string `json:"queue" jsonschema:"title=Queue,description=Name of the RabbitMQ queue to capture.,minLength=1"`
	QueueType         string `json:"queueType,omitempty" jsonschema:"title=Queue Type,description=Queue type reported by discovery. When omitted the connector probes for stream support.,enum=classic,enum=quorum,enum=stream" jsonschema_extras:"advanced=true"`
	StreamStartOffset string `json:"streamStartOffset,omitempty" jsonschema:"title=Stream Start Offset,description=Initial stream position: first (first retained message)\\, last (last chunk)\\, next (new messages only)\\, an RFC3339 timestamp\\, or an interval such as 7D or 12h. Saved offsets take precedence. Used only for stream queues.,default=first" jsonschema_extras:"advanced=true"`
}

func (r *resource) Validate() error {
	if r.Queue == "" {
		return errors.New("missing 'queue'")
	}
	if r.QueueType != "" && r.QueueType != "classic" && r.QueueType != "quorum" && r.QueueType != "stream" {
		return fmt.Errorf("invalid 'queueType': %q (expected classic, quorum, or stream)", r.QueueType)
	}
	_, err := r.initialStreamOffset()
	return err
}

var streamInterval = regexp.MustCompile(`^[1-9][0-9]*[YMDhms]$`)

func (r *resource) initialStreamOffset() (any, error) {
	switch r.StreamStartOffset {
	case "", "first":
		return "first", nil
	case "last", "next":
		return r.StreamStartOffset, nil
	}
	if streamInterval.MatchString(r.StreamStartOffset) {
		if _, err := strconv.ParseUint(r.StreamStartOffset[:len(r.StreamStartOffset)-1], 10, 63); err == nil {
			return r.StreamStartOffset, nil
		}
	}
	if timestamp, err := time.Parse(time.RFC3339Nano, r.StreamStartOffset); err == nil && timestamp.Unix() >= 0 {
		// AMQP timestamps have whole-second precision.
		return timestamp.UTC().Truncate(time.Second), nil
	}
	return nil, fmt.Errorf("invalid 'streamStartOffset': %q (expected first, last, next, an RFC3339 timestamp since 1970, or a positive interval with unit Y, M, D, h, m, or s)", r.StreamStartOffset)
}

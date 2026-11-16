package connector

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	mysqltls "github.com/estuary/connectors/go/mysql/tls"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	"github.com/redis/go-redis/v9"
)

type advancedConfig struct {
	SSLServerCA   string `json:"ssl_server_ca,omitempty" jsonschema:"title=SSL Server CA,description=PEM-encoded CA certificates to trust instead of the system certificate authorities." jsonschema_extras:"secret=true,multiline=true"`
	SSLClientCert string `json:"ssl_client_cert,omitempty" jsonschema:"title=SSL Client Certificate,description=PEM-encoded client certificate for mutual TLS." jsonschema_extras:"secret=true,multiline=true"`
	SSLClientKey  string `json:"ssl_client_key,omitempty" jsonschema:"title=SSL Client Key,description=PEM-encoded private key for the client certificate." jsonschema_extras:"secret=true,multiline=true"`
	SSLServerName string `json:"ssl_server_name,omitempty" jsonschema:"title=SSL Server Name,description=Override the hostname used to verify the server certificate. Defaults to the hostname being connected to including through SSH."`
	SSLSkipVerify bool   `json:"ssl_skip_verify,omitempty" jsonschema:"title=Skip Certificate Verification,description=Disable server certificate and hostname verification. This makes TLS vulnerable to impersonation.,default=false"`
}

type sentinelConfig struct {
	MasterName string   `json:"masterName" jsonschema:"title=Primary Name,description=Name of the primary monitored by Sentinel.,minLength=1"`
	Addresses  []string `json:"addresses,omitempty" jsonschema:"title=Additional Sentinel Addresses,description=Additional Sentinel endpoints in host:port format. The endpoint address is always included."`
	Username   string   `json:"username,omitempty" jsonschema:"title=Sentinel Username,description=Username for authenticating to Sentinel separately from Redis."`
	Password   string   `json:"password,omitempty" jsonschema:"title=Sentinel Password,description=Password for authenticating to Sentinel separately from Redis." jsonschema_extras:"secret=true"`
}

func validateAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return fmt.Errorf("address must be in host:port format")
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("address port must be an integer between 1 and 65535")
	}
	return nil
}

func (c config) validateConnection() error {
	if _, err := c.tlsConfig(); err != nil {
		return err
	}
	if c.Sentinel != nil {
		if c.Sentinel.MasterName == "" {
			return fmt.Errorf("sentinel.masterName is required")
		}
		for _, address := range c.Sentinel.Addresses {
			if err := validateAddress(address); err != nil {
				return fmt.Errorf("sentinel.addresses: %w", err)
			}
		}
	}
	if c.NetworkTunnel != nil && c.NetworkTunnel.SSHForwarding != nil {
		ssh := c.NetworkTunnel.SSHForwarding
		u, err := url.Parse(ssh.SSHEndpoint)
		if err != nil || u.Scheme != "ssh" || u.Hostname() == "" || u.User == nil || u.User.Username() == "" {
			return fmt.Errorf("sshEndpoint must be ssh://user@hostname[:port]")
		}
		if port := u.Port(); port != "" {
			if err := validateAddress(net.JoinHostPort(u.Hostname(), port)); err != nil {
				return fmt.Errorf("sshEndpoint: %w", err)
			}
		}
		if ssh.PrivateKey == "" {
			return fmt.Errorf("networkTunnel.sshForwarding.privateKey is required")
		}
	}
	return nil
}

func (c config) tlsConfig() (*tls.Config, error) {
	if !c.TLS {
		if c.Advanced != (advancedConfig{}) {
			return nil, fmt.Errorf("advanced TLS options require tls to be enabled")
		}
		return nil, nil
	}
	settings := mysqltls.Settings{
		Mode:       mysqltls.ModeVerifyIdentity,
		ServerCA:   c.Advanced.SSLServerCA,
		ClientCert: c.Advanced.SSLClientCert,
		ClientKey:  c.Advanced.SSLClientKey,
	}
	cfg, err := settings.Config(c.Advanced.SSLServerName)
	if err != nil {
		return nil, err
	}
	// Preserve the system-only trust store used by existing Redis configurations.
	if c.Advanced.SSLServerCA == "" {
		cfg.RootCAs = nil
	}
	cfg.MinVersion = tls.VersionTLS12
	cfg.InsecureSkipVerify = c.Advanced.SSLSkipVerify
	return cfg, nil
}

func (c config) client() (*redis.Client, error) {
	opts, err := c.clientOptions()
	if err != nil {
		return nil, err
	}
	if c.NetworkTunnel.InUse() {
		start := c.startTunnel
		if start == nil {
			start = func(ctx context.Context, address string) (string, error) {
				return startTunnel(ctx, c.NetworkTunnel, address)
			}
		}
		forwarder := &tunnelForwarder{start: start}
		opts.Dialer = forwarder.dialer(opts.TLSConfig)
	}
	return c.newClient(opts), nil
}

func (c config) clientOptions() (*redis.Options, error) {
	tlsConfig, err := c.tlsConfig()
	if err != nil {
		return nil, err
	}
	return &redis.Options{
		Addr:                  c.Address,
		Username:              c.Username,
		Password:              c.Password,
		DB:                    c.Database,
		TLSConfig:             tlsConfig,
		ReadTimeout:           time.Minute,
		WriteTimeout:          time.Minute,
		ContextTimeoutEnabled: true,
	}, nil
}

func (c config) newClient(opts *redis.Options) *redis.Client {
	if c.Sentinel == nil {
		return redis.NewClient(opts)
	}
	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:            c.Sentinel.MasterName,
		SentinelAddrs:         append([]string{c.Address}, c.Sentinel.Addresses...),
		SentinelUsername:      c.Sentinel.Username,
		SentinelPassword:      c.Sentinel.Password,
		Username:              opts.Username,
		Password:              opts.Password,
		DB:                    opts.DB,
		TLSConfig:             opts.TLSConfig,
		Dialer:                opts.Dialer,
		ReadTimeout:           opts.ReadTimeout,
		WriteTimeout:          opts.WriteTimeout,
		ContextTimeoutEnabled: opts.ContextTimeoutEnabled,
	})
}

type tunnelForwarder struct {
	mu        sync.Mutex
	addresses map[string]string
	start     func(context.Context, string) (string, error)
}

func (f *tunnelForwarder) address(ctx context.Context, remote string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if local, ok := f.addresses[remote]; ok {
		return local, nil
	}
	local, err := f.start(ctx, remote)
	if err != nil {
		return "", fmt.Errorf("starting SSH tunnel: %w", err)
	}
	if f.addresses == nil {
		f.addresses = make(map[string]string)
	}
	f.addresses[remote] = local
	return local, nil
}

func (f *tunnelForwarder) dialer(tlsConfig *tls.Config) redis.DialHook {
	return func(ctx context.Context, network, remote string) (net.Conn, error) {
		local, err := f.address(ctx, remote)
		if err != nil {
			return nil, err
		}
		opts := &redis.Options{DialTimeout: 5 * time.Second, TLSConfig: tlsConfig}
		if tlsConfig != nil && tlsConfig.ServerName == "" {
			opts.TLSConfig = tlsConfig.Clone()
			opts.TLSConfig.ServerName, _, err = net.SplitHostPort(remote)
			if err != nil {
				return nil, err
			}
		}
		return redis.NewDialer(opts)(ctx, network, local)
	}
}

func startTunnel(ctx context.Context, cfg *networkTunnel.TunnelConfig, address string) (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	local := listener.Addr().String()
	_, port, _ := net.SplitHostPort(local)
	if err := listener.Close(); err != nil {
		return "", err
	}
	// Shared tunnels live for the connector process, including across primary changes.
	if _, err := cfg.Start(ctx, address, port); err != nil {
		return "", err
	}
	return local, nil
}

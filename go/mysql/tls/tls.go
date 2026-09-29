// Package mysqltls builds the TLS configuration for connections to a MySQL
// server from an 'sslmode' setting modelled on the MySQL client's --ssl-mode
// option, as also exposed by materialize-mysql.
package mysqltls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"slices"
)

// SSL modes, in increasing order of strictness. The names match the MySQL
// client's --ssl-mode values lowercased.
const (
	// ModeDisabled never negotiates TLS.
	ModeDisabled = "disabled"
	// ModePreferred negotiates TLS without verifying the server, and falls back
	// to an unencrypted connection when the TLS connection attempt fails.
	ModePreferred = "preferred"
	// ModeRequired negotiates TLS without verifying the server and never falls
	// back to an unencrypted connection.
	ModeRequired = "required"
	// ModeVerifyCA verifies that the server certificate chains to the provided
	// CA, but does not check the server's hostname. Useful when connecting to
	// an IP address or a certificate whose names don't match the address.
	ModeVerifyCA = "verify_ca"
	// ModeVerifyIdentity verifies the server certificate chain and that it is
	// valid for the configured server hostname. Uses the provided CA when set,
	// otherwise the system root store.
	ModeVerifyIdentity = "verify_identity"
)

var Modes = []string{ModeDisabled, ModePreferred, ModeRequired, ModeVerifyCA, ModeVerifyIdentity}

// DefaultMode applies when 'sslmode' is unset: the connection must be encrypted,
// but the server certificate is not verified.
const DefaultMode = ModeRequired

// Settings holds the user-facing SSL configuration.
type Settings struct {
	Mode       string // One of Modes, or empty for DefaultMode.
	ServerCA   string // PEM-encoded CA certificate(s) the server certificate must chain to.
	ClientCert string // Optional PEM-encoded client certificate for mutual TLS.
	ClientKey  string // PEM-encoded private key for ClientCert.
}

// EffectiveMode returns Mode, or DefaultMode when Mode is unset.
func (s Settings) EffectiveMode() string {
	if s.Mode == "" {
		return DefaultMode
	}
	return s.Mode
}

// Validate checks that the settings are internally consistent and that any
// PEM material parses.
func (s Settings) Validate() error {
	var _, _, err = s.parse()
	return err
}

// parse validates the settings and decodes their PEM material: the pool
// 'ssl_server_ca' configures, or nil when it is unset, and the client
// certificate to present, if any.
func (s Settings) parse() (serverCA *x509.CertPool, clientCerts []tls.Certificate, err error) {
	var mode = s.EffectiveMode()
	if !slices.Contains(Modes, mode) {
		return nil, nil, fmt.Errorf("invalid 'sslmode' configuration: unknown setting %q", mode)
	}
	if mode == ModeVerifyCA && s.ServerCA == "" {
		return nil, nil, fmt.Errorf("'ssl_server_ca' is required when 'sslmode' is %q", ModeVerifyCA)
	}
	if s.ServerCA != "" && mode != ModeVerifyCA && mode != ModeVerifyIdentity {
		return nil, nil, fmt.Errorf("'ssl_server_ca' is set but 'sslmode' is %q, "+
			"which does not verify the server certificate: "+
			"set 'sslmode' to %q or %q, or remove 'ssl_server_ca'", mode, ModeVerifyCA, ModeVerifyIdentity)
	}
	if s.ClientCert != "" && mode == ModeDisabled {
		return nil, nil, fmt.Errorf("'ssl_client_cert' is set but 'sslmode' is %q, "+
			"which never uses TLS: choose another 'sslmode', "+
			"or remove 'ssl_client_cert' and 'ssl_client_key'", ModeDisabled)
	}
	if s.ServerCA != "" {
		serverCA = x509.NewCertPool()
		if !serverCA.AppendCertsFromPEM([]byte(s.ServerCA)) {
			return nil, nil, errors.New("invalid 'ssl_server_ca': no PEM-encoded certificates found")
		}
	}
	if (s.ClientCert == "") != (s.ClientKey == "") {
		return nil, nil, errors.New("'ssl_client_cert' and 'ssl_client_key' must be provided together")
	}
	if s.ClientCert != "" {
		cert, err := tls.X509KeyPair([]byte(s.ClientCert), []byte(s.ClientKey))
		if err != nil {
			return nil, nil, fmt.Errorf("invalid 'ssl_client_cert' / 'ssl_client_key': %w", err)
		}
		clientCerts = []tls.Certificate{cert}
	}
	return serverCA, clientCerts, nil
}

// AllowsPlaintextFallback reports whether a failed TLS connection attempt may
// be retried without encryption.
func (s Settings) AllowsPlaintextFallback() bool {
	return s.EffectiveMode() == ModePreferred
}

// GuaranteesEncryption reports whether every connection made with these settings
// is necessarily encrypted.
func (s Settings) GuaranteesEncryption() bool {
	switch s.EffectiveMode() {
	case ModeRequired, ModeVerifyCA, ModeVerifyIdentity:
		return true
	case ModeDisabled, ModePreferred:
		return false
	}

	return false
}

// Config returns the *tls.Config to use when connecting to the server whose
// hostname is serverHost, or nil when TLS must not be used. serverHost should
// be the hostname the user configured, not the local end of any tunnel, since
// that is the name the certificate is checked against under verify_identity.
func (s Settings) Config(serverHost string) (*tls.Config, error) {
	serverCA, clientCerts, err := s.parse()
	if err != nil {
		return nil, err
	}
	var mode = s.EffectiveMode()
	if mode == ModeDisabled {
		return nil, nil
	}

	var cfg = &tls.Config{Certificates: clientCerts}

	switch mode {
	case ModePreferred, ModeRequired:
		// Encryption only. The server is not authenticated, so an on-path
		// attacker can impersonate it; this matches MySQL's own PREFERRED and
		// REQUIRED modes.
		cfg.InsecureSkipVerify = true
	case ModeVerifyCA:
		// Go's verifier always checks the hostname when InsecureSkipVerify is
		// false, so chain-only verification has to be done by hand.
		cfg.InsecureSkipVerify = true
		cfg.VerifyPeerCertificate = verifyChainOnly(serverCA)
	case ModeVerifyIdentity:
		// A nil pool, when 'ssl_server_ca' is unset, trusts the system roots.
		cfg.ServerName, cfg.RootCAs = serverHost, serverCA
	}
	return cfg, nil
}

// verifyChainOnly verifies that the presented leaf certificate chains to one
// of roots, treating any additional presented certificates as intermediates,
// without checking the certificate's names against the server address.
func verifyChainOnly(roots *x509.CertPool) func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("server presented no certificate")
		}
		var certs = make([]*x509.Certificate, 0, len(rawCerts))
		for _, raw := range rawCerts {
			cert, err := x509.ParseCertificate(raw)
			if err != nil {
				return fmt.Errorf("parsing server certificate: %w", err)
			}
			certs = append(certs, cert)
		}
		var intermediates = x509.NewCertPool()
		for _, cert := range certs[1:] {
			intermediates.AddCert(cert)
		}
		if _, err := certs[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates}); err != nil {
			return fmt.Errorf("server certificate not signed by the configured 'ssl_server_ca': %w", err)
		}
		return nil
	}
}

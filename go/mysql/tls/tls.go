// Package mysqltls builds the TLS configuration for connections to a MySQL
// server from an 'sslmode' setting modelled on the MySQL client's --ssl-mode
// option, as also exposed by materialize-mysql.
package mysqltls

import (
	"crypto/tls"
	"crypto/x509"
	"embed"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"sync"
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
	// otherwise the system root store plus the bundled managed-database CAs.
	ModeVerifyIdentity = "verify_identity"
)

var Modes = []string{ModeDisabled, ModePreferred, ModeRequired, ModeVerifyCA, ModeVerifyIdentity}

// DefaultMode applies when 'sslmode' is unset: the connection must be encrypted,
// and the server certificate must be valid for the configured host.
const DefaultMode = ModeVerifyIdentity

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

// cloudCAs holds the CA bundles of managed MySQL services whose server
// certificates don't chain to a public root. Refresh with cloudcas/update.sh.
//
//go:embed cloudcas/*.pem
var cloudCAs embed.FS

// defaultRoots is the pool 'verify_identity' trusts when no 'ssl_server_ca' is
// configured: the system root store plus the bundled managed-database CAs.
// The pool is only ever read, so it is built once and shared.
var defaultRoots = sync.OnceValue(func() *x509.CertPool {
	var pool, err = x509.SystemCertPool()
	if err != nil {
		// Only the bundled CAs will be trusted, which is still fail-closed.
		pool = x509.NewCertPool()
	}
	// Reading embedded files cannot fail, and TestDefaultRootsTrustCloudCAs
	// checks that every bundled certificate parses.
	var names, _ = fs.Glob(cloudCAs, "cloudcas/*.pem")
	for _, name := range names {
		var pem, _ = cloudCAs.ReadFile(name)
		pool.AppendCertsFromPEM(pem)
	}
	return pool
})

// rootCAs returns the pool the server certificate must chain to: 'ssl_server_ca'
// when set, otherwise the default roots.
func (s Settings) rootCAs() (*x509.CertPool, error) {
	if s.ServerCA == "" {
		return defaultRoots(), nil
	}
	var pool = x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(s.ServerCA)) {
		return nil, errors.New("invalid 'ssl_server_ca': no PEM-encoded certificates found")
	}
	return pool, nil
}

// Validate checks that the settings are internally consistent and that any
// PEM material parses.
func (s Settings) Validate() error {
	var mode = s.EffectiveMode()
	if !slices.Contains(Modes, mode) {
		return fmt.Errorf("invalid 'sslmode' configuration: unknown setting %q", mode)
	}
	if mode == ModeVerifyCA && s.ServerCA == "" {
		return fmt.Errorf("'ssl_server_ca' is required when 'sslmode' is %q", ModeVerifyCA)
	}
	if s.ServerCA != "" && mode != ModeVerifyCA && mode != ModeVerifyIdentity {
		return fmt.Errorf("'ssl_server_ca' is set but 'sslmode' is %q, "+
			"which does not verify the server certificate: "+
			"set 'sslmode' to %q or %q, or remove 'ssl_server_ca'", mode, ModeVerifyCA, ModeVerifyIdentity)
	}
	if s.ClientCert != "" && mode == ModeDisabled {
		return fmt.Errorf("'ssl_client_cert' is set but 'sslmode' is %q, "+
			"which never uses TLS: choose another 'sslmode', "+
			"or remove 'ssl_client_cert' and 'ssl_client_key'", ModeDisabled)
	}
	if s.ServerCA != "" {
		if _, err := s.rootCAs(); err != nil {
			return err
		}
	}
	if (s.ClientCert == "") != (s.ClientKey == "") {
		return errors.New("'ssl_client_cert' and 'ssl_client_key' must be provided together")
	}
	if s.ClientCert != "" {
		if _, err := tls.X509KeyPair([]byte(s.ClientCert), []byte(s.ClientKey)); err != nil {
			return fmt.Errorf("invalid 'ssl_client_cert' / 'ssl_client_key': %w", err)
		}
	}
	return nil
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
	if err := s.Validate(); err != nil {
		return nil, err
	}
	var mode = s.EffectiveMode()
	if mode == ModeDisabled {
		return nil, nil
	}

	var cfg = &tls.Config{}
	if s.ClientCert != "" {
		cert, err := tls.X509KeyPair([]byte(s.ClientCert), []byte(s.ClientKey))
		if err != nil {
			return nil, fmt.Errorf("invalid 'ssl_client_cert' / 'ssl_client_key': %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	switch mode {
	case ModePreferred, ModeRequired:
		// Encryption only. The server is not authenticated, so an on-path
		// attacker can impersonate it; this matches MySQL's own PREFERRED and
		// REQUIRED modes.
		cfg.InsecureSkipVerify = true
	case ModeVerifyCA, ModeVerifyIdentity:
		roots, err := s.rootCAs()
		if err != nil {
			return nil, err
		}
		if mode == ModeVerifyIdentity {
			cfg.ServerName, cfg.RootCAs = serverHost, roots
			break
		}
		// Go's verifier always checks the hostname when InsecureSkipVerify is
		// false, so chain-only verification has to be done by hand. The
		// handshake has already failed if the server presented no certificate.
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			var opts = x509.VerifyOptions{Roots: roots, Intermediates: x509.NewCertPool()}
			for _, cert := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(cert)
			}
			if _, err := cs.PeerCertificates[0].Verify(opts); err != nil {
				// Wrapped like Go's own verification failures, so that
				// presentedCA can find the certificates the server presented.
				return &tls.CertificateVerificationError{
					UnverifiedCertificates: cs.PeerCertificates,
					Err:                    fmt.Errorf("server certificate not signed by the configured 'ssl_server_ca': %w", err),
				}
			}
			return nil
		}
	}
	return cfg, nil
}

// certificateNames lists the host names and IP addresses a certificate is
// valid for.
func certificateNames(cert *x509.Certificate) []string {
	if cert == nil {
		return nil
	}
	var names = slices.Clone(cert.DNSNames)
	for _, ip := range cert.IPAddresses {
		names = append(names, ip.String())
	}
	return names
}

// presentedCA returns the self-signed CA certificate that the server presented
// alongside its own certificate, and which signed it, or nil when a failed
// verification err carries no such certificate. MySQL's auto-generated
// certificates are presented this way.
func presentedCA(err error) *x509.Certificate {
	var verifyErr *tls.CertificateVerificationError
	if !errors.As(err, &verifyErr) || len(verifyErr.UnverifiedCertificates) < 2 {
		return nil
	}
	var leaf, rest = verifyErr.UnverifiedCertificates[0], verifyErr.UnverifiedCertificates[1:]
	for _, candidate := range rest {
		if !candidate.IsCA || candidate.CheckSignatureFrom(candidate) != nil {
			continue
		}
		var opts = x509.VerifyOptions{
			Roots:         x509.NewCertPool(),
			Intermediates: x509.NewCertPool(),
			KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		}
		opts.Roots.AddCert(candidate)
		for _, cert := range rest {
			opts.Intermediates.AddCert(cert)
		}
		if _, err := leaf.Verify(opts); err == nil {
			return candidate
		}
	}
	return nil
}

// presentedCAHint shows the CA certificate the server presented, so that it can
// be copied into 'ssl_server_ca', or returns "" when ca is nil.
func presentedCAHint(ca *x509.Certificate) string {
	if ca == nil {
		return ""
	}
	return fmt.Sprintf("\n\nThe server presented this CA certificate (%s). "+
		"This connection was not verified, so confirm that it matches your server's CA certificate before using it:\n%s",
		ca.Subject, strings.TrimSpace(string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Raw}))))
}

// serverCAToSet describes the certificate to set 'ssl_server_ca' to, pointing at
// the one presentedCAHint shows when the server presented ca.
func serverCAToSet(ca *x509.Certificate) string {
	if ca != nil {
		return "the CA certificate below"
	}
	return "the CA certificate that signed the server certificate"
}

// verificationRemediation explains how to resolve a connection attempt to
// serverHost that failed because the server certificate was rejected, or
// returns "" when err is not a certificate verification failure.
func (s Settings) verificationRemediation(err error, serverHost string) string {
	var hostErr x509.HostnameError
	var authErr x509.UnknownAuthorityError
	var invalidErr x509.CertificateInvalidError
	var ca = presentedCA(err)

	// Go checks the certificate's validity and names before its chain, so a
	// hostname error says nothing about whether the chain would be trusted.
	switch {
	case errors.As(err, &invalidErr):
		return fmt.Sprintf("The server certificate is invalid (%s). "+
			"Renew or reissue the server certificate.", invalidErr.Error())
	case errors.As(err, &hostErr):
		var problem = "does not name any host, as is standard in MySQL's auto-generated certificates"
		if listed := certificateNames(hostErr.Certificate); len(listed) > 0 {
			problem = fmt.Sprintf("is issued for %q, not %q", listed, serverHost)
		}
		return fmt.Sprintf("The server certificate %s.\n\n"+
			"To fix this, either:\n"+
			"- set 'sslmode' to %q and 'ssl_server_ca' to %s, or\n"+
			"- set 'sslmode' to %q.%s",
			problem, ModeVerifyCA, serverCAToSet(ca), ModeRequired, presentedCAHint(ca))
	case errors.As(err, &authErr):
		if s.ServerCA != "" {
			return "The server certificate is not signed by the configured 'ssl_server_ca'. " +
				"Set 'ssl_server_ca' to " + serverCAToSet(ca) + "." + presentedCAHint(ca)
		}
		return fmt.Sprintf("The server certificate is not signed by a public certificate authority, "+
			"nor by Amazon RDS or Google Cloud SQL's shared CA.\n\n"+
			"To fix this, either:\n"+
			"- set 'ssl_server_ca' to %s, or\n"+
			"- set 'sslmode' to %q.%s",
			serverCAToSet(ca), ModeRequired, presentedCAHint(ca))
	}
	return ""
}

// tlsUnsupported reports whether err is go-mysql's refusal to connect to a
// server that doesn't offer TLS.
func tlsUnsupported(err error) bool {
	return strings.Contains(err.Error(), "does not support TLS required by the client")
}

// FailureMessage explains a TLS connection attempt to serverHost that failed
// with err and could not fall back to an unencrypted connection: the failure,
// which 'sslmode' was in effect, and how to resolve it. It returns "" when TLS
// didn't cause the failure, as with a network error or one the server reports
// after the handshake, since changing 'sslmode' wouldn't fix it.
// plaintextAllowed reports whether suggesting a mode that may leave the
// connection unencrypted is acceptable.
func (s Settings) FailureMessage(err error, serverHost string, plaintextAllowed bool) string {
	var origin = fmt.Sprintf("'sslmode' is %q", s.EffectiveMode())
	if s.Mode == "" {
		origin = fmt.Sprintf("'sslmode' is unset and defaults to %q", s.EffectiveMode())
	}
	var explanation string
	if remediation := s.verificationRemediation(err, serverHost); remediation != "" {
		explanation = fmt.Sprintf("%s, which verifies the server certificate. %s", origin, remediation)
	} else if !tlsUnsupported(err) {
		return ""
	} else if !plaintextAllowed {
		explanation = origin + ", which does not permit falling back to an unencrypted connection. " +
			"The server must be configured to accept TLS."
	} else {
		explanation = fmt.Sprintf("%s, which does not permit falling back to an unencrypted connection. "+
			"Configure the server to accept TLS, or set 'sslmode' to %q or %q to connect without it.",
			origin, ModePreferred, ModeDisabled)
	}
	return fmt.Sprintf("%v.\n\n%s", err, explanation)
}

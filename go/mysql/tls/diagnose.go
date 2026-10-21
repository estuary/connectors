package mysqltls

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// mariadbEphemeralCN is the Common Name of the certificate MariaDB 11.4+
// generates in memory at startup when none is configured.
const mariadbEphemeralCN = "MariaDB Server"

// FailureMessage explains a TLS connection attempt to serverHost that failed
// with err and could not fall back to an unencrypted connection. It returns ""
// when TLS didn't cause the failure. plaintextAllowed reports whether
// suggesting a mode that may leave the connection unencrypted is acceptable.
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

// tlsUnsupported reports whether err is go-mysql's refusal to connect to a
// server that doesn't offer TLS.
func tlsUnsupported(err error) bool {
	return strings.Contains(err.Error(), "does not support TLS required by the client")
}

// verificationRemediation explains how to resolve a connection attempt to
// serverHost that failed because the server certificate was rejected, or
// returns "" when err is not a certificate verification failure.
func (s Settings) verificationRemediation(err error, serverHost string) string {
	var verifyErr *tls.CertificateVerificationError
	var hostErr x509.HostnameError
	var authErr x509.UnknownAuthorityError
	var invalidErr x509.CertificateInvalidError

	var chain []*x509.Certificate // The certificates the server presented, leaf first.
	if errors.As(err, &verifyErr) {
		chain = verifyErr.UnverifiedCertificates
	}
	var pin = findPinnableCert(chain)

	if isMariadbEphemeralCert(chain) {
		return fmt.Sprintf("The server certificate is the one MariaDB generates in memory at startup when none is configured. "+
			"It names no host and changes on every server restart, so it can't be pinned with 'ssl_server_ca'.\n\n"+
			"To fix this, either:\n"+
			"- configure a persistent certificate on the server (its 'ssl_cert' and 'ssl_key' settings), "+
			"then set 'ssl_server_ca' to the CA certificate that signed it, with 'sslmode' %q unless it names the host, or\n"+
			"- set 'sslmode' to %q.",
			ModeVerifyCA, ModeRequired)
	}

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
			problem, ModeVerifyCA, pin.toSet(), ModeRequired, pin.hint())
	case errors.As(err, &authErr):
		if s.ServerCA != "" {
			return "The server certificate is not signed by the configured 'ssl_server_ca'. " +
				"Set 'ssl_server_ca' to " + pin.toSet() + "." + pin.hint()
		}
		return fmt.Sprintf("The server certificate is not signed by a public certificate authority, "+
			"nor by Amazon RDS or Google Cloud SQL's shared CA.\n\n"+
			"To fix this, either:\n"+
			"- set 'ssl_server_ca' to %s, or\n"+
			"- set 'sslmode' to %q.%s",
			pin.toSet(), ModeRequired, pin.hint())
	}
	return ""
}

// certPin is the certificate 'ssl_server_ca' could be set to so that the chain
// the server presented verifies, or the zero value when there is no such
// certificate.
type certPin struct {
	cert *x509.Certificate
	// leaf is set when cert is the server's own self-signed certificate rather
	// than a separate CA that signed it.
	leaf bool
}

// findPinnableCert finds the certificate to pin in the chain the server
// presented: the self-signed CA presented alongside the server certificate and
// which signed it, as MySQL presents its auto-generated CA, or the server
// certificate itself when it is self-signed.
func findPinnableCert(chain []*x509.Certificate) certPin {
	if len(chain) == 0 {
		return certPin{}
	}
	var leaf, rest = chain[0], chain[1:]
	if len(rest) == 0 {
		if isSelfSigned(leaf) {
			return certPin{cert: leaf, leaf: true}
		}
		return certPin{}
	}
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
			return certPin{cert: candidate}
		}
	}
	return certPin{}
}

// toSet describes the certificate to set 'ssl_server_ca' to, pointing at the
// one hint shows when there is one.
func (p certPin) toSet() string {
	switch {
	case p.cert == nil:
		return "the CA certificate that signed the server certificate"
	case p.leaf:
		return "the self-signed certificate below"
	}
	return "the CA certificate below"
}

// hint shows the certificate to pin, so that it can be copied into
// 'ssl_server_ca', or returns "" when there is none.
func (p certPin) hint() string {
	if p.cert == nil {
		return ""
	}
	var kind, owner = "CA certificate", "your server's CA certificate"
	if p.leaf {
		kind, owner = "self-signed certificate", "your server's certificate"
	}
	return fmt.Sprintf("\n\nThe server presented this %s (%s). "+
		"This connection was not verified, so confirm that it matches %s before using it:\n%s",
		kind, p.cert.Subject, owner,
		strings.TrimSpace(string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: p.cert.Raw}))))
}

// isMariadbEphemeralCert reports whether chain is MariaDB's auto-generated
// certificate: a lone self-signed certificate with its fixed Common Name and
// no names. It changes on every server restart, so pinning it doesn't help;
// MariaDB's own clients verify it through the password exchange instead, which
// the MySQL protocol as we speak it doesn't support.
func isMariadbEphemeralCert(chain []*x509.Certificate) bool {
	return len(chain) == 1 && chain[0].Subject.CommonName == mariadbEphemeralCN &&
		len(certificateNames(chain[0])) == 0 && isSelfSigned(chain[0])
}

// isSelfSigned reports whether cert is signed by its own key. Unlike
// CheckSignatureFrom it doesn't require cert to be marked as a CA, which a
// server's self-signed leaf certificate typically isn't.
func isSelfSigned(cert *x509.Certificate) bool {
	return bytes.Equal(cert.RawIssuer, cert.RawSubject) &&
		cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature) == nil
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

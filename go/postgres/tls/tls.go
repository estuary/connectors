// Package pgtls adjusts the TLS configuration that pgx builds for connections
// to a PostgreSQL server.
package pgtls

import (
	"github.com/estuary/connectors/go/cloudcas"
	"github.com/jackc/pgx/v5/pgconn"
)

// defaultRoots is the CA pool that the verifying modes trust. Tests replace it
// with a pool that holds their own CA.
var defaultRoots = cloudcas.DefaultRoots

// Configure changes the TLS configuration that pgconn.ParseConfig built for
// the given 'sslmode'. Call it after parsing and before connecting.
//
// Under 'verify-ca' and 'verify-full', pgx trusts only the system root store.
// Amazon RDS and Aurora certificates don't chain to it, so Configure adds the
// bundled managed-database CAs. Other modes are left unchanged.
//
// Under 'verify-full', a non-empty serverHost replaces the name the
// certificate is checked against. Pass it only when pgx dials a different
// name than the server's, as through a network tunnel, where pgx dials
// localhost. An empty serverHost keeps the name pgx parsed for each host,
// which is right for an address that lists several hosts.
func Configure(cfg *pgconn.Config, sslmode, serverHost string) {
	if sslmode != "verify-ca" && sslmode != "verify-full" {
		return
	}
	var configs = []*pgconn.FallbackConfig{{TLSConfig: cfg.TLSConfig}}
	configs = append(configs, cfg.Fallbacks...)
	for _, c := range configs {
		// The fields are set on pgx's own *tls.Config, not on a replacement.
		// pgx's 'verify-ca' check reads RootCAs from that exact value at
		// handshake time, so a replacement would be silently ignored.
		if c.TLSConfig == nil {
			continue
		}
		if c.TLSConfig.RootCAs == nil {
			c.TLSConfig.RootCAs = defaultRoots()
		}
		if sslmode == "verify-full" && serverHost != "" {
			c.TLSConfig.ServerName = serverHost
		}
	}
}

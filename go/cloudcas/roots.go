// Package cloudcas bundles the CAs of managed database services whose server
// certificates don't chain to a public root, such as Amazon RDS and Aurora.
package cloudcas

import (
	"crypto/x509"
	"embed"
	"io/fs"
	"sync"
)

// cloudCAs holds the bundled CA certificates. Refresh with update.sh.
//
//go:embed *.pem
var cloudCAs embed.FS

// DefaultRoots is the system root store plus the bundled managed-database CAs.
// Connectors trust it when they verify a server and the user configured no CA.
// The pool is only ever read, so it is built once and shared.
var DefaultRoots = sync.OnceValue(func() *x509.CertPool {
	var pool, err = x509.SystemCertPool()
	if err != nil {
		// Only the bundled CAs will be trusted, which is still fail-closed.
		pool = x509.NewCertPool()
	}
	// Reading embedded files cannot fail, and TestDefaultRootsTrustCloudCAs
	// checks that every bundled certificate parses.
	var names, _ = fs.Glob(cloudCAs, "*.pem")
	for _, name := range names {
		var pem, _ = cloudCAs.ReadFile(name)
		pool.AppendCertsFromPEM(pem)
	}
	return pool
})

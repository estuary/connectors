package mysqltls

import (
	"crypto/x509"
	"embed"
	"io/fs"
	"sync"
)

// cloudCAs holds the CA bundles of managed MySQL services whose server
// certificates don't chain to a public root. Refresh with cloudcas/update.sh.
//
//go:embed cloudcas/*.pem
var cloudCAs embed.FS

// defaultRoots is the pool 'verify_identity' trusts when no 'ssl_server_ca' is
// configured: the system root store plus the bundled managed-database CAs. The
// pool is only ever read, so it is built once and shared.
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

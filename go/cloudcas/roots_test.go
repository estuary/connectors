package cloudcas

import (
	"crypto/x509"
	"encoding/pem"
	"io/fs"
	"testing"

	"github.com/stretchr/testify/require"
)

// DefaultRoots skips any bundled certificate that fails to parse, so this is
// what guarantees that every one of them is trusted.
func TestDefaultRootsTrustCloudCAs(t *testing.T) {
	var pool = DefaultRoots()
	names, err := fs.Glob(cloudCAs, "*.pem")
	require.NoError(t, err)
	require.NotEmpty(t, names)

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			var rest, err = cloudCAs.ReadFile(name)
			require.NoError(t, err)
			var count int
			for {
				var block *pem.Block
				if block, rest = pem.Decode(rest); block == nil {
					break
				}
				cert, err := x509.ParseCertificate(block.Bytes)
				require.NoError(t, err)
				// Every bundled certificate is itself a trust anchor in the pool.
				_, err = cert.Verify(x509.VerifyOptions{
					Roots:     pool,
					KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
					// Checks only that the certificate is in the pool, not whether it has
					// expired, which the refresh workflow tracks.
					CurrentTime: cert.NotBefore,
				})
				require.NoError(t, err, "%s: %s", name, cert.Subject)
				count++
			}
			require.NotZero(t, count)
		})
	}
}

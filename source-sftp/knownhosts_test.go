package main

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

func newEd25519Key(t *testing.T) (ssh.PublicKey, ssh.Signer) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(priv)
	require.NoError(t, err)
	sshPub, err := ssh.NewPublicKey(pub)
	require.NoError(t, err)
	return sshPub, signer
}

func newRSAKey(t *testing.T) (ssh.PublicKey, ssh.Signer) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(priv)
	require.NoError(t, err)
	return signer.PublicKey(), signer
}

func newECDSAKey(t *testing.T) (ssh.PublicKey, ssh.Signer) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(priv)
	require.NoError(t, err)
	return signer.PublicKey(), signer
}

// keyLine renders a key as it appears in a known_hosts line, without a host
// column.
func keyLine(key ssh.PublicKey) string {
	return key.Type() + " " + base64.StdEncoding.EncodeToString(key.Marshal())
}

var testRemote = &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 2222}

func TestParseKnownHosts(t *testing.T) {
	edKey, _ := newEd25519Key(t)

	for _, tt := range []struct {
		name    string
		content string
		wantErr string
		wantNil bool
	}{
		{
			name:    "known_hosts line with bracketed port",
			content: "[myserver.com]:2222 " + keyLine(edKey),
		},
		{
			name:    "cert-authority marker",
			content: "@cert-authority *.example.com " + keyLine(edKey),
		},
		{
			name:    "comments and blank lines are skipped",
			content: "# a comment\n\n   \n[myserver.com]:2222 " + keyLine(edKey) + "\n",
		},
		{
			name:    "garbage line",
			content: "not a host key",
			wantErr: "line 1",
		},
		{
			name:    "fingerprints are not accepted",
			content: "ssh-ed25519 " + ssh.FingerprintSHA256(edKey),
			wantErr: "line 1",
		},
		{
			name:    "error names the offending line",
			content: "[myserver.com]:2222 " + keyLine(edKey) + "\n\nbroken",
			wantErr: "line 3",
		},
		{
			name:    "multi-word trailing comment is accepted",
			content: "[myserver.com]:2222 " + keyLine(edKey) + " root@build server 3",
		},
		{
			name:    "key type column must match the key",
			content: "[myserver.com]:2222 ssh-rsa " + base64.StdEncoding.EncodeToString(edKey.Marshal()),
			wantErr: "line 1",
		},
		{
			name:    "unknown marker is rejected",
			content: "@foo [myserver.com]:2222 " + keyLine(edKey),
			wantErr: "line 1",
		},
		{
			name:    "bracketed host without a port is rejected on the right line",
			content: "# leading comment\n\n[myserver.com " + keyLine(edKey),
			wantErr: "line 3",
		},
		{
			name:    "empty content means no pins",
			content: "",
			wantNil: true,
		},
		{
			name:    "whitespace-only content means no pins",
			content: " \n\t\n",
			wantNil: true,
		},
		{
			name:    "comment-only content is an error, not silently unverified",
			content: "# paste key here\n",
			wantErr: "no host key entries",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pins, err := parseKnownHosts(tt.content)
			switch {
			case tt.wantErr != "":
				require.ErrorContains(t, err, tt.wantErr)
				require.NotContains(t, err.Error(), os.TempDir(), "temp file path leaked into a user-facing error")
			case tt.wantNil:
				require.NoError(t, err)
				require.Nil(t, pins)
			default:
				require.NoError(t, err)
				require.NotNil(t, pins)
				require.Len(t, pins.entries, 1)
			}
		})
	}
}

func TestHostKeyCallback(t *testing.T) {
	serverKey, _ := newEd25519Key(t)
	otherKey, _ := newEd25519Key(t)
	rsaKey, _ := newRSAKey(t)
	serverKeyB64 := base64.StdEncoding.EncodeToString(serverKey.Marshal())
	const address = "myserver.com:2222"

	for _, tt := range []struct {
		name      string
		address   string // defaults to `address`
		content   string
		presented ssh.PublicKey
		wantErr   []string // substrings the error must contain; empty means accept
		forbid    []string // substrings the error must not contain
	}{
		{
			name:      "known_hosts entry accepts the pinned key",
			content:   "[myserver.com]:2222 " + keyLine(serverKey),
			presented: serverKey,
		},
		{
			name:      "hashed host entry accepts",
			content:   knownhosts.HashHostname(knownhosts.Normalize(address)) + " " + keyLine(serverKey),
			presented: serverKey,
		},
		{
			name:      "hashed host entry still detects a mismatch",
			content:   knownhosts.HashHostname(knownhosts.Normalize(address)) + " " + keyLine(otherKey),
			presented: serverKey,
			wantErr:   []string{"does not match"},
			forbid:    []string{serverKeyB64},
		},
		{
			name:      "comma-separated host list accepts",
			content:   "[a.example.com]:2222,[myserver.com]:2222,10.0.0.1 " + keyLine(serverKey),
			presented: serverKey,
		},
		{
			name:      "IPv6 literal with port accepts",
			address:   "[::1]:2222",
			content:   "[::1]:2222 " + keyLine(serverKey),
			presented: serverKey,
		},
		{
			name:      "IPv6 literal on the default port accepts the bare form",
			address:   "[2001:db8::10]:22",
			content:   knownhosts.Line([]string{"[2001:db8::10]:22"}, serverKey),
			presented: serverKey,
		},
		{
			name:      "IPv6 literal on another port is not the default-port entry",
			address:   "[2001:db8::10]:2222",
			content:   knownhosts.Line([]string{"[2001:db8::10]:22"}, serverKey),
			presented: serverKey,
			wantErr:   []string{"have no entry for", "[2001:db8::10]:2222"},
		},
		{
			name:      "known_hosts wildcard entry accepts",
			content:   "[*.com]:2222 " + keyLine(serverKey),
			presented: serverKey,
		},
		{
			name:      "second of several keys accepts",
			content:   "[myserver.com]:2222 " + keyLine(otherKey) + "\n[myserver.com]:2222 " + keyLine(serverKey),
			presented: serverKey,
		},
		{
			name:      "mismatch is refused without echoing the presented key",
			content:   "[myserver.com]:2222 " + keyLine(otherKey),
			presented: serverKey,
			wantErr:   []string{"does not match", "ssh-ed25519"},
			forbid:    []string{serverKeyB64, ssh.FingerprintSHA256(serverKey)},
		},
		{
			name:      "mismatch of another key type is still a mismatch",
			content:   "[myserver.com]:2222 " + keyLine(otherKey),
			presented: rsaKey,
			wantErr:   []string{"does not match", "ssh-rsa", "ssh-ed25519"},
			forbid:    []string{base64.StdEncoding.EncodeToString(rsaKey.Marshal()), ssh.FingerprintSHA256(rsaKey)},
		},
		{
			name:      "unknown host echoes a paste-ready line and fingerprint",
			content:   "[elsewhere.com]:2222 " + keyLine(otherKey),
			presented: serverKey,
			wantErr:   []string{"have no entry for", knownhosts.Line([]string{address}, serverKey), ssh.FingerprintSHA256(serverKey)},
		},
		{
			name:      "key pinned under another host column names the expected column",
			content:   "localhost " + keyLine(serverKey),
			presented: serverKey,
			wantErr:   []string{"have no entry for", `"localhost"`, "[myserver.com]:2222", knownhosts.Line([]string{address}, serverKey)},
		},
		{
			name:      "port must match",
			content:   "myserver.com " + keyLine(serverKey),
			presented: serverKey,
			wantErr:   []string{"have no entry for", "[myserver.com]:2222"},
		},
		{
			name:      "revoked key is refused",
			content:   "@revoked * " + keyLine(serverKey),
			presented: serverKey,
			wantErr:   []string{"revoked"},
			forbid:    []string{serverKeyB64},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			address := address
			if tt.address != "" {
				address = tt.address
			}
			pins, err := parseKnownHosts(tt.content)
			require.NoError(t, err)
			cb := pins.callback(address)

			err = cb(address, testRemote, tt.presented)
			if len(tt.wantErr) == 0 {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, want := range tt.wantErr {
				require.Contains(t, err.Error(), want)
			}
			for _, forbid := range tt.forbid {
				require.NotContains(t, err.Error(), forbid)
			}
		})
	}
}

func TestHostKeyCallbackCertificateAuthority(t *testing.T) {
	caKey, caSigner := newEd25519Key(t)
	hostKey, _ := newEd25519Key(t)
	_, rogueCASigner := newEd25519Key(t)
	const address = "sftp.example.com:22"

	sign := func(signer ssh.Signer) ssh.PublicKey {
		cert := &ssh.Certificate{
			Key:             hostKey,
			CertType:        ssh.HostCert,
			ValidPrincipals: []string{"sftp.example.com"},
			ValidBefore:     ssh.CertTimeInfinity,
		}
		require.NoError(t, cert.SignCert(rand.Reader, signer))
		return cert
	}

	pins, err := parseKnownHosts("@cert-authority *.example.com " + keyLine(caKey))
	require.NoError(t, err)
	cb := pins.callback(address)

	require.NoError(t, cb(address, testRemote, sign(caSigner)))

	err = cb(address, testRemote, sign(rogueCASigner))
	require.Error(t, err)
	require.NotContains(t, err.Error(), base64.StdEncoding.EncodeToString(hostKey.Marshal()))

	// A bare key signed by nobody is not accepted by a CA-only pin either, and is not echoed: the
	// CA applies to this host, so this is a mismatch rather than an unknown host.
	err = cb(address, testRemote, hostKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match")
	require.Contains(t, err.Error(), "configured: @cert-authority")
	require.NotContains(t, err.Error(), base64.StdEncoding.EncodeToString(hostKey.Marshal()))

	// A CA whose host pattern does not cover the address applies to nothing here, so a bare key is
	// an unknown host: the user has no pin for this server and gets the paste-ready line, not a
	// man-in-the-middle warning.
	pins, err = parseKnownHosts("@cert-authority *.other.com " + keyLine(caKey))
	require.NoError(t, err)
	cb = pins.callback(address)
	err = cb(address, testRemote, hostKey)
	require.Error(t, err)
	require.Contains(t, err.Error(), "have no entry for")
	require.Contains(t, err.Error(), knownhosts.Line([]string{address}, hostKey))
	require.NotContains(t, err.Error(), "man-in-the-middle")
}

// TestHostKeyCallbackRevokedThroughCertificate pins that @revoked outranks @cert-authority: a key
// revoked after a compromise must not stay usable through a certificate the CA issued for it
// before the revocation.
func TestHostKeyCallbackRevokedThroughCertificate(t *testing.T) {
	caKey, caSigner := newEd25519Key(t)
	hostKey, _ := newEd25519Key(t)
	otherKey, _ := newEd25519Key(t)
	const address = "sftp.example.com:22"

	cert := &ssh.Certificate{
		Key:             hostKey,
		CertType:        ssh.HostCert,
		ValidPrincipals: []string{"sftp.example.com"},
		ValidBefore:     ssh.CertTimeInfinity,
	}
	require.NoError(t, cert.SignCert(rand.Reader, caSigner))

	pins, err := parseKnownHosts("@cert-authority *.example.com " + keyLine(caKey) + "\n@revoked * " + keyLine(hostKey))
	require.NoError(t, err)
	err = pins.callback(address)(address, testRemote, cert)
	require.Error(t, err)
	require.Contains(t, err.Error(), "revoked")
	require.NotContains(t, err.Error(), base64.StdEncoding.EncodeToString(hostKey.Marshal()))

	// Revoking some other key leaves the certificate acceptable.
	pins, err = parseKnownHosts("@cert-authority *.example.com " + keyLine(caKey) + "\n@revoked * " + keyLine(otherKey))
	require.NoError(t, err)
	require.NoError(t, pins.callback(address)(address, testRemote, cert))
}

// TestConfigValidateKnownHosts pins the wiring of the knownHosts field into config.Validate, which
// is where a user first hears about a bad paste. It also covers the Skip Host Key Verification
// opt-out.
func TestConfigValidateKnownHosts(t *testing.T) {
	edKey, _ := newEd25519Key(t)
	valid := config{
		Address:     "myserver.com:2222",
		Directory:   "/data",
		Credentials: credentialsConfig{Type: "password", Username: "u", Password: "p"},
	}
	require.NoError(t, valid.Validate())

	cfg := valid
	cfg.KnownHosts = "[myserver.com]:2222 " + keyLine(edKey)
	require.NoError(t, cfg.Validate())

	cfg = valid
	cfg.KnownHosts = "[myserver.com]:2222 " + keyLine(edKey) + "\nnot a host key"
	require.ErrorContains(t, cfg.Validate(), "knownHosts line 2")

	cfg = valid
	cfg.SkipHostKeyVerification = true
	require.NoError(t, cfg.Validate())

	cfg = valid
	cfg.SkipHostKeyVerification = true
	cfg.KnownHosts = "[myserver.com]:2222 " + keyLine(edKey)
	require.ErrorContains(t, cfg.Validate(), "not both")
}

func TestHostKeyAlgorithms(t *testing.T) {
	edKey, _ := newEd25519Key(t)
	rsaKey, _ := newRSAKey(t)
	var certAlgos []string
	for _, algo := range ssh.SupportedAlgorithms().HostKeys {
		if strings.Contains(algo, "-cert-v01@") {
			certAlgos = append(certAlgos, algo)
		}
	}

	for _, tt := range []struct {
		name    string
		address string // defaults to "host:22"
		content string
		algos   []string
	}{
		{
			name:    "ed25519 pin negotiates only ed25519",
			content: "host " + keyLine(edKey),
			algos:   []string{ssh.KeyAlgoED25519},
		},
		{
			name:    "rsa pin prefers the SHA-2 signature algorithms",
			content: "host " + keyLine(rsaKey),
			algos:   []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSA},
		},
		{
			name:    "multiple pins keep configured order",
			content: "host " + keyLine(edKey) + "\nhost " + keyLine(rsaKey),
			algos:   []string{ssh.KeyAlgoED25519, ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSA},
		},
		{
			name:    "cert-authority negotiates every certificate algorithm",
			content: "@cert-authority * " + keyLine(edKey),
			algos:   certAlgos,
		},
		{
			name:    "revoked entries contribute nothing",
			content: "@revoked * " + keyLine(edKey),
			algos:   nil,
		},
		{
			// A key type pinned for some other server must not reorder negotiation
			// for this one.
			name:    "an entry for another host contributes nothing",
			content: "otherhost " + keyLine(rsaKey) + "\nhost " + keyLine(edKey),
			algos:   []string{ssh.KeyAlgoED25519},
		},
		{
			name:    "the port is part of the host, so another port is another host",
			content: "[host]:2222 " + keyLine(rsaKey) + "\nhost " + keyLine(edKey),
			algos:   []string{ssh.KeyAlgoED25519},
		},
		{
			name:    "a wildcard entry still applies",
			content: "*.example.com " + keyLine(rsaKey),
			address: "sftp.example.com:22",
			algos:   []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSA},
		},
		{
			// Nothing to steer towards leaves the defaults in force, so the server's
			// key reaches the callback and the user gets the paste-ready
			// unknown-host message instead of an opaque negotiation failure.
			name:    "no applicable entry negotiates the defaults",
			content: "otherhost " + keyLine(edKey),
			algos:   nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			address := "host:22"
			if tt.address != "" {
				address = tt.address
			}
			pins, err := parseKnownHosts(tt.content)
			require.NoError(t, err)
			require.Equal(t, tt.algos, pins.hostKeyAlgorithms(address))
		})
	}
}

// TestHostKeyNegotiation dials an in-process SSH server holding both an RSA
// and an ed25519 host key. Go's default preference order would negotiate RSA;
// a pin on the ed25519 key must steer negotiation so that a correct pin
// connects.
func TestHostKeyNegotiation(t *testing.T) {
	_, rsaSigner := newRSAKey(t)
	edKey, edSigner := newEd25519Key(t)
	_, otherSigner := newEd25519Key(t)

	serverConfig := &ssh.ServerConfig{NoClientAuth: true}
	serverConfig.AddHostKey(rsaSigner)
	serverConfig.AddHostKey(edSigner)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				sconn, chans, reqs, err := ssh.NewServerConn(conn, serverConfig)
				if err != nil {
					return
				}
				go ssh.DiscardRequests(reqs)
				for ch := range chans {
					ch.Reject(ssh.Prohibited, "test server")
				}
				sconn.Close()
			}()
		}
	}()
	address := listener.Addr().String()

	dial := func(t *testing.T, content string) error {
		pins, err := parseKnownHosts(content)
		require.NoError(t, err)
		clientConfig := &ssh.ClientConfig{
			HostKeyCallback:   pins.callback(address),
			HostKeyAlgorithms: pins.hostKeyAlgorithms(address),
		}
		client, err := ssh.Dial("tcp", address, clientConfig)
		if err == nil {
			client.Close()
		}
		return err
	}

	t.Run("correct ed25519 pin connects", func(t *testing.T) {
		require.NoError(t, dial(t, knownhosts.Line([]string{address}, edKey)))
	})
	t.Run("wrong key is refused", func(t *testing.T) {
		err := dial(t, knownhosts.Line([]string{address}, otherSigner.PublicKey()))
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not match")
	})
	t.Run("pin of a key type the server does not hold fails in negotiation", func(t *testing.T) {
		// Only pinned key types are negotiated, so the handshake itself fails, and
		// the ssh package names both sides' algorithms in the error.
		ecdsaKey, _ := newECDSAKey(t)
		err := dial(t, knownhosts.Line([]string{address}, ecdsaKey))
		require.Error(t, err)
		require.Contains(t, err.Error(), "no common algorithm for host key")
		require.Contains(t, err.Error(), ssh.KeyAlgoECDSA256)
		require.Contains(t, err.Error(), ssh.KeyAlgoED25519)
	})
	t.Run("a correct pin connects despite an entry for an unrelated host", func(t *testing.T) {
		// The regression this guards: the unrelated RSA entry used to contribute
		// rsa-sha2-* ahead of ed25519, so this dual-key server presented its RSA
		// key and the correct ed25519 pin below was reported as a possible
		// man-in-the-middle attack.
		require.NoError(t, dial(t, "[unrelated.example.com]:2222 "+keyLine(rsaSigner.PublicKey())+
			"\n"+knownhosts.Line([]string{address}, edKey)))
	})
	t.Run("unknown host is refused and echoes the presented key", func(t *testing.T) {
		// No entry covers this address, so nothing steers negotiation and the
		// defaults apply. Whichever key the server then presents must come back
		// paste-ready, rather than the handshake failing with "no common algorithm
		// for host key".
		err := dial(t, "[elsewhere]:1 "+keyLine(otherSigner.PublicKey()))
		require.Error(t, err)
		require.Contains(t, err.Error(), "have no entry for")
		require.Contains(t, err.Error(), address)
		presented := []string{keyLine(edKey), keyLine(rsaSigner.PublicKey())}
		require.True(t,
			strings.Contains(err.Error(), presented[0]) || strings.Contains(err.Error(), presented[1]),
			"error must echo one of the server's own host keys, got: %v", err)
	})
}

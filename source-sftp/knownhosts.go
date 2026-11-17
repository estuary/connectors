package main

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strings"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// knownHosts is the parsed form of the `knownHosts` config field: OpenSSH
// known_hosts content, one entry per line, for example
//
//	[myserver.com]:2222 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5...
//	@cert-authority *.example.com ssh-rsa AAAAB3NzaC1yc2E...
//
// The content is handed to golang.org/x/crypto/ssh/knownhosts verbatim, which
// is what gives us host patterns, hashed hosts, multiple keys per host,
// @cert-authority and @revoked; `verify` is the callback it built. knownhosts
// is also the authority on what is a valid line: any line it can't parse is
// rejected when the config is validated, rather than silently skipped.
// The entries are kept parsed for diagnostics and for deriving the host key
// algorithm preference.
type knownHosts struct {
	verify  ssh.HostKeyCallback
	entries []knownHostEntry
}

type knownHostEntry struct {
	marker string // "", "cert-authority" or "revoked"
	hosts  []string
	key    ssh.PublicKey
}

// lineError reports a line of the `knownHosts` field that is not a known_hosts
// entry.
func lineError(line int, cause error) error {
	return fmt.Errorf("knownHosts line %d: not a known_hosts entry ('[host]:port key-type base64-key', as printed by ssh-keyscan): %v", line, cause)
}

// newKnownHostsCallback builds the host key callback for the field's content.
// knownhosts skips lines it can't parse as long as another line is valid, so
// each line is first handed to it on its own, where a bad line is an error
// that can be reported with its line number in the field.
func newKnownHostsCallback(content string) (ssh.HostKeyCallback, error) {
	for i, line := range strings.Split(content, "\n") {
		if strings.TrimSuffix(line, "\r") == "" {
			continue
		}
		if _, err := knownhostsNew(line); err != nil {
			if _, ok := errors.AsType[*knownhostsError](err); ok {
				return nil, lineError(i+1, err)
			}
			return nil, err
		}
	}

	cb, err := knownhostsNew(content)
	if err != nil {
		return nil, fmt.Errorf("parsing knownHosts: %w", err)
	}
	return cb, nil
}

// knownhostsError is knownhosts rejecting the content, as opposed to the temp
// file for it failing to be written.
type knownhostsError struct{ cause string }

func (e *knownhostsError) Error() string { return e.cause }

// knownhostsNew runs knownhosts.New over content, which it only reads from
// files. The file is temporary and means nothing to the user, so its path is
// kept out of the error.
func knownhostsNew(content string) (ssh.HostKeyCallback, error) {
	f, err := os.CreateTemp("", "known_hosts")
	if err != nil {
		return nil, fmt.Errorf("creating known_hosts file: %w", err)
	}
	defer os.Remove(f.Name())

	if _, err := f.WriteString(content); err != nil {
		f.Close()
		return nil, fmt.Errorf("writing known_hosts file: %w", err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("writing known_hosts file: %w", err)
	}

	cb, err := knownhosts.New(f.Name())
	if err != nil {
		// knownhosts reports "knownhosts: <file>: no valid entries, line <n>:
		// <cause>". Keep only the cause, and should the format change, still
		// drop the path.
		msg := err.Error()
		if rest, ok := strings.CutPrefix(msg, "knownhosts: "+f.Name()+": no valid entries, line "); ok {
			if _, cause, ok := strings.Cut(rest, ": "); ok {
				return nil, &knownhostsError{cause: cause}
			}
		}
		return nil, &knownhostsError{cause: strings.ReplaceAll(msg, f.Name(), "known_hosts")}
	}
	return cb, nil
}

// parseKnownHosts parses the `knownHosts` config field. It returns nil, nil
// for empty content. Content that has text but no entries (only comments, say)
// is an error.
func parseKnownHosts(content string) (*knownHosts, error) {
	if strings.TrimSpace(content) == "" {
		return nil, nil
	}

	verify, err := newKnownHostsCallback(content)
	if err != nil {
		return nil, err
	}
	pins := knownHosts{verify: verify}

	for i, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// ssh.ParseKnownHosts allows a single trailing comment word where
		// knownhosts allows any number, so only the entry's own columns are handed
		// to it.
		fields := strings.Fields(line)
		columns := 3
		if strings.HasPrefix(fields[0], "@") {
			columns = 4
		}
		if len(fields) > columns {
			fields = fields[:columns]
		}

		marker, hosts, key, _, _, err := ssh.ParseKnownHosts([]byte(strings.Join(fields, " ")))
		if err != nil {
			return nil, lineError(i+1, err)
		}
		pins.entries = append(pins.entries, knownHostEntry{marker: marker, hosts: hosts, key: key})
	}

	if len(pins.entries) == 0 {
		return nil, errors.New("knownHosts has no host key entries, only comments or blank lines. Paste the output of ssh-keyscan")
	}
	return &pins, nil
}

// isRevoked reports whether key is pinned as @revoked.
func (p *knownHosts) isRevoked(key ssh.PublicKey) bool {
	return slices.ContainsFunc(p.entries, func(e knownHostEntry) bool {
		return e.marker == "revoked" && bytes.Equal(e.key.Marshal(), key.Marshal())
	})
}

// isCertAuthority reports whether key is pinned as a @cert-authority.
func (p *knownHosts) isCertAuthority(key ssh.PublicKey) bool {
	return slices.ContainsFunc(p.entries, func(e knownHostEntry) bool {
		return e.marker == "cert-authority" && bytes.Equal(e.key.Marshal(), key.Marshal())
	})
}

// mismatchError reports a key that is not among those pinned for the host. It
// deliberately does not include the presented key: we do not want to invite
// the user to paste it into the configuration.
func (p *knownHosts) mismatchError(address string, key ssh.PublicKey, pinnedTypes []string) error {
	slices.Sort(pinnedTypes)
	return fmt.Errorf("the %s host key presented by %s does not match any entry for that host in the configured SSH Known Hosts (configured: %s). "+
		"This may indicate a man-in-the-middle attack or a host key rotation. "+
		"Verify the server's current host key before updating the SSH Known Hosts configuration",
		key.Type(), address, strings.Join(slices.Compact(pinnedTypes), ", "))
}

// unknownHostError reports that no pinned entry applies to the host at all.
// Here the presented key is echoed, paste-ready, since the user has no pin yet
// and must obtain one somehow.
func (p *knownHosts) unknownHostError(address string, key ssh.PublicKey) error {
	var hint string
	for _, entry := range p.entries {
		if entry.marker == "" && bytes.Equal(entry.key.Marshal(), key.Marshal()) {
			hint = fmt.Sprintf(" The presented key is configured for host %q, but the connector connects to %q, so its known_hosts entry must name %q.",
				strings.Join(entry.hosts, ","), address, knownhosts.Normalize(address))
			break
		}
	}
	return fmt.Errorf("the configured SSH Known Hosts have no entry for %s.%s The server presented the %s key '%s' (fingerprint %s). "+
		"Verify this is the server's genuine host key, then add that line to the SSH Known Hosts configuration",
		address, hint, key.Type(), knownhosts.Line([]string{address}, key), ssh.FingerprintSHA256(key))
}

// callback builds the ssh.HostKeyCallback verifying the server at `address`
// (the configured host:port, which is also what ssh.Dial is given) against the
// pins.
func (p *knownHosts) callback(address string) ssh.HostKeyCallback {
	return func(hostname string, remote net.Addr, key ssh.PublicKey) error {
		if cert, ok := key.(*ssh.Certificate); ok && p.isRevoked(cert.Key) {
			return fmt.Errorf("the %s host key in the certificate presented by %s is revoked by the configured SSH Known Hosts", cert.Key.Type(), address)
		}

		err := p.verify(hostname, remote, key)
		if err == nil {
			return nil
		}

		if _, ok := errors.AsType[*knownhosts.RevokedError](err); ok {
			return fmt.Errorf("the %s host key presented by %s is revoked by the configured SSH Known Hosts", key.Type(), address)
		}
		if keyErr, ok := errors.AsType[*knownhosts.KeyError](err); ok && len(keyErr.Want) > 0 {
			// Want holds every entry that applies to the host, @cert-authority lines
			// included: a CA is pinned and the server offered a bare key instead of
			// a certificate.
			var pinned []string
			for _, want := range keyErr.Want {
				if p.isCertAuthority(want.Key) {
					pinned = append(pinned, "@cert-authority")
				} else {
					pinned = append(pinned, want.Key.Type())
				}
			}
			return p.mismatchError(address, key, pinned)
		}
		if _, ok := errors.AsType[*knownhosts.KeyError](err); ok {
			// Nothing applies to the host, whatever else is configured.
			return p.unknownHostError(address, key)
		}
		// Certificate checks report plain errors: not signed by a configured
		// authority, expired, wrong principal, revoked CA. Never echo the
		// presented certificate.
		return fmt.Errorf("verifying the %s host key presented by %s against the configured SSH Known Hosts: %w", key.Type(), address, err)
	}
}

// hostAddr adapts the configured address to the net.Addr that the knownhosts
// callback takes. That callback prefers the hostname it is handed over the
// remote address and only needs the remote to split into host and port, so the
// configured address serves as both.
type hostAddr string

func (hostAddr) Network() string  { return "tcp" }
func (a hostAddr) String() string { return string(a) }

// appliesTo reports whether the entry's host patterns cover address.
func (p *knownHosts) appliesTo(entry knownHostEntry, address string) bool {
	return entry.marker != "revoked" && p.verify(address, hostAddr(address), entry.key) == nil
}

// algorithmsForKeyType maps a public key type to the host key (signature)
// algorithms a server may use to prove possession of it. RSA keys sign under
// several algorithm names; every other type signs under its own name.
func algorithmsForKeyType(keyType string) []string {
	switch keyType {
	case ssh.KeyAlgoRSA:
		return []string{ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSA}
	case ssh.CertAlgoRSAv01:
		return []string{ssh.CertAlgoRSASHA512v01, ssh.CertAlgoRSASHA256v01, ssh.CertAlgoRSAv01}
	default:
		return []string{keyType}
	}
}

// hostKeyAlgorithms derives the host key algorithms to negotiate from the pins
// that apply to `address`, in configured order. The server presents the first
// key type on this list that it holds, so without it a server holding both an
// RSA and an ed25519 key would present the RSA key: ed25519 sits last in Go's
// default preference (ECDSA, then RSA, then DSA, then ed25519), and a correct
// ed25519 pin would fail as a mismatch. Unpinned key types are left out: a key
// of such a type could only ever fail verification.
//
// Only the entries that cover `address` steer anything.
func (p *knownHosts) hostKeyAlgorithms(address string) []string {
	var algos []string
	add := func(candidates ...string) {
		for _, c := range candidates {
			if !slices.Contains(algos, c) {
				algos = append(algos, c)
			}
		}
	}

	for _, entry := range p.entries {
		if !p.appliesTo(entry, address) {
			continue
		}
		switch entry.marker {
		case "cert-authority":
			// The CA key's type says nothing about the host key it signs, so prefer
			// every certificate algorithm.
			for _, algo := range ssh.SupportedAlgorithms().HostKeys {
				if strings.Contains(algo, "-cert-v01@") {
					add(algo)
				}
			}
		default:
			add(algorithmsForKeyType(entry.key.Type())...)
		}
	}

	return algos
}

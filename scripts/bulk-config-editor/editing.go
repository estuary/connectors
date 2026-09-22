package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"
)

// configEdit represents a single "set property foo to bar" edit to an endpoint config.
//
// We have to represent edits in this form so that we can either apply them ourselves
// (to plaintext configs) or by invoking `sops set` on an encrypted config.
type configEdit struct {
	Path  []string
	Value any
}

func (e *configEdit) String() string {
	return fmt.Sprintf("%s = %v", strings.Join(e.Path, "."), e.Value)
}

func editPlaintextTaskConfig(cfg any, edits []configEdit) (any, error) {
	for _, edit := range edits {
		modified, err := setAtPath(cfg, edit.Path, edit.Value)
		if err != nil {
			return nil, fmt.Errorf("error applying edit [%s]: %w", edit.String(), err)
		}
		cfg = modified
	}
	return cfg, nil
}

func editEncryptedTaskConfig(ctx context.Context, configFile string, edits []configEdit) error {
	// Execute sops commands to perform the specified edits, asking for YAML output to match
	// the YAML input that 'flowctl catalog pull-specs' writes.
	//
	// This previously requested JSON output on the theory that it mirrored our
	// config-encryption service and so produced the most minimal diffs. On the YAML files
	// flowctl actually writes it does the opposite: JSON output reserializes the whole file,
	// rewriting every line and rendering unset key groups ('pgp', 'age', ...) differently
	// than they appeared. Matching the input format keeps the diff to the edited property
	// plus the 'lastmodified' and 'mac' that any edit necessarily changes.
	if err := quoteSOPSTimestamps(configFile); err != nil {
		return fmt.Errorf("error normalizing SOPS metadata: %w", err)
	}

	for _, edit := range edits {
		log.WithField("edit", edit.String()).Debug("applying edit")

		// TODO(wgd): Implement unsetting if edit.Value is nil?
		var bs, err = json.Marshal(edit.Value)
		if err != nil {
			return fmt.Errorf("error serializing edited value: %w", err)
		}
		var cmd = exec.CommandContext(ctx, "sops", "set", "--output-type", "yaml", configFile, asPyDictIndex(edit.Path), string(bs))
		if _, err = cmd.Output(); err != nil {
			if err, ok := err.(*exec.ExitError); ok {
				return fmt.Errorf("error editing with SOPS: %s", strings.TrimSpace(string(err.Stderr)))
			}
			return fmt.Errorf("error editing with SOPS: %w", err)
		}
	}
	return nil
}

// sopsTimestampPattern matches the unquoted RFC3339 timestamps which 'flowctl catalog
// pull-specs' writes into a config's SOPS metadata stanza. The optional list marker
// covers the per-key 'created_at' entries under 'gcp_kms', 'kms', 'pgp' and friends.
var sopsTimestampPattern = regexp.MustCompile(`(?m)^(\s*(?:- )?(?:created_at|lastmodified):[ \t]+)([0-9]{4}-[0-9]{2}-[0-9]{2}T[^"'\s#]+)[ \t]*$`)

// quoteSOPSTimestamps rewrites those timestamps as quoted strings, and is a no-op on a
// file which doesn't have any.
//
// YAML parses a bare RFC3339 timestamp into a native timestamp, but SOPS decodes its own
// metadata into a struct whose 'created_at' and 'lastmodified' fields are strings. So
// 'sops set' fails on the files flowctl writes, with "expected type 'string', got
// unconvertible type 'time.Time'", before it even looks at the data we mean to edit.
// Quoting forces those values to parse as the strings SOPS expects.
//
// Only the metadata stanza is affected: every value in an encrypted config is an
// 'ENC[...]' string, so there are no bare timestamps elsewhere in the file to disturb.
func quoteSOPSTimestamps(configFile string) error {
	var original, err = os.ReadFile(configFile)
	if err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}
	var quoted = sopsTimestampPattern.ReplaceAll(original, []byte(`${1}"${2}"`))
	if bytes.Equal(quoted, original) {
		return nil
	}
	if err := os.WriteFile(configFile, quoted, 0664); err != nil {
		return fmt.Errorf("error writing file: %w", err)
	}
	log.WithField("name", configFile).Debug("quoted timestamps in SOPS metadata")
	return nil
}

func asPyDictIndex(path []string) string {
	var xs []string
	for _, elem := range path {
		// TODO(wgd): Consider supporting array indices?
		xs = append(xs, fmt.Sprintf(`[%q]`, elem))
	}
	return strings.Join(xs, "")
}

func setAtPath(x any, p []string, v any) (any, error) {
	if len(p) == 0 {
		return v, nil
	}

	if x == nil {
		x = make(map[string]any)
	}
	if obj, ok := x.(map[string]any); ok {
		modified, err := setAtPath(obj[p[0]], p[1:], v)
		if err != nil {
			return nil, err
		}
		obj[p[0]] = modified
		return obj, nil
	}

	return nil, fmt.Errorf("unable to modify %v at pointer %s", x, strings.Join(p, "."))
}

func indexPath(x any, path ...string) (any, bool) {
	for _, p := range path {
		if m, ok := x.(map[string]any); !ok {
			return nil, false
		} else if v, ok := m[p]; !ok {
			return nil, false
		} else {
			x = v
		}
	}
	return x, true
}

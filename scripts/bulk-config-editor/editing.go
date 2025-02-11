package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
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
	// Execute sops commands to perform the specified edits. Note that even though the input
	// is typically YAML we're explicitly requesting JSON output from SOPS.
	//
	// This is because SOPS handles unset properties in the SOPS stanza slightly differently
	// in YAML vs JSON output formats. In JSON an unset property is like `kms: null` but in
	// YAML it's like `kms: []`. Our config-encryption service produces JSON output which is
	// only YAML here because that's what 'flowctl catalog pull-specs' writes, so to produce
	// the most minimal diffs we need to mirror that "JSON from SOPS" behavior. Since YAML
	// is a superset of JSON it's fine to just leave the file in JSON format after editing.
	//
	// We could in principle have the 'list-tasks' helper script output JSON instead, but that
	// is surprisingly tricky. Just telling 'flowctl catalog pull-specs' to output JSON only
	// seems to impact the 'flow.json' file and the broken-out configs are still YAML. Also
	// it seemed better in general to make this script's behavior work with the default output
	// of a 'flowctl draft develop' if possible.
	for _, edit := range edits {
		log.WithField("edit", edit.String()).Debug("applying edit")

		// TODO(wgd): Implement unsetting if edit.Value is nil?
		var bs, err = json.Marshal(edit.Value)
		if err != nil {
			return fmt.Errorf("error serializing edited value: %w", err)
		}
		var cmd = exec.CommandContext(ctx, "sops", "set", "--output-type", "json", configFile, asPyDictIndex(edit.Path), string(bs))
		if _, err = cmd.Output(); err != nil {
			if err, ok := err.(*exec.ExitError); ok {
				return fmt.Errorf("error editing with SOPS: %s", strings.TrimSpace(string(err.Stderr)))
			}
			return fmt.Errorf("error editing with SOPS: %w", err)
		}
	}
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

package common

import (
	"bytes"
	"encoding/json"
	"fmt"

	log "github.com/sirupsen/logrus"
)

// EmitConfigUpdate emits a "configUpdate" connector event carrying a full
// restatement of the endpoint config. The control plane reacts to the event by
// republishing the task's spec with its endpoint config replaced wholesale by
// the emitted config, so config must be a complete config document and not a
// partial patch.
func EmitConfigUpdate(msg string, config json.RawMessage) {
	log.WithFields(log.Fields{
		"eventType": "configUpdate",
		"config":    config,
	}).Info(msg)
}

// SetSealedConfigProperty returns the task's sealed endpoint config with value
// set at the given object path, as a complete config document ready to be
// emitted with EmitConfigUpdate.
//
// A sops-encrypted config is never decrypted and re-encrypted to carry the new
// value. The value is written into the plaintext `sops.overlay` of the config's
// `sops` stanza instead, which the runtime merges over the decrypted config
// (RFC 7396 merge patch) once it has checked that every location the overlay
// touches is annotated `nonsensitive` in the connector's config schema. The
// ciphertext is left alone, so the config keeps the key it was encrypted with
// rather than being re-keyed to whatever an encryption service would pick, and
// no secret is ever sent anywhere to be re-sealed.
//
// Encrypted values are copied across untouched, but the document is otherwise
// re-serialized, which sorts its properties. That is safe despite sops computing
// its MAC over values in the order it encounters them: the control plane parses
// the emitted config into a `serde_json::Value` and re-serializes it when
// applying the update, which sorts it the same way, so a config that verifies
// after being stored verifies here too.
func SetSealedConfigProperty(sealed json.RawMessage, path []string, value any) (json.RawMessage, error) {
	var doc struct {
		Sops json.RawMessage `json:"sops"`
	}
	if err := json.Unmarshal(sealed, &doc); err != nil {
		return nil, fmt.Errorf("config is not a JSON object: %w", err)
	}

	// A `sops` stanza is what marks the config as encrypted, so its absence means
	// there is no ciphertext to protect and the value belongs in the document
	// itself. Adding a stanza to such a config would send the runtime looking for
	// ciphertext that isn't there.
	if len(doc.Sops) != 0 && !bytes.Equal(doc.Sops, []byte("null")) {
		path = append([]string{"sops", "overlay"}, path...)
	}

	return SetJSONProperty(sealed, path, value)
}

// SetJSONProperty returns doc with value set at the given object path,
// creating intermediate objects as needed. Path elements other than the last
// must be absent, null, or objects.
func SetJSONProperty(doc json.RawMessage, path []string, value any) (json.RawMessage, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("path must not be empty")
	}

	var root map[string]any
	var dec = json.NewDecoder(bytes.NewReader(doc))
	dec.UseNumber()
	if err := dec.Decode(&root); err != nil {
		return nil, fmt.Errorf("decoding document: %w", err)
	}

	var current = root
	for _, p := range path[:len(path)-1] {
		next, ok := current[p].(map[string]any)
		if !ok {
			if existing, present := current[p]; present && existing != nil {
				return nil, fmt.Errorf("property %q is not an object", p)
			}
			next = make(map[string]any)
			current[p] = next
		}
		current = next
	}
	current[path[len(path)-1]] = value

	return json.Marshal(root)
}

package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
)

// defaultEncryptionURL is Estuary's config encryption service, which encrypts
// any config properties marked as secret in the schema using sops.
const defaultEncryptionURL = "https://config-encryption.estuary.dev/v1/encrypt-config"

// EmitConfigUpdate encrypts a full restatement of the endpoint config against
// its JSON schema and emits a "configUpdate" connector event. The control
// plane reacts to the event by republishing the task's spec with its endpoint
// config replaced wholesale by the emitted config, so config must be a
// complete config document and not a partial patch.
func EmitConfigUpdate(ctx context.Context, msg string, config, schema json.RawMessage) error {
	encrypted, err := encryptConfig(ctx, config, schema)
	if err != nil {
		return fmt.Errorf("encrypting config: %w", err)
	}

	log.WithFields(log.Fields{
		"eventType": "configUpdate",
		"config":    encrypted,
	}).Info(msg)

	return nil
}

// encryptConfig submits the config and its schema to the config encryption
// service and returns the encrypted config document.
func encryptConfig(ctx context.Context, config, schema json.RawMessage) (json.RawMessage, error) {
	// Flow always sorts object properties lexicographically, and sops relies
	// on encountered property order when computing the HMAC portion of its
	// stanza. Round-tripping through a map sorts keys recursively, since
	// encoding/json marshals maps in sorted key order. UseNumber preserves
	// numbers as json.Number instead of converting to float64.
	var asMap map[string]any
	var dec = json.NewDecoder(bytes.NewReader(config))
	dec.UseNumber()
	if err := dec.Decode(&asMap); err != nil {
		return nil, fmt.Errorf("decoding config: %w", err)
	}

	body, err := json.Marshal(map[string]any{
		"config": asMap,
		"schema": schema,
	})
	if err != nil {
		return nil, fmt.Errorf("encoding request body: %w", err)
	}

	var url = defaultEncryptionURL
	if fromEnv := os.Getenv("ENCRYPTION_URL"); fromEnv != "" {
		url = fromEnv
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting config encryption: %w", err)
	}
	defer res.Body.Close()

	resBody, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading config encryption response: %w", err)
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return nil, fmt.Errorf("config encryption returned status %d: %s", res.StatusCode, string(resBody))
	}

	return json.RawMessage(resBody), nil
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

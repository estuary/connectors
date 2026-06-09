package common

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

func TestSetJSONProperty(t *testing.T) {
	for _, tt := range []struct {
		name    string
		doc     string
		path    []string
		value   any
		want    string
		wantErr string
	}{
		{
			name:  "absent intermediate object",
			doc:   `{"address":"db:5432"}`,
			path:  []string{"advanced", "created_at"},
			value: "2026-06-15T00:00:00Z",
			want:  `{"address":"db:5432","advanced":{"created_at":"2026-06-15T00:00:00Z"}}`,
		},
		{
			name:  "null intermediate object",
			doc:   `{"advanced":null}`,
			path:  []string{"advanced", "created_at"},
			value: "2026-06-15T00:00:00Z",
			want:  `{"advanced":{"created_at":"2026-06-15T00:00:00Z"}}`,
		},
		{
			name:  "existing intermediate object is preserved",
			doc:   `{"advanced":{"feature_flags":"foo"},"port":1234567890123456789}`,
			path:  []string{"advanced", "created_at"},
			value: "2026-06-15T00:00:00Z",
			want:  `{"advanced":{"created_at":"2026-06-15T00:00:00Z","feature_flags":"foo"},"port":1234567890123456789}`,
		},
		{
			name:  "overwrites existing value",
			doc:   `{"advanced":{"created_at":"old"}}`,
			path:  []string{"advanced", "created_at"},
			value: "new",
			want:  `{"advanced":{"created_at":"new"}}`,
		},
		{
			name:  "top-level property",
			doc:   `{}`,
			path:  []string{"created_at"},
			value: "2026-06-15T00:00:00Z",
			want:  `{"created_at":"2026-06-15T00:00:00Z"}`,
		},
		{
			name:    "non-object intermediate errors",
			doc:     `{"advanced":"nope"}`,
			path:    []string{"advanced", "created_at"},
			value:   "x",
			wantErr: `property "advanced" is not an object`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SetJSONProperty(json.RawMessage(tt.doc), tt.path, tt.value)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.JSONEq(t, tt.want, string(got))
		})
	}
}

func TestEmitConfigUpdate(t *testing.T) {
	var requestBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		requestBody, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		w.Write([]byte(`{"address":"db:5432","password_sops":"ENC[AES256_GCM,...]"}`))
	}))
	defer server.Close()
	t.Setenv("ENCRYPTION_URL", server.URL)

	hook := logtest.NewGlobal()
	defer hook.Reset()

	// Out-of-order keys and a large integer that doesn't round-trip through
	// float64, to verify key sorting and number fidelity.
	config := json.RawMessage(`{"port":1234567890123456789,"advanced":{"feature_flags":"foo"},"address":"db:5432"}`)
	schema := json.RawMessage(`{"type":"object"}`)

	require.NoError(t, EmitConfigUpdate(context.Background(), "updating config", config, schema))

	require.JSONEq(t, `{
		"config": {"address":"db:5432","advanced":{"feature_flags":"foo"},"port":1234567890123456789},
		"schema": {"type":"object"}
	}`, string(requestBody))

	// Keys of the encrypted config must be in sorted order for sops HMAC
	// stability, which JSONEq alone doesn't verify.
	var body struct {
		Config json.RawMessage `json:"config"`
	}
	require.NoError(t, json.Unmarshal(requestBody, &body))
	require.Equal(t,
		`{"address":"db:5432","advanced":{"feature_flags":"foo"},"port":1234567890123456789}`,
		string(body.Config),
	)

	require.Len(t, hook.Entries, 1)
	entry := hook.LastEntry()
	require.Equal(t, log.InfoLevel, entry.Level)
	require.Equal(t, "updating config", entry.Message)
	require.Equal(t, "configUpdate", entry.Data["eventType"])
	require.JSONEq(t,
		`{"address":"db:5432","password_sops":"ENC[AES256_GCM,...]"}`,
		string(entry.Data["config"].(json.RawMessage)),
	)
}

func TestEmitConfigUpdateEncryptionFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "bad schema", http.StatusBadRequest)
	}))
	defer server.Close()
	t.Setenv("ENCRYPTION_URL", server.URL)

	hook := logtest.NewGlobal()
	defer hook.Reset()

	err := EmitConfigUpdate(context.Background(), "updating config", json.RawMessage(`{}`), json.RawMessage(`{}`))
	require.ErrorContains(t, err, "status 400")
	require.Empty(t, hook.Entries)
}

package common

import (
	"encoding/json"
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
	hook := logtest.NewGlobal()
	defer hook.Reset()

	config := json.RawMessage(`{"address":"db:5432","sops":{"mac":"ENC[AES256_GCM,data:yy]"}}`)
	EmitConfigUpdate("updating config", config)

	require.Len(t, hook.Entries, 1)
	entry := hook.LastEntry()
	require.Equal(t, log.InfoLevel, entry.Level)
	require.Equal(t, "updating config", entry.Message)
	require.Equal(t, "configUpdate", entry.Data["eventType"])
	require.Equal(t, config, entry.Data["config"])
}

func TestSetSealedConfigProperty(t *testing.T) {
	const sealed = `{"port":5432,"address":"db:5432","credentials":{"user":"flow","password_sops":"ENC[AES256_GCM,data:xx]"},"sops":{"encrypted_suffix":"_sops","mac":"ENC[AES256_GCM,data:yy]"}}`

	for _, tt := range []struct {
		name    string
		doc     string
		path    []string
		value   any
		want    string
		wantErr string
	}{
		{
			// The encrypted values are carried across as they were: the overlay is
			// what the runtime merges over them once they are decrypted.
			name:  "sops document carries the value in an overlay",
			doc:   sealed,
			path:  []string{"advanced", "feature_flags"},
			value: "gated_flag",
			want:  `{"address":"db:5432","credentials":{"password_sops":"ENC[AES256_GCM,data:xx]","user":"flow"},"port":5432,"sops":{"encrypted_suffix":"_sops","mac":"ENC[AES256_GCM,data:yy]","overlay":{"advanced":{"feature_flags":"gated_flag"}}}}`,
		},
		{
			name:  "existing overlay properties are kept",
			doc:   `{"port":5432,"sops":{"overlay":{"other":true},"mac":"ENC[AES256_GCM,data:yy]"}}`,
			path:  []string{"advanced", "feature_flags"},
			value: "gated_flag",
			want:  `{"port":5432,"sops":{"mac":"ENC[AES256_GCM,data:yy]","overlay":{"advanced":{"feature_flags":"gated_flag"},"other":true}}}`,
		},
		{
			name:  "unencrypted config is set directly, without a sops stanza",
			doc:   `{"port":5432,"address":"db:5432"}`,
			path:  []string{"advanced", "feature_flags"},
			value: "gated_flag",
			want:  `{"address":"db:5432","advanced":{"feature_flags":"gated_flag"},"port":5432}`,
		},
		{
			name: "null sops stanza is not a sops document",
			// The runtime reads a null `sops` as an unencrypted config, so a
			// restatement must not hand it an overlay to apply.
			doc:   `{"port":5432,"sops":null}`,
			path:  []string{"feature_flags"},
			value: "gated_flag",
			want:  `{"feature_flags":"gated_flag","port":5432,"sops":null}`,
		},
		{
			name:    "non-object config",
			doc:     `["not","a","config"]`,
			path:    []string{"feature_flags"},
			value:   "gated_flag",
			wantErr: "config is not a JSON object",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SetSealedConfigProperty(json.RawMessage(tt.doc), tt.path, tt.value)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, string(got))
		})
	}
}

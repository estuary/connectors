package catalog

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGetTableToleratesVersionScopedFields is a regression test for a
// production failure seen after upgrading iceberg-go from v0.5.0 to v0.6.0:
//
//	completeRecovery: store.RestoreCheckpoint: getting table: invalid or
//	missing format-version in table metadata: v3-only field 'next-row-id'
//	present in v2 metadata
//
// Some catalog services include fields from newer format versions in
// loadTable responses for older-version tables. iceberg-go v0.6.0 added
// strict version-scoped field validation which rejects such metadata, where
// v0.5.0 ignored the extra fields. GetTable drops those fields before
// parsing to retain the lenient reading behavior.
//
// The fixture is real v2 metadata produced by the local docker-compose
// Polaris stack after a Spark data commit; the offending fields are injected
// per test case.
func TestGetTableToleratesVersionScopedFields(t *testing.T) {
	fixture, err := os.ReadFile("testdata/v2-metadata.json")
	require.NoError(t, err)

	for _, tt := range []struct {
		name   string
		inject map[string]any
	}{
		{name: "clean metadata", inject: nil},
		{name: "next-row-id in v2", inject: map[string]any{"next-row-id": 0}},
		{name: "encryption-keys in v2", inject: map[string]any{
			"encryption-keys": []map[string]string{{"key-id": "k1", "encrypted-key-metadata": "abc123"}},
		}},
		{name: "both v3-only fields in v2", inject: map[string]any{
			"next-row-id":     42,
			"encryption-keys": []map[string]string{{"key-id": "k1", "encrypted-key-metadata": "abc123"}},
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var meta map[string]any
			require.NoError(t, json.Unmarshal(fixture, &meta))
			for k, v := range tt.inject {
				meta[k] = v
			}
			metaBytes, err := json.Marshal(meta)
			require.NoError(t, err)

			mux := http.NewServeMux()
			mux.HandleFunc("GET /v1/config", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{
					"defaults":  map[string]string{},
					"overrides": map[string]string{},
				})
			})
			mux.HandleFunc("GET /v1/namespaces/some_ns/tables/some_table", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{
					"metadata-location": "s3://bucket/metadata/v2.metadata.json",
					"metadata":          json.RawMessage(metaBytes),
				})
			})

			ts := httptest.NewServer(mux)
			defer ts.Close()

			scope := "my-scope"
			cat, err := New(t.Context(), ts.URL, "my-warehouse", WithClientCredential("my-creds", "my-uri", &scope))
			require.NoError(t, err)

			table, err := cat.GetTable(t.Context(), "some_ns", "some_table")
			require.NoError(t, err)
			require.EqualValues(t, 2, table.Metadata.Version())
			require.Len(t, table.Metadata.Snapshots(), 1)
			require.Equal(t, "s3://bucket/metadata/v2.metadata.json", table.MetadataLocation)
		})
	}
}

// TestDropFieldsBeyondFormatVersion covers the sanitizer's edge cases
// directly: metadata that isn't JSON at all, missing or invalid
// format-version, and fields that are legitimate for the declared version
// must all pass through unchanged.
func TestDropFieldsBeyondFormatVersion(t *testing.T) {
	for _, tt := range []struct {
		name        string
		in          string
		want        string
		wantDropped []string
		wantErr     string
	}{
		{
			name:    "not an object errors",
			in:      `"just a string"`,
			wantErr: "parsing table metadata",
		},
		{
			name: "missing format-version passes through",
			in:   `{"next-row-id": 0}`,
			want: `{"next-row-id": 0}`,
		},
		{
			name:    "non-integer format-version errors",
			in:      `{"format-version": "two", "next-row-id": 0}`,
			wantErr: "parsing format-version",
		},
		{
			name:    "nonsensical format-version errors",
			in:      `{"format-version": 0, "next-row-id": 0}`,
			wantErr: "invalid format-version 0",
		},
		{
			name: "v3 keeps its own fields",
			in:   `{"format-version": 3, "next-row-id": 7}`,
			want: `{"format-version": 3, "next-row-id": 7}`,
		},
		{
			name: "v2 keeps last-sequence-number",
			in:   `{"format-version": 2, "last-sequence-number": 5}`,
			want: `{"format-version":2,"last-sequence-number":5}`,
		},
		{
			name:        "v2 drops next-row-id",
			in:          `{"format-version": 2, "next-row-id": 0, "last-sequence-number": 5}`,
			want:        `{"format-version":2,"last-sequence-number":5}`,
			wantDropped: []string{"next-row-id"},
		},
		{
			name:        "v1 drops last-sequence-number and v3-only fields",
			in:          `{"format-version": 1, "last-sequence-number": 5, "next-row-id": 0, "encryption-keys": []}`,
			want:        `{"format-version":1}`,
			wantDropped: []string{"encryption-keys", "last-sequence-number", "next-row-id"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, dropped, _, err := dropFieldsBeyondFormatVersion(json.RawMessage(tt.in))
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.JSONEq(t, tt.want, string(got))
			require.Equal(t, tt.wantDropped, dropped)
		})
	}
}

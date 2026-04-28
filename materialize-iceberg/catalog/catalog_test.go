package catalog

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBaseURL(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		check   func(catalogURL, baseURL *url.URL)
	}{
		{
			name: "no prefix or uri",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				respBody, err := json.Marshal(map[string]any{
					"defaults":  map[string]string{},
					"overrides": map[string]string{},
				})
				require.NoError(t, err)
				w.Write(respBody)
			},
			check: func(catalogURL, baseURL *url.URL) {
				require.Equal(t, baseURL.Path, "/iceberg/v1")
				require.Equal(t, catalogURL.Host, baseURL.Host)
			},
		},
		{
			name: "with prefix",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				respBody, err := json.Marshal(map[string]any{
					"defaults": map[string]string{
						"prefix": "main",
					},
					"overrides": map[string]string{},
				})
				require.NoError(t, err)
				w.Write(respBody)
			},
			check: func(catalogURL, baseURL *url.URL) {
				require.Equal(t, baseURL.Path, "/iceberg/v1/main")
				require.Equal(t, catalogURL.Host, baseURL.Host)
			},
		},
		{
			name: "with uri",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				respBody, err := json.Marshal(map[string]any{
					"defaults": map[string]string{},
					"overrides": map[string]string{
						"uri": "http://example.org/iceberg/",
					},
				})
				require.NoError(t, err)
				w.Write(respBody)
			},
			check: func(catalogURL, baseURL *url.URL) {
				require.Equal(t, baseURL.String(), "http://example.org/iceberg/v1")
			},
		},
		{
			name: "with prefix and uri",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				respBody, err := json.Marshal(map[string]any{
					"defaults": map[string]string{
						"prefix": "main",
					},
					"overrides": map[string]string{
						"uri": "http://example.org/iceberg/",
					},
				})
				require.NoError(t, err)
				w.Write(respBody)
			},
			check: func(catalogURL, baseURL *url.URL) {
				require.Equal(t, baseURL.String(), "http://example.org/iceberg/v1/main")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("GET /iceberg/v1/config", tt.handler)

			ts := httptest.NewServer(mux)
			defer ts.Close()

			testURL, err := url.Parse(ts.URL)
			require.NoError(t, err)

			catalogURL := testURL.JoinPath("iceberg")
			scope := "my-scope"
			options := WithClientCredential("my-creds", "my-uri", &scope)
			catalog, err := New(t.Context(), catalogURL.String(), "my-warehouse", options)
			require.NoError(t, err)

			baseURL, err := url.Parse(catalog.rHttp.BaseURL())
			require.NoError(t, err)

			tt.check(catalogURL, baseURL)
		})
	}
}

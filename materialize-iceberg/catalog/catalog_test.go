package catalog

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
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
			name: "with defaults prefix",
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
			name: "with defaults uri",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				respBody, err := json.Marshal(map[string]any{
					"defaults": map[string]string{
						"uri": "http://example.org/iceberg/",
					},
					"overrides": map[string]string{},
				})
				require.NoError(t, err)
				w.Write(respBody)
			},
			check: func(catalogURL, baseURL *url.URL) {
				require.Equal(t, baseURL.String(), "http://example.org/iceberg/v1")
			},
		},
		{
			name: "with overrides uri",
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
			// This case matches how Nessie reports a branch.
			name: "with defaults prefix and overrides uri",
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

func TestTokenURL(t *testing.T) {
	configHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		respBody, err := json.Marshal(map[string]any{
			"defaults":  map[string]string{},
			"overrides": map[string]string{},
		})
		require.NoError(t, err)
		w.Write(respBody)
	}
	tokenHandler := func(hits *atomic.Int32) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"access_token":"t","token_type":"bearer"}`))
		}
	}

	t.Run("relative uri joins catalog url", func(t *testing.T) {
		var tokenHits atomic.Int32
		mux := http.NewServeMux()
		mux.HandleFunc("GET /iceberg/v1/config", configHandler)
		mux.HandleFunc("POST /iceberg/v1/oauth/tokens", tokenHandler(&tokenHits))

		ts := httptest.NewServer(mux)
		defer ts.Close()

		testURL, err := url.Parse(ts.URL)
		require.NoError(t, err)
		catalogURL := testURL.JoinPath("iceberg")

		scope := "my-scope"
		catalog, err := New(t.Context(), catalogURL.String(), "my-warehouse", WithClientCredential("id:secret", "v1/oauth/tokens", &scope))
		require.NoError(t, err)

		require.Equal(t, catalogURL.JoinPath("v1/oauth/tokens").String(), catalog.TokenURL())
		require.Equal(t, int32(1), tokenHits.Load())
	})

	t.Run("absolute uri is used as-is", func(t *testing.T) {
		var tokenHits atomic.Int32
		idpMux := http.NewServeMux()
		idpMux.HandleFunc("POST /oauth/token", tokenHandler(&tokenHits))
		idp := httptest.NewServer(idpMux)
		defer idp.Close()

		mux := http.NewServeMux()
		mux.HandleFunc("GET /iceberg/v1/config", configHandler)
		ts := httptest.NewServer(mux)
		defer ts.Close()

		testURL, err := url.Parse(ts.URL)
		require.NoError(t, err)
		catalogURL := testURL.JoinPath("iceberg")
		tokenURL := idp.URL + "/oauth/token"

		scope := "my-scope"
		catalog, err := New(t.Context(), catalogURL.String(), "my-warehouse", WithClientCredential("id:secret", tokenURL, &scope))
		require.NoError(t, err)

		require.Equal(t, tokenURL, catalog.TokenURL())
		require.Equal(t, int32(1), tokenHits.Load())
	})
}

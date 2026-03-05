package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"resty.dev/v3"
)

func TestWaitForTokenPersisted(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		check   func(err error)
	}{
		{
			name: "success",
			handler: func() http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					_, err := io.ReadAll(r.Body)
					require.NoError(t, err)

					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					respBody, err := json.Marshal(map[string]any{
						"status_code": 0,
						"message":     "Success",
						"channels": []any{
							map[string]any{
								"status_code":                0,
								"persisted_offset_token":     nil,
								"persisted_client_sequencer": 0,
								"persisted_row_sequencer":    0,
							},
						},
					})
					require.NoError(t, err)
					w.Write(respBody)
				}
			}(),
			check: func(err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "top status code",
			handler: func() http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					_, err := io.ReadAll(r.Body)
					require.NoError(t, err)

					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					respBody, err := json.Marshal(map[string]any{
						"status_code": 4,
						"message":     "the supplied table does not exist or is not authorized",
					})
					require.NoError(t, err)
					w.Write(respBody)
				}
			}(),
			check: func(err error) {
				require.ErrorContains(t, err, "request was not successful")
			},
		},
		{
			name: "retry top status code",
			handler: func() http.HandlerFunc {
				count := 0
				return func(w http.ResponseWriter, r *http.Request) {
					_, err := io.ReadAll(r.Body)
					require.NoError(t, err)

					if count == 0 {
						count++
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusOK)
						respBody, err := json.Marshal(map[string]any{
							"status_code": 10,
							"message":     "Snowflake experienced a transient exception, please retry the request",
						})
						require.NoError(t, err)
						w.Write(respBody)
					} else {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusOK)
						respBody, err := json.Marshal(map[string]any{
							"status_code": 0,
							"message":     "Success",
							"channels": []any{
								map[string]any{
									"status_code":                0,
									"persisted_offset_token":     nil,
									"persisted_client_sequencer": 0,
									"persisted_row_sequencer":    0,
								},
							},
						})
						require.NoError(t, err)
						w.Write(respBody)
					}
				}
			}(),
			check: func(err error) {
				require.NoError(t, err)
			},
		},
		{
			name: "channel error status code",
			handler: func() http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					_, err := io.ReadAll(r.Body)
					require.NoError(t, err)

					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					respBody, err := json.Marshal(map[string]any{
						"status_code": 0,
						"message":     "Success",
						"channels": []any{
							map[string]any{
								"status_code":                5,
								"persisted_offset_token":     nil,
								"persisted_client_sequencer": 0,
								"persisted_row_sequencer":    0,
							},
						},
					})
					require.NoError(t, err)
					w.Write(respBody)
				}
			}(),
			check: func(err error) {
				require.ErrorContains(t, err, "failed to get channel status")
			},
		},
		{
			name: "retry temporary channel status code",
			handler: func() http.HandlerFunc {
				count := 0
				return func(w http.ResponseWriter, r *http.Request) {
					_, err := io.ReadAll(r.Body)
					require.NoError(t, err)

					if count == 0 {
						count++
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusOK)
						respBody, err := json.Marshal(map[string]any{
							"status_code": 0,
							"message":     "Success",
							"channels": []any{
								map[string]any{
									"status_code":                10,
									"persisted_offset_token":     nil,
									"persisted_client_sequencer": 0,
									"persisted_row_sequencer":    0,
								},
							},
						})
						require.NoError(t, err)
						w.Write(respBody)
					} else {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusOK)
						respBody, err := json.Marshal(map[string]any{
							"status_code": 0,
							"message":     "Success",
							"channels": []any{
								map[string]any{
									"status_code":                0,
									"persisted_offset_token":     nil,
									"persisted_client_sequencer": 0,
									"persisted_row_sequencer":    0,
								},
							},
						})
						require.NoError(t, err)
						w.Write(respBody)
					}
				}
			}(),
			check: func(err error) {
				require.NoError(t, err)
			},
		},
	}

	pkey, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("POST /v1/streaming/channels/status", tt.handler)

			ts := httptest.NewServer(mux)
			defer ts.Close()

			ctx := context.Background()

			role := "TEST_ROLE"
			streamClient := &streamClient{
				r:        resty.New().SetBaseURL(ts.URL + "/v1/streaming").SetDisableWarn(true),
				key:      pkey,
				user:     "TEST_USER",
				database: "TEST_DB",
				account:  "TEST_ACCOUNT",
				role:     &role,
			}
			err := streamClient.waitForTokenPersisted(ctx, "", 0, "TEST_SCHEMA", "TEST_TABLE", "TEST_CHANNEL")
			tt.check(err)
		})
	}
}

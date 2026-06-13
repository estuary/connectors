package hubspot

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSearchRequest(t *testing.T) {
	tests := []struct {
		name          string
		searchRequest *SearchRequest
		expected      json.RawMessage
	}{
		{
			name: "simple",
			searchRequest: &SearchRequest{
				FilterGroups: NewFilterGroupsEquals("domain", []string{
					"foo.example.com",
					"bar.example.com",
				}),
				Properties: []string{"property1", "property2"},
			},
			expected: json.RawMessage(`
				{
				  "filterGroups": [
					{
					  "filters": [
						{
						  "propertyName": "domain",
						  "operator": "EQ",
						  "value": "foo.example.com"
						}
					  ]
					},
					{
					  "filters": [
						{
						  "propertyName": "domain",
						  "operator": "EQ",
						  "value": "bar.example.com"
						}
					  ]
					}
				  ],
				  "properties": ["property1", "property2"]
				}
			`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := json.Marshal(tt.searchRequest)
			require.NoError(t, err)

			expected, err := json.Marshal(tt.expected)
			require.NoError(t, err)

			require.Equal(t, string(expected), string(actual))
		})

	}
}

func TestParseAPIError(t *testing.T) {
	tests := []struct {
		name           string
		resp           *http.Response
		expected       error
		expectedString string
	}{
		{
			name: "no parse response",
			resp: func() *http.Response {
				w := httptest.NewRecorder()
				w.WriteHeader(http.StatusInternalServerError)
				return w.Result()
			}(),
			expected: &TemporaryError{
				err: &APIError{
					StatusCode: 500,
				},
			},
			expectedString: "500 Internal Server Error",
		},
		{
			name: "bad request",
			resp: func() *http.Response {
				w := httptest.NewRecorder()
				w.WriteHeader(http.StatusBadRequest)
				return w.Result()
			}(),
			expected: &APIError{
				StatusCode: 400,
			},
			expectedString: "400 Bad Request",
		},
		{
			name: "429 rate limit",
			resp: func() *http.Response {
				w := httptest.NewRecorder()
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				w.Body.WriteString(`
					{
					  "status": "error",
					  "message": "You have reached your ten_secondly_rolling limit.",
					  "errorType": "RATE_LIMIT",
					  "correlationId": "64f5906e-d941-43a8-88fe-367aa32bf1a7",
					  "policyName": "TEN_SECONDLY_ROLLING"
					}
				`)
				return w.Result()
			}(),
			expected: &TemporaryError{
				err: &APIError{
					StatusCode:    429,
					Status:        "error",
					Message:       "You have reached your ten_secondly_rolling limit.",
					CorrelationID: "64f5906e-d941-43a8-88fe-367aa32bf1a7",
				},
				extraDelay: 10 * time.Second,
			},
			expectedString: "429 Too Many Requests: You have reached your ten_secondly_rolling limit.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := parseAPIError(tt.resp)
			require.Equal(t, tt.expected, actual)
			require.Equal(t, tt.expectedString, actual.Error())
		})
	}
}

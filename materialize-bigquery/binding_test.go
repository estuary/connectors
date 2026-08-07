package connector

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/require"
)

func TestBindingDocumentLoad(t *testing.T) {
	for _, tt := range []struct {
		name        string
		values      []bigquery.Value
		wantBinding int
		wantDoc     string
		wantErrIs   error
		wantErr     string
	}{
		{
			name:        "valid",
			values:      []bigquery.Value{int64(3), `{"id":1}`},
			wantBinding: 3,
			wantDoc:     `{"id":1}`,
		},
		{
			name:    "wrong value count",
			values:  []bigquery.Value{int64(3)},
			wantErr: "invalid value count: 1",
		},
		{
			name:    "binding wrong type",
			values:  []bigquery.Value{"3", `{"id":1}`},
			wantErr: "value[0] wrong type string expecting int",
		},
		{
			name:      "document is NULL",
			values:    []bigquery.Value{int64(3), nil},
			wantErrIs: errNullFlowDocument,
		},
		{
			// The reported type must be the document's, not the binding index's.
			name:    "document wrong type",
			values:  []bigquery.Value{int64(3), 3.5},
			wantErr: "value[1] wrong type float64 expecting string",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var bd bindingDocument
			var err = bd.Load(tt.values, nil)

			if tt.wantErrIs != nil {
				require.ErrorIs(t, err, tt.wantErrIs)
				return
			} else if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.NotErrorIs(t, err, errNullFlowDocument)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantBinding, bd.Binding)
			require.Equal(t, tt.wantDoc, string(bd.Document))
		})
	}
}

// The wording is free to change, but a NULL document is expensive to diagnose
// without these two, so they are worth pinning.
func TestNullFlowDocumentNamesTheRemedy(t *testing.T) {
	require.Contains(t, errNullFlowDocument.Error(), "Exclude Flow Document")
	require.Contains(t, errNullFlowDocument.Error(), "go.estuary.dev/matff")
}

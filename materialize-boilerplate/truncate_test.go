package boilerplate

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestPublishedAtMismatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		str     *pf.Inference_String
		wantErr string
	}{
		{
			name: "date-time with uuid encoding",
			str:  &pf.Inference_String{Format: "date-time", ContentEncoding: "uuid"},
		},
		{
			name:    "not a string",
			wantErr: `field "flow_published_at" projects /_meta/uuid without a string type, where a date-time with uuid encoding is required`,
		},
		{
			name:    "unexpected format",
			str:     &pf.Inference_String{Format: "uuid"},
			wantErr: `field "flow_published_at" projects /_meta/uuid with the unexpected format "uuid" and content encoding "", where a date-time with uuid encoding is required`,
		},
		{
			name:    "unexpected content encoding",
			str:     &pf.Inference_String{Format: "date-time"},
			wantErr: `field "flow_published_at" projects /_meta/uuid with the unexpected format "date-time" and content encoding "", where a date-time with uuid encoding is required`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var p = pf.Projection{
				Field:     "flow_published_at",
				Ptr:       "/_meta/uuid",
				Inference: pf.Inference{String_: tc.str},
			}
			err := PublishedAtMismatch(&p)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

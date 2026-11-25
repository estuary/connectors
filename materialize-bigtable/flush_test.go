package connector

import (
	"testing"
	"time"

	"github.com/estuary/connectors/go/common"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestFlushFeatureFlags(t *testing.T) {
	var boundary = time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		flags string
		want  map[int]time.Time
	}{
		{flags: "", want: map[int]time.Time{0: boundary}},
		{flags: "no_truncate_after_backfill", want: map[int]time.Time{}},
		{flags: "retain_existing_data_on_backfill", want: map[int]time.Time{}},
	} {
		t.Run(tc.flags, func(t *testing.T) {
			var flags = common.ResolveFlags(tc.flags, featureFlagDefaults, common.CreatedAt{})
			var tr = &transactor{
				bindings: []binding{
					{path: []string{"standard"}, publishedAtField: "flow_published_at"},
				},
				truncateAfterBackfill: truncateAfterBackfill(flags),
			}
			require.NoError(t, tr.Flush(t.Context(), map[int]time.Time{0: boundary}))
			require.Equal(t, tc.want, tr.truncations)
		})
	}

	t.Run("excludes flow_published_at", func(t *testing.T) {
		var flags = common.ResolveFlags("", featureFlagDefaults, common.CreatedAt{})
		var tr = &transactor{
			bindings: []binding{{
				tableName:      "no_published_at",
				path:           []string{"no_published_at"},
				publishedAtErr: boilerplate.ErrPublishedAtExcluded,
			}},
			truncateAfterBackfill: truncateAfterBackfill(flags),
		}
		require.NoError(t, tr.Flush(t.Context(), map[int]time.Time{0: boundary}))
		require.Equal(t, map[int]time.Time{}, tr.truncations)
	})
}

func TestPublishedAtField(t *testing.T) {
	var projection = func(field, ptr string, str *pf.Inference_String) boilerplate.MappedProjection[mappedType] {
		return boilerplate.MappedProjection[mappedType]{
			Projection: boilerplate.Projection{
				Projection: pf.Projection{
					Field:     field,
					Ptr:       ptr,
					Inference: pf.Inference{String_: str},
				},
			},
		}
	}
	var dateTime = &pf.Inference_String{Format: "date-time", ContentEncoding: "uuid"}

	for _, tc := range []struct {
		name    string
		values  []boilerplate.MappedProjection[mappedType]
		field   string
		wantErr string
	}{
		{
			name:   "qualifying field",
			values: []boilerplate.MappedProjection[mappedType]{projection("other", "/other", nil), projection("flow_published_at", "/_meta/uuid", dateTime)},
			field:  "flow_published_at",
		},
		{
			name:    "field excluded",
			values:  []boilerplate.MappedProjection[mappedType]{projection("other", "/other", nil)},
			wantErr: boilerplate.ErrPublishedAtExcluded.Error(),
		},
		{
			name:    "not a string",
			values:  []boilerplate.MappedProjection[mappedType]{projection("flow_published_at", "/_meta/uuid", nil)},
			wantErr: `field "flow_published_at" projects /_meta/uuid without a string type, where a date-time with uuid encoding is required`,
		},
		{
			name:   "later qualifying field wins over earlier mismatch",
			values: []boilerplate.MappedProjection[mappedType]{projection("custom", "/_meta/uuid", nil), projection("flow_published_at", "/_meta/uuid", dateTime)},
			field:  "flow_published_at",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var b = boilerplate.MappedBinding[config, resource, mappedType]{Values: tc.values}
			field, err := publishedAtField(&b)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				require.Empty(t, field)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.field, field)
		})
	}
}

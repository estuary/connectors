package connector

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
	} {
		t.Run(tc.flags, func(t *testing.T) {
			var flags = common.ResolveFlags(tc.flags, featureFlagDefaults, common.CreatedAt{})
			var tr = &transactor{
				bindings: []binding{
					{path: []string{"standard"}, publishedAtField: "flow_published_at"},
					{path: []string{"delta"}, deltaUpdates: true},
				},
				truncateAfterBackfill: flags["truncate_after_backfill"],
			}
			require.NoError(t, tr.Flush(t.Context(), map[int]time.Time{0: boundary, 1: boundary}))
			require.Equal(t, tc.want, tr.truncations)
		})
	}

	t.Run("excludes flow_published_at", func(t *testing.T) {
		var flags = common.ResolveFlags("", featureFlagDefaults, common.CreatedAt{})
		var tr = &transactor{
			bindings: []binding{{
				tableName:      "no_published_at",
				path:           []string{"no_published_at"},
				publishedAtErr: errors.New("the binding excludes the flow_published_at field, which must be included to enable deletion after a backfill"),
			}},
			truncateAfterBackfill: flags["truncate_after_backfill"],
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
			wantErr: "the binding excludes the flow_published_at field, which must be included to enable deletion after a backfill",
		},
		{
			name:    "not a string",
			values:  []boilerplate.MappedProjection[mappedType]{projection("flow_published_at", "/_meta/uuid", nil)},
			wantErr: "field flow_published_at projects /_meta/uuid without a string type, where a date-time with uuid encoding is required",
		},
		{
			name:    "unexpected format",
			values:  []boilerplate.MappedProjection[mappedType]{projection("flow_published_at", "/_meta/uuid", &pf.Inference_String{Format: "uuid"})},
			wantErr: `field flow_published_at projects /_meta/uuid with the unexpected format "uuid" and content encoding "", where a date-time with uuid encoding is required`,
		},
		{
			name:   "later qualifying field wins over earlier mismatch",
			values: []boilerplate.MappedProjection[mappedType]{projection("custom", "/_meta/uuid", nil), projection("flow_published_at", "/_meta/uuid", dateTime)},
			field:  "flow_published_at",
		},
		{
			name:    "first mismatch is reported",
			values:  []boilerplate.MappedProjection[mappedType]{projection("custom", "/_meta/uuid", nil), projection("flow_published_at", "/_meta/uuid", &pf.Inference_String{Format: "uuid"})},
			wantErr: "field custom projects /_meta/uuid without a string type, where a date-time with uuid encoding is required",
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

func TestIsStale(t *testing.T) {
	var boundary = time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	var b = binding{tableName: "a_table", publishedAtField: "flow_published_at"}

	for _, tc := range []struct {
		name string
		item map[string]types.AttributeValue
		want bool
	}{
		{
			name: "before boundary",
			item: map[string]types.AttributeValue{"flow_published_at": &types.AttributeValueMemberS{Value: "2026-09-28T00:00:00Z"}},
			want: true,
		},
		{
			name: "after boundary",
			item: map[string]types.AttributeValue{"flow_published_at": &types.AttributeValueMemberS{Value: "2026-09-30T00:00:00Z"}},
			want: false,
		},
		{
			name: "nanosecond precision",
			item: map[string]types.AttributeValue{"flow_published_at": &types.AttributeValueMemberS{Value: "2026-09-28T23:59:59.999999999Z"}},
			want: true,
		},
		{
			name: "missing field",
			item: map[string]types.AttributeValue{"id": &types.AttributeValueMemberN{Value: "1"}},
			want: false,
		},
		{
			name: "wrong type",
			item: map[string]types.AttributeValue{"flow_published_at": &types.AttributeValueMemberN{Value: "123"}},
			want: false,
		},
		{
			name: "unparseable",
			item: map[string]types.AttributeValue{"flow_published_at": &types.AttributeValueMemberS{Value: "not a date"}},
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, b.isStale(tc.item, boundary))
		})
	}
}

func TestFilterBoundary(t *testing.T) {
	for _, tc := range []struct {
		name     string
		boundary time.Time
		want     string
	}{
		{
			name:     "exact whole second",
			boundary: time.Date(2026, 9, 29, 0, 0, 5, 0, time.UTC),
			want:     "2026-09-29T00:00:05Z",
		},
		{
			name:     "rounds up a fractional second",
			boundary: time.Date(2026, 9, 29, 0, 0, 5, 500_000_000, time.UTC),
			want:     "2026-09-29T00:00:06Z",
		},
		{
			name:     "rounds up a single nanosecond",
			boundary: time.Date(2026, 9, 29, 0, 0, 5, 1, time.UTC),
			want:     "2026-09-29T00:00:06Z",
		},
		{
			name:     "converts to UTC first",
			boundary: time.Date(2026, 9, 29, 0, 0, 5, 0, time.FixedZone("x", 3600)),
			want:     "2026-09-28T23:00:05Z",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, filterBoundary(tc.boundary))
		})
	}
}

// TestFilterBoundaryNeverExcludesStale checks the property filterBoundary
// exists for: any timestamp string a genuinely stale item could carry,
// however many fractional digits it has, must sort lexicographically
// before filterBoundary(boundary).
func TestFilterBoundaryNeverExcludesStale(t *testing.T) {
	var boundary = time.Date(2026, 9, 29, 0, 0, 5, 500_000_000, time.UTC)
	var want = filterBoundary(boundary)

	for _, before := range []string{
		"2026-09-29T00:00:05Z",           // no fraction, same second, earlier instant
		"2026-09-29T00:00:05.499999999Z", // just under the boundary, full precision
		"2026-09-29T00:00:05.4Z",         // just under the boundary, coarse precision
		"2026-09-28T23:59:59.999999999Z", // previous second entirely
	} {
		t.Run(before, func(t *testing.T) {
			require.True(t, before < want, "%q should sort before %q", before, want)
		})
	}
}

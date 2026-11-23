package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestDeleteTimeRange(t *testing.T) {
	tx := testTransactor(t, func(*http.Request) (*http.Response, error) {
		t.Fatal("encoding a delete should not send a request")
		return nil, nil
	})
	for _, tc := range []struct {
		name  string
		stamp any
		start string
		stop  string
		err   string
	}{
		{"instant", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", ""},
		{"nanoseconds and offset", "2026-01-01T01:00:00.123456789+01:00", "2026-01-01T00:00:00.123456789Z", "2026-01-01T00:00:00.123456789Z", ""},
		{"next second", "2026-01-01T00:00:00.999999999Z", "2026-01-01T00:00:00.999999999Z", "2026-01-01T00:00:00.999999999Z", ""},
		{"minimum time", "1677-09-21T00:12:43.145224194Z", "1677-09-21T00:12:43.145224194Z", "1677-09-21T00:12:43.145224194Z", ""},
		{"maximum time", "2262-04-11T23:47:16.854775806Z", "2262-04-11T23:47:16.854775806Z", "2262-04-11T23:47:16.854775806Z", ""},
		{name: "missing", err: "must contain a date-time string"},
		{name: "invalid", stamp: "not a timestamp", err: "parsing timestamp"},
		{name: "out of range", stamp: "3000-01-01T00:00:00Z", err: "outside InfluxDB's nanosecond time range"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request, err := tx.bindings[0].deleteRequest(tuple.Tuple{"lab 1", tc.stamp}, tuple.Tuple{nil, nil, nil, nil, nil})
			if tc.err != "" {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.start, request.Start)
			require.Equal(t, tc.stop, request.Stop)
			require.Equal(t, `_measurement="readings" AND "sensor"="lab 1"`, request.Predicate)
		})
	}

	// A publication time outside the key cannot narrow a delete to one point.
	for _, stamp := range []any{nil, "2026-01-01T00:00:00Z"} {
		request, err := tx.bindings[1].deleteRequest(tuple.Tuple{int64(3)}, tuple.Tuple{nil, stamp, nil})
		require.NoError(t, err)
		require.Equal(t, deleteRequest{
			Start: "1677-09-21T00:12:43.145224194Z", Stop: "2262-04-11T23:47:16.854775806Z",
			Predicate: `_measurement="orders" AND "id"="3"`,
		}, request)
	}

	// The point was written without the empty tag, so no predicate matches only its points.
	_, err := tx.bindings[0].deleteRequest(tuple.Tuple{"", "2026-01-01T00:00:00Z"}, tuple.Tuple{nil, nil, nil, nil, nil})
	require.ErrorIs(t, err, errEmptyKeyTag)
	require.ErrorContains(t, err, `tag "sensor" from the collection key is empty`)
}

func TestDeletePredicate(t *testing.T) {
	binding := measurementBinding{
		resource: resource{
			Measurement: `lab, east=1\west"`, TimestampField: defaultTimestampField,
			TagFields: []string{`tag, key=\"`, "id", "active", "ratio", "missing"},
		},
		fields: pf.FieldSelection{
			Keys:   []string{"id"},
			Values: []string{"active", "missing", "ratio", `tag, key=\"`},
		},
		keyTags: map[string]bool{`tag, key=\"`: true, "id": true},
	}
	// Non-key tags may differ from the values of the document's earlier points.
	request, err := binding.deleteRequest(tuple.Tuple{int64(9007199254740993)}, tuple.Tuple{false, nil, 1.25, `room, 1=\west"`})
	require.NoError(t, err)
	require.Equal(t, `_measurement="lab, east=1\\west\"" AND "tag, key=\\\""="room, 1=\\west\"" AND "id"="9007199254740993"`, request.Predicate)

	// Without tags or a key timestamp, the delete would match the whole measurement.
	binding.resource.TagFields = nil
	_, err = binding.deleteRequest(tuple.Tuple{int64(3)}, tuple.Tuple{nil, nil, nil, nil})
	require.ErrorContains(t, err, "would delete every point")

	binding.resource.TagFields = []string{"active"}
	binding.keyTags = map[string]bool{"active": true}
	_, err = binding.deleteRequest(tuple.Tuple{int64(3)}, tuple.Tuple{[]byte(`{"bad":"tag"}`), nil, nil, nil})
	require.ErrorContains(t, err, "expected a scalar tag")
	_, err = binding.deleteRequest(tuple.Tuple{int64(3)}, tuple.Tuple{"line\nbreak", nil, nil, nil})
	require.ErrorContains(t, err, "do not support newlines")
	_, err = binding.deleteRequest(tuple.Tuple{int64(3)}, nil)
	require.ErrorContains(t, err, "do not match the field selection")
}

func TestDeleteTimestampKeyAlias(t *testing.T) {
	open := testOpen()
	binding := open.Materialization.Bindings[0]
	timestamp := *binding.Collection.GetProjection("ts")
	timestamp.Field = "event_time"
	timestamp.IsPrimaryKey = false
	addProjections(&binding.Collection, timestamp)
	binding.ResourceConfigJson = json.RawMessage(`{"measurement":"readings","timestampField":"event_time"}`)
	binding.FieldSelection.Values = []string{"event_time", "temp"}
	tx, _, _, err := Driver().NewTransactor(context.Background(), *open, nil)
	require.NoError(t, err)
	defer tx.Destroy()
	request, err := tx.(*transactor).bindings[0].deleteRequest(
		tuple.Tuple{"s1", "2026-01-01T00:00:00Z"}, tuple.Tuple{"2026-01-01T00:00:00Z", nil},
	)
	require.NoError(t, err)
	require.Equal(t, "2026-01-01T00:00:00Z", request.Start)
	require.Equal(t, "2026-01-01T00:00:00Z", request.Stop)
	require.Equal(t, `_measurement="readings" AND "sensor"="s1"`, request.Predicate)
}

func TestDeleteIgnoresNonKeyTags(t *testing.T) {
	open := testOpen()
	open.Materialization.Bindings[1].ResourceConfigJson = json.RawMessage(`{"measurement":"orders","tagFields":["id","status"]}`)
	tx, _, _, err := Driver().NewTransactor(context.Background(), *open, nil)
	require.NoError(t, err)
	defer tx.Destroy()

	// The order's earlier points may carry another status, and must be deleted too.
	request, err := tx.(*transactor).bindings[1].deleteRequest(tuple.Tuple{int64(3)}, tuple.Tuple{nil, nil, "shipped"})
	require.NoError(t, err)
	require.Equal(t, `_measurement="orders" AND "id"="3"`, request.Predicate)
}

func TestDeleteRequiresKeyTags(t *testing.T) {
	open := testOpen()
	open.Materialization.Bindings[1].ResourceConfigJson = json.RawMessage(`{"measurement":"orders","tagFields":["status"]}`)
	_, _, _, err := Driver().NewTransactor(context.Background(), *open, nil)
	require.ErrorContains(t, err, `add "id" to 'tagFields'`)

	open = testOpen()
	open.Materialization.Bindings[0].ResourceConfigJson = json.RawMessage(`{"measurement":"readings","tagFields":["sensor"]}`)
	open.Materialization.Bindings[0].FieldSelection.Values = append(open.Materialization.Bindings[0].FieldSelection.Values, "flow_published_at")
	_, _, _, err = Driver().NewTransactor(context.Background(), *open, nil)
	require.ErrorContains(t, err, `add "ts" to 'tagFields'`)
}

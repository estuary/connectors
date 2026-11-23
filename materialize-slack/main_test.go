package main

import (
	"context"
	"encoding/json"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

// requestStream replays a fixed sequence of requests.
type requestStream struct {
	requests []*pm.Request
}

func (s *requestStream) Send(*pm.Response) error { return nil }

func (s *requestStream) RecvMsg(r *pm.Request) error {
	*r = *s.requests[0]
	s.requests = s.requests[1:]
	return nil
}

func TestStoreBlocksValue(t *testing.T) {
	// Old enough that Store never posts the message.
	const ts = "2020-01-01T00:00:00Z"

	for _, tc := range []struct {
		name    string
		blocks  tuple.TupleElement
		wantErr string
	}{
		{name: "absent", blocks: nil},
		{name: "array", blocks: []byte(`[{"type":"divider"}]`)},
		{name: "string", blocks: "some text", wantErr: "invalid blocks value of type string"},
		{name: "integer", blocks: int64(7), wantErr: "invalid blocks value of type int64"},
		{name: "number", blocks: 1.5, wantErr: "invalid blocks value of type float64"},
		{name: "boolean", blocks: true, wantErr: "invalid blocks value of type bool"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stream = &requestStream{requests: []*pm.Request{
				{Store: &pm.Request_Store{
					KeyPacked:    tuple.Tuple{int64(1)}.Pack(),
					ValuesPacked: tuple.Tuple{ts, "hello", tc.blocks}.Pack(),
					DocJson:      json.RawMessage(`{"id":1}`),
				}},
				{StartCommit: &pm.Request_StartCommit{}},
			}}
			var it = m.NewStoreIterator(context.Background(), stream, &pm.Request{Flush: &pm.Request_Flush{}})

			var tr = &transactor{bindings: []*binding{{
				resource: resource{Channel: "id:C123"},
				spec: &pf.MaterializationSpec_Binding{
					FieldSelection: pf.FieldSelection{
						Keys:   []string{"id"},
						Values: []string{"ts", "text", "blocks"},
					},
				},
			}}}

			var _, err = tr.Store(it)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}
			require.NoError(t, it.Err())
		})
	}
}

func TestValidateBlocksConstraint(t *testing.T) {
	for _, tc := range []struct {
		name  string
		types []string
		want  pm.Response_Validated_Constraint_Type
	}{
		{name: "array", types: []string{"array"}, want: pm.Response_Validated_Constraint_FIELD_OPTIONAL},
		{name: "nullable array", types: []string{"array", "null"}, want: pm.Response_Validated_Constraint_FIELD_OPTIONAL},
		{name: "array or string", types: []string{"array", "string"}, want: pm.Response_Validated_Constraint_FIELD_OPTIONAL},
		{name: "string", types: []string{"string"}, want: pm.Response_Validated_Constraint_FIELD_FORBIDDEN},
		{name: "integer", types: []string{"integer"}, want: pm.Response_Validated_Constraint_FIELD_FORBIDDEN},
		{name: "object", types: []string{"object"}, want: pm.Response_Validated_Constraint_FIELD_FORBIDDEN},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var projection = func(field string, types ...string) pf.Projection {
				return pf.Projection{Field: field, Ptr: "/" + field, Inference: pf.Inference{Types: types}}
			}

			var validated, err = driver{}.Validate(context.Background(), &pm.Request_Validate{
				ConfigJson: json.RawMessage(`{"credentials":{"client_id":"id","client_secret":"secret","access_token":"token"}}`),
				Bindings: []*pm.Request_Validate_Binding{{
					ResourceConfigJson: json.RawMessage(`{"channel":"id:C123","sender_config":{"display_name":"Bot"}}`),
					Collection: pf.CollectionSpec{Projections: []pf.Projection{
						projection("ts", "string"),
						projection("text", "string"),
						projection("blocks", tc.types...),
					}},
				}},
			})
			require.NoError(t, err)

			var got = make(map[string]pm.Response_Validated_Constraint_Type)
			for _, c := range validated.Bindings[0].ProjectionConstraints {
				got[c.Field] = c.Constraint.Type
			}
			require.Equal(t, map[string]pm.Response_Validated_Constraint_Type{
				"ts":     pm.Response_Validated_Constraint_FIELD_REQUIRED,
				"text":   pm.Response_Validated_Constraint_FIELD_REQUIRED,
				"blocks": tc.want,
			}, got)
		})
	}
}

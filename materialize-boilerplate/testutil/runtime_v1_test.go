package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStripRuntimeNextOutput(t *testing.T) {
	singleShard := `Task: acmeCo/tests/materialize-x

Resource: public.simple_standard
["applied.actionDescription", "\nCREATE TABLE IF NOT EXISTS ..."]
["connectorState",{}]
["connectorState",{"updated":{}}]

{"Name":"id","Nullable":false,"Type":"bigint"}

Table Data:
{"id":1}
`
	sharded := `Task: acmeCo/tests/materialize-x

Resource: public.simple_standard

{"Name":"id","Nullable":false,"Type":"bigint"}

Table Data:
{"id":1}
`
	require.Equal(t, sharded, stripRuntimeNextOutput(singleShard))
	require.Equal(t, sharded, stripRuntimeNextOutput(sharded), "a snapshot without output lines is unchanged")
}

func TestSignExponents(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{`{"a":1.7014118346046921e38,"b":1e21}`, `{"a":1.7014118346046921e+38,"b":1e+21}`},
		{`[1e21,2E5]`, `[1e+21,2E+5]`},
		{`{"a":1e+21,"b":1e-7,"c":123.456}`, `{"a":1e+21,"b":1e-7,"c":123.456}`},
		{`"{\"a\":5.789604461865809e76}"`, `"{\"a\":5.789604461865809e+76}"`},
		{`{"uuid":"550e8400-e29b-41d4-a716-446655440000","s":"1e5","hex":"\\x6e2066"}`, `{"uuid":"550e8400-e29b-41d4-a716-446655440000","s":"1e5","hex":"\\x6e2066"}`},
		{"x: 1e21\n2e3", "x: 1e+21\n2e+3"},
	} {
		require.Equal(t, tc.want, signExponents(tc.in), tc.in)
	}
}

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

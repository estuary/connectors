package connector

import (
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

func TestQuoting(t *testing.T) {
	require.Equal(t, `"a""b'c\d"`, quoteIdentifier(`a"b'c\d`))
	require.Equal(t, `'a"b''c\\d'`, quoteLiteral(`a"b'c\d`))
	require.Equal(t, `'it''s\\_100\\%\\\\done'`, showLikeLiteral(`it's_100%\done`))

	for _, tc := range []struct{ name, want string }{
		{`DB.MY_SCHEMA.FLOW_PIPE_0`, `DB.MY_SCHEMA.FLOW_PIPE_0`},
		{`DB.MY$SCHEMA.FLOW_PIPE_0`, `DB.MY$SCHEMA.FLOW_PIPE_0`},
		{`"MY-DB".S.FLOW_PIPE_0`, `"MY-DB".S.FLOW_PIPE_0`},
		{`"A""B".S.FLOW_PIPE_0`, `"A""B".S.FLOW_PIPE_0`},
		{`"A"B".S.FLOW_PIPE_0`, `"""A""B""".S.FLOW_PIPE_0`},
		{`DB.MY-SCHEMA.FLOW_PIPE_0`, `DB."MY-SCHEMA".FLOW_PIPE_0`},
		{`DB.A"B'C.FLOW_PIPE_0`, `DB."A""B'C".FLOW_PIPE_0`},
		{`DB.1S.FLOW_PIPE_0`, `DB."1S".FLOW_PIPE_0`},
	} {
		require.Equal(t, tc.want, pipeIdentifier(tc.name), tc.name)
	}
}

func TestPipeTemplatesQuoteNames(t *testing.T) {
	var target = sql.Table{
		TableShape: sql.TableShape{Path: []string{"a-schema", `it's\here`}},
		Identifier: testDialect.Identifier("a-schema", `it's\here`),
	}

	createPipe, err := renderTablePipeTemplate(target, `DB.A"B'C.FLOW_PIPE_0`, testTemplates.createPipe)
	require.NoError(t, err)
	require.Contains(t, createPipe, `CREATE PIPE DB."A""B'C".FLOW_PIPE_0`+"\n")
	require.Contains(t, createPipe, `COMMENT = 'Pipe for table [a-schema it''s\\here]'`+"\n")

	copyHistory, err := renderCopyHistoryTemplate(target.Identifier, []string{"dir/a", "dir/b"}, testTemplates.copyHistory)
	require.NoError(t, err)
	require.Contains(t, copyHistory, `TABLE_NAME=>'"a-schema"."it''s\\here"',`+"\n")
	require.Contains(t, copyHistory, `FILE_NAME IN ('dir/a','dir/b')`)
}

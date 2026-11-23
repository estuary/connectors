package connector

import (
	"testing"

	mssqldb "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/require"
)

func TestBulkCopyIn(t *testing.T) {
	var table = "#flow_temp_store_0"

	var cols = []string{"id", "a b", `a"b`, "a[b", "_flow_delete"}
	stmt, err := bulkCopyIn(table, cols)
	require.NoError(t, err)
	require.Equal(t, mssqldb.CopyIn(table, mssqldb.BulkOptions{}, cols...), stmt)

	for _, name := range []string{"a]b", "]", "a]"} {
		_, err := bulkCopyIn(table, []string{"id", name})
		require.ErrorContains(t, err, name)
	}
}

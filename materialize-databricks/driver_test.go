//go:build !nodb

package main

import (
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
)

func TestValidate(t *testing.T) {
	sql.RunValidateTestCases(t, databricksDialect)
}

//go:build !nodb

package main

import (
	/*"context"
	stdsql "database/sql"
	"fmt"
	"os"
	"strings"*/
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
)

/*func mustGetCfg(t *testing.T) config {
	if os.Getenv("TESTDB") != "yes" {
		t.Skipf("skipping %q: ${TESTDB} != \"yes\"", t.Name())
		return config{}
	}

	out := config{}
	out.Credentials.AuthType = "PAT"

	for _, prop := range []struct {
		key  string
		dest *string
	}{
		{"DATABRICKS_HOST", &out.Address},
		{"DATABRICKS_HTTP_PATH", &out.HTTPPath},
		{"DATABRICKS_CATALOG", &out.CatalogName},
		{"DATABRICKS_PAT", &out.Credentials.PersonalAccessToken},
	} {
		*prop.dest = os.Getenv(prop.key)
	}

	if err := out.Validate(); err != nil {
		t.Fatal(err)
	}

	return out
}*/

func TestValidate(t *testing.T) {
	sql.RunValidateTestCases(t, databricksDialect)
}

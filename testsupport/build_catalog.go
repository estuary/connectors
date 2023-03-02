package testsupport

import (
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/estuary/connectors/go/protocol"
	sqlDriver "github.com/estuary/connectors/materialize-sql-json"
	"github.com/estuary/flow/go/protocols/catalog"
	"github.com/stretchr/testify/require"
)

// CatalogExtract invokes `flowctl-go` to build the named catalog
// |sourcePath|, and then invokes the callback with its build
// output database.
func CatalogExtract(t *testing.T, sourcePath string, fn func(*sql.DB) error) error {
	sourcePath, err := filepath.Abs(sourcePath)
	require.NoError(t, err)

	var tempdir = t.TempDir()
	var cmd = exec.Command(
		"flowctl-go",
		"api",
		"build",
		"--build-id", "catalog",
		"--directory", tempdir,
		"--source", sourcePath,
	)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	require.NoError(t, cmd.Run())

	return catalog.Extract(filepath.Join(tempdir, "catalog"), fn)
}

func BindingsFromCatalog(db *sql.DB, name string) (*sqlDriver.StoredSpec, error) {
	protobufSpec, err := catalog.LoadMaterialization(db, name)
	if err != nil {
		return nil, err
	}

	return &sqlDriver.StoredSpec{
		Materialization: name,
		Bindings:        protocol.MaterializationSpecPbToBindings(protobufSpec),
	}, nil
}

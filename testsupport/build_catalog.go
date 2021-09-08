package testsupport

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/estuary/protocols/catalog"
	"github.com/stretchr/testify/require"
)

// BuildCatalog invokes `flowctl check` to build the named |source|
// fixture, which must be a relative file under a `testdata` directory
// of the current test. The processed catalog is loaded and returned.
func BuildCatalog(t *testing.T, source string) *catalog.BuiltCatalog {
	datadir, err := filepath.Abs("testdata")
	require.NoError(t, err)

	workdir, err := ioutil.TempDir("", "catalog")
	require.NoError(t, err)
	defer os.RemoveAll(workdir)

	require.NoError(t, os.Symlink(datadir, filepath.Join(workdir, "testdata")))
	source = filepath.Join(workdir, "testdata", source)

	var cmd = exec.Command("flowctl", "check", "--directory", workdir, "--source", source)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	require.NoError(t, cmd.Run())

	built, err := catalog.LoadFromSQLite(filepath.Join(workdir, "catalog.db"))
	require.NoError(t, err)

	return built
}

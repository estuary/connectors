package sql

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

// snapshotPath is a common set of test snapshots that may be used by SQL materialization connectors
// that produce standard snapshots.
var snapshotPath = "../materialize-sql/.snapshots"

// RunFenceTestCases is a generalized form of test cases over fencing behavior,
// which ought to function with any Client implementation.
func RunFenceTestCases(
	t *testing.T,
	client Client,
	checkpointsPath []string,
	dialect Dialect,
	createTableTpl *template.Template,
	updateFence func(Table, Fence) error,
	dumpTable func(Table) (string, error),
) {

	// runTest takes zero or more key range fixtures, followed by a final pair
	// which is the key range under test.
	var runTest = func(t *testing.T, ranges ...uint32) {
		var ctx = context.Background()

		var metaShape = FlowCheckpointsTable(checkpointsPath...)
		var metaTable, err = ResolveTable(metaShape, dialect)
		require.NoError(t, err)

		createSQL, err := RenderTableTemplate(metaTable, createTableTpl)
		require.NoError(t, err)
		err = client.ExecStatements(ctx, []string{createSQL})
		require.NoError(t, err)

		defer func() {
			err = client.ExecStatements(ctx, []string{fmt.Sprintf("DROP TABLE %s;", metaTable.Identifier)})
			require.NoError(t, err)
		}()

		var fixtures = ranges[:len(ranges)-2]
		var testCase = ranges[len(ranges)-2:]

		for i := 0; i*2 < len(fixtures); i++ {
			var _, err = client.InstallFence(ctx, metaTable, Fence{
				TablePath:       metaShape.Path,
				Materialization: "the/materialization",
				KeyBegin:        ranges[i*2],
				KeyEnd:          ranges[i*2+1],
				Fence:           5,
				Checkpoint:      bytes.Repeat([]byte{byte(i + 1)}, 10),
			})
			require.NoError(t, err)
		}

		// Add an extra fixture from a different materialization.
		_, err = client.InstallFence(ctx, metaTable, Fence{
			TablePath:       metaShape.Path,
			Materialization: "other/one",
			KeyBegin:        0,
			KeyEnd:          math.MaxUint32,
			Fence:           99,
			Checkpoint:      []byte("other-checkpoint"),
		})
		require.NoError(t, err)

		dump1, err := dumpTable(metaTable)
		require.NoError(t, err)

		// Install the fence under test
		fence, err := client.InstallFence(ctx, metaTable, Fence{
			TablePath:       metaShape.Path,
			Materialization: "the/materialization",
			KeyBegin:        testCase[0],
			KeyEnd:          testCase[1],
			Fence:           0,
			Checkpoint:      nil,
		})
		require.NoError(t, err)

		dump2, err := dumpTable(metaTable)
		require.NoError(t, err)

		// Update it once.
		fence.Checkpoint = append(fence.Checkpoint, []byte{0, 0, 0, 0, 0, 0, 0, 0}...)
		require.NoError(t, updateFence(metaTable, fence))

		dump3, err := dumpTable(metaTable)
		require.NoError(t, err)

		snapshotter := cupaloy.New(cupaloy.SnapshotSubdirectory(snapshotPath))

		snapshotter.SnapshotT(t,
			"After installing fixtures:\n"+dump1+
				"\nAfter install fence under test:\n"+dump2+
				"\nAfter update:\n"+dump3)
	}

	// If a fence exactly matches a checkpoint, we'll fence that checkpoint and its parent
	// but not siblings. The used checkpoint is that of the exact match.
	t.Run("exact match", func(t *testing.T) {
		runTest(t,
			0, 99, // Unrelated sibling.
			100, 199, // Exactly matched.
			200, 299, // Unrelated sibling.
			0, 1000, // Old parent.
			100, 199)
	})
	// If a fence sub-divides a parent, we'll fence the parent and grand parent
	// but not siblings of the parent. The checkpoint is the younger parent.
	t.Run("split from parent", func(t *testing.T) {
		runTest(t,
			0, 499, // Younger uncle.
			500, 799, // Younger parent.
			800, 1000, // Other uncle.
			0, 1000, // Grand parent.
			500, 599)
	})
	// If a new range straddles existing ranges (this shouldn't ever happen),
	// we'll fence the straddled ranges while taking the checkpoint of the parent.
	t.Run("straddle", func(t *testing.T) {
		runTest(t,
			0, 499,
			500, 1000,
			0, 1000,
			400, 599)
	})
	// If a new range covers another (this also shouldn't ever happen),
	// it takes the checkpoint of its parent while also fencing the covered sub-range.
	t.Run("covered child", func(t *testing.T) {
		runTest(t,
			100, 199,
			0, 1000,
			100, 800)
	})
}

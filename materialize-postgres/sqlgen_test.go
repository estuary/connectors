package main

import (
	"database/sql"
	"encoding/base64"
	"strings"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/go/protocol"
	sqlDriver "github.com/estuary/connectors/materialize-sql-json"
	"github.com/estuary/connectors/testsupport"
	"github.com/stretchr/testify/require"
)

func TestSQLGeneration(t *testing.T) {
	var bindings []protocol.ApplyBinding
	require.NoError(t, testsupport.CatalogExtract(t, "testdata/flow.yaml",
		func(db *sql.DB) error {
			var err error
			bindings, err = protocol.BindingsFromCatalog(db, "test/sqlite")
			return err
		}))

	// TODO(whb): These keys are manually set as "nullable" for now to test the query generation
	// with nullable keys. Once flow supports nullable keys natively, this should be cleaned up.
	bindings[0].Collection.Projections[7].Inference.Exists = protocol.MayExist
	bindings[0].Collection.Projections[7].Inference.Types = append(bindings[0].Collection.Projections[7].Inference.Types, "null")
	bindings[1].Collection.Projections[2].Inference.Exists = protocol.MayExist
	bindings[1].Collection.Projections[2].Inference.Types = append(bindings[1].Collection.Projections[2].Inference.Types, "null")

	var shape1 = sqlDriver.BuildTableShape("test/sqlite", bindings, 0, tableConfig{
		Schema: "a-schema",
		Table:  "target_table",
		Delta:  false,
	})
	var shape2 = sqlDriver.BuildTableShape("test/sqlite", bindings, 1, tableConfig{
		Schema: "",
		Table:  "Delta Updates",
		Delta:  true,
	})
	shape2.Document = nil // TODO(johnny): this is a bit gross.

	table1, err := sqlDriver.ResolveTable(shape1, pgDialect)
	require.NoError(t, err)
	table2, err := sqlDriver.ResolveTable(shape2, pgDialect)
	require.NoError(t, err)

	var snap strings.Builder

	for _, tpl := range []*template.Template{
		tplCreateTargetTable,
		tplCreateLoadTable,
		tplPrepLoadInsert,
		tplExecLoadInsert,
		tplLoadQuery,
		tplPrepStoreInsert,
		tplExecStoreInsert,
		tplPrepStoreUpdate,
		tplExecStoreUpdate,
	} {
		for _, tbl := range []sqlDriver.Table{table1, table2} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			snap.WriteString("--- Begin " + testcase + " ---\n")
			require.NoError(t, tpl.Execute(&snap, &tbl))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	var fence = sqlDriver.Fence{
		TablePath:       sqlDriver.TablePath{"path", "To", "checkpoints"},
		Checkpoint:      base64.StdEncoding.EncodeToString([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
		Fence:           123,
		Materialization: "some/Materialization",
		KeyBegin:        0x00112233,
		KeyEnd:          0xffeeddcc,
	}
	snap.WriteString("--- Begin Fence Install ---\n")
	require.NoError(t, tplInstallFence.Execute(&snap, fence))
	snap.WriteString("--- End Fence Install ---\n")

	snap.WriteString("--- Begin Fence Update ---\n")
	require.NoError(t, tplUpdateFence.Execute(&snap, fence))
	snap.WriteString("--- End Fence Update ---\n")

	cupaloy.SnapshotT(t, snap.String())
}

func TestDateTimeColumn(t *testing.T) {
	var mapped, err = pgDialect.MapType(&sqlDriver.Projection{
		Projection: &protocol.Projection{
			Inference: protocol.Inference{
				Types:   []string{"string"},
				String_: &protocol.StringInference{Format: "date-time"},
				Exists:  protocol.MustExist,
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "TIMESTAMPTZ NOT NULL", mapped.DDL)

	parsed, err := mapped.Converter("2022-04-04T10:09:08.234567Z")
	require.Equal(t, "2022-04-04T10:09:08.234567Z", parsed)
	require.NoError(t, err)
}

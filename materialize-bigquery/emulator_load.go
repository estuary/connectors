package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	log "github.com/sirupsen/logrus"
)

// The production Load and Acknowledge paths query temporary EXTERNAL tables
// backed by gs:// URIs via Query.TableDefinitions. No BigQuery emulator
// implements external data configuration, so when the goccy emulator is
// detected the connector instead materializes each external table as a real
// NATIVE table: create it with the external schema, populate it with a native
// GCS load job, run the query against it, and drop it afterward.
//
// Two consequences of the tables being real rather than query-scoped:
//
//   - The rendered queries reference the tables by their unqualified
//     flow_temp_table_<binding> names, so emulator queries must set
//     DefaultProjectID/DefaultDatasetID for those references to resolve. The
//     defaults are NOT set for production queries: there every table reference
//     is either a fully-qualified 3-part identifier or a query-scoped external
//     table definition, so the defaults are never consulted — but setting them
//     anyway would change every job configuration sent to SaaS BigQuery and
//     would turn an accidentally-unqualified reference from a loud error into
//     a silent read of an unrelated table. Not provably safe, so it stays
//     emulator-gated.
//
//   - The same flow_temp_table_<binding> name is used by Load's key files and
//     Acknowledge's store files, and the pipelined transactor protocol lets
//     the Load of one transaction overlap the Acknowledge of the previous one.
//     Query-scoped external definitions cannot collide, but real tables can,
//     so client.emulatorTempTableMu serializes the emulator sections of Load
//     and Acknowledge. Throughput is irrelevant on an emulator.
type emulatorTempTable struct {
	name   string
	schema bigquery.Schema
	uris   []string
}

// prepareEmulatorTempTables creates and populates a native table for each
// described external table, returning a cleanup function that drops whatever
// was created. Cleanup must run on both success and failure paths; on failure
// prepareEmulatorTempTables invokes it itself and callers only clean up after
// a nil error.
func (c client) prepareEmulatorTempTables(ctx context.Context, tables []emulatorTempTable) (func(), error) {
	var created []string

	cleanup := func() {
		// A background context so temp tables are still dropped when the
		// request context that produced them has already been canceled.
		bgCtx := context.Background()
		for _, name := range created {
			if err := c.bigqueryClient.DatasetInProject(c.cfg.ProjectID, c.cfg.Dataset).Table(name).Delete(bgCtx); err != nil {
				log.WithFields(log.Fields{
					"table": name,
					"error": err,
				}).Warn("failed to delete emulator temp table")
			}
		}
	}

	for _, tt := range tables {
		table := c.bigqueryClient.DatasetInProject(c.cfg.ProjectID, c.cfg.Dataset).Table(tt.name)

		// Drop any leftover from a run that crashed between creating the table
		// and cleaning it up, since Create fails on an existing table.
		_ = table.Delete(ctx)

		if err := table.Create(ctx, &bigquery.TableMetadata{Schema: tt.schema}); err != nil {
			cleanup()
			return nil, fmt.Errorf("creating emulator temp table %q: %w", tt.name, err)
		}
		created = append(created, tt.name)

		// The staged files are uncompressed JSON on the emulator path (see
		// stagedFileClient.disableGzip), so no Compression is set here.
		gcsRef := bigquery.NewGCSReference(tt.uris...)
		gcsRef.SourceFormat = bigquery.JSON

		job, err := table.LoaderFrom(gcsRef).Run(ctx)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("starting load job for emulator temp table %q: %w", tt.name, err)
		}
		status, err := job.Wait(ctx)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("waiting for load job for emulator temp table %q: %w", tt.name, err)
		} else if status.Err() != nil {
			cleanup()
			return nil, fmt.Errorf("load job for emulator temp table %q failed with status %+v: %w", tt.name, status, status.Err())
		}
	}

	return cleanup, nil
}

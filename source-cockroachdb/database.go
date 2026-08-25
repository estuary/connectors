package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/invopop/jsonschema"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

type cockroachdbDatabase struct {
	config       *Config
	conn         *pgxpool.Pool   // Connection pool used for discovery and backfill queries.
	featureFlags map[string]bool // Parsed feature flag settings with defaults applied.

	// snapshot.timestamp is the HLC at which backfills read (`AS OF SYSTEM TIME`) and from which
	// the changefeed (re)starts. It begins at the capture's start position and is advanced to each
	// resolved timestamp the changefeed reports. Advancing it is what makes unfiltered streaming
	// safe: a resolved timestamp means every change up to it has already been emitted, so a chunk
	// read at that timestamp can never publish row state older than an already-emitted change
	// event, while changes after it are still streamed afterwards and supersede the chunk.
	snapshot struct {
		sync.Mutex
		timestamp string
	}

	explained       map[sqlcapture.StreamID]struct{} // Tracks tables which have had their backfill query EXPLAINed this invocation.
	tableStatistics struct {
		sync.Mutex
		counts map[sqlcapture.StreamID]int // Cached estimated row counts.
	}
}

func (db *cockroachdbDatabase) HistoryMode() bool {
	return db.config.HistoryMode
}

// snapshotTimestamp returns the current backfill/changefeed anchor timestamp.
func (db *cockroachdbDatabase) snapshotTimestamp() string {
	db.snapshot.Lock()
	defer db.snapshot.Unlock()
	return db.snapshot.timestamp
}

// advanceSnapshotTimestamp moves the anchor timestamp forward. Regressions are ignored so that a
// replayed or out-of-order resolved timestamp can never move backfill reads backwards.
func (db *cockroachdbDatabase) advanceSnapshotTimestamp(hlc string) {
	db.snapshot.Lock()
	defer db.snapshot.Unlock()
	if db.snapshot.timestamp == "" || compareHLC(hlc, db.snapshot.timestamp) > 0 {
		db.snapshot.timestamp = hlc
	}
}

func (db *cockroachdbDatabase) Close(ctx context.Context) error {
	db.conn.Close()
	return nil
}

func (db *cockroachdbDatabase) SourceMetadataSchema(writeSchema bool) *jsonschema.Schema {
	var sourceSchema = (&jsonschema.Reflector{
		ExpandedStruct:            true,
		DoNotReference:            true,
		AllowAdditionalProperties: writeSchema,
	}).Reflect(&cockroachdbSource{})
	sourceSchema.Version = ""
	if db.config.Advanced.SourceTag == "" {
		sourceSchema.Properties.Delete("tag")
	}
	return sourceSchema
}

// FallbackCollectionKey is used for tables which have no usable primary key. The HLC cursor
// position ("loc") is the best available change-ordering value in that situation.
func (db *cockroachdbDatabase) FallbackCollectionKey() []string {
	return []string{"/_meta/source/loc"}
}

func (db *cockroachdbDatabase) RediscoveryInterval() time.Duration {
	return sqlcapture.ParseRediscoveryInterval(db.config.Advanced.RediscoveryInterval)
}

func (db *cockroachdbDatabase) ReplicationPollingInterval() time.Duration {
	return 0 // Changefeed rows arrive continuously rather than being polled for.
}

func (db *cockroachdbDatabase) ShouldBackfill(streamID sqlcapture.StreamID) bool {
	if db.config.Advanced.SkipBackfills == "*.*" {
		return false
	}
	if db.config.Advanced.SkipBackfills != "" {
		for _, skipStreamID := range strings.Split(db.config.Advanced.SkipBackfills, ",") {
			if strings.EqualFold(streamID.String(), skipStreamID) {
				return false
			}
		}
	}
	return true
}

// RequestTxIDs is a no-op for CockroachDB. The transaction-ID plumbing exists for connectors which
// can cheaply attach a transaction identifier to replication events; sinkless changefeeds don't
// surface one, so there's nothing to enable.
func (db *cockroachdbDatabase) RequestTxIDs(schema, table string) {}

func (db *cockroachdbDatabase) EstimatedRowCounts(ctx context.Context, tables []sqlcapture.TableID) (map[sqlcapture.TableID]int, error) {
	db.tableStatistics.Lock()
	defer db.tableStatistics.Unlock()
	if db.tableStatistics.counts == nil {
		db.tableStatistics.counts = make(map[sqlcapture.StreamID]int)
	}

	var result = make(map[sqlcapture.TableID]int, len(tables))
	for _, table := range tables {
		var streamID = sqlcapture.JoinStreamID(table.Schema, table.Table)
		if count, ok := db.tableStatistics.counts[streamID]; ok {
			result[table] = count
			continue
		}
		// `SHOW TABLES` exposes CockroachDB's optimizer row-count estimate without a full scan.
		var count int
		var query = fmt.Sprintf("SELECT estimated_row_count FROM [SHOW TABLES FROM %q] WHERE table_name = $1", table.Schema)
		if err := db.conn.QueryRow(ctx, query, table.Table).Scan(&count); err != nil {
			logrus.WithFields(logrus.Fields{"table": streamID, "err": err}).Debug("error querying estimated row count")
			count = 0
		}
		db.tableStatistics.counts[streamID] = count
		result[table] = count
	}
	return result, nil
}

// PeriodicChecks has nothing to do for CockroachDB. Unlike PostgreSQL there's no replication slot
// whose disk usage we need to keep an eye on.
func (db *cockroachdbDatabase) PeriodicChecks(ctx context.Context) error {
	return nil
}

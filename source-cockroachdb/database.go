package main

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/invopop/jsonschema"
	"github.com/jackc/pgx/v5/pgxpool"
)

type cockroachdbDatabase struct {
	config       *Config
	conn         *pgxpool.Pool   // Connection pool used for discovery and backfill queries.
	featureFlags map[string]bool // Parsed feature flag settings with defaults applied.

	// snapshot.timestamp is the HLC at which backfills read (`AS OF SYSTEM TIME`) and from which
	// the changefeed (re)starts. It advances to each resolved timestamp the changefeed reports,
	// which is what keeps unfiltered streaming safe (see ScanTableChunk in backfill.go).
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

// RequestTxIDs is a no-op: sinkless changefeeds don't surface a transaction identifier to attach.
func (db *cockroachdbDatabase) RequestTxIDs(schema, table string) {}

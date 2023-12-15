package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/snowflakedb/gosnowflake"
)

func connectSnowflake(ctx context.Context, cfg *config) (*sql.DB, error) {
	log.WithFields(log.Fields{
		"host":     cfg.Host,
		"user":     cfg.User,
		"database": cfg.Database,
	}).Info("connecting to database")

	// The Snowflake client library logs some stuff at ERROR severity which
	// we don't actually want in our task logs. A Snowflake query failing is
	// not necessarily an error that the user needs to be told about, and when
	// it is our normal error propagation will handle it.
	gosnowflake.GetLogger().SetOutput(io.Discard)

	var conn, err = sql.Open("snowflake", cfg.ToURI())
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}
	return conn, nil
}

// snowflakeObject represents the (schema, name) tuple identifying a Snowflake object
// such as a table, stream, or view.
type snowflakeObject struct {
	Schema string
	Name   string
}

func (t snowflakeObject) String() string {
	return t.Schema + "." + t.Name
}

// UniqueID returns a string which can be used to uniquely identify resources (such
// as streams and staging tables) related to the source table.
//
// This identifier combines a SHA-256 hash derived from the table's name and schema with
// a human-readable suffix (truncated to ensure that we never exceed Snowflake identifier
// length limits).
func (t snowflakeObject) UniqueID() string {
	var hash = sha256.Sum256([]byte(fmt.Sprintf("schema: %s, table: %s", t.Schema, t.Name)))
	var suffix = strings.ToUpper(fmt.Sprintf("%s_%s", t.Schema, t.Name))
	if len(suffix) > 128 {
		suffix = suffix[:128]
	}
	return fmt.Sprintf("%X_%s", string(hash[:]), suffix)
}

// QuotedName returns a quoted and fully-qualified version of the table name which
// can be directly interpolated into a Snowflake SQL query.
func (t snowflakeObject) QuotedName() string {
	return quoteSnowflakeIdentifier(t.Schema) + "." + quoteSnowflakeIdentifier(t.Name)
}

func (t snowflakeObject) MarshalText() ([]byte, error) {
	return []byte(escapeTildes(t.Schema) + "/" + escapeTildes(t.Name)), nil
}

func escapeTildes(x string) string {
	x = strings.ReplaceAll(x, "~", "~0")
	x = strings.ReplaceAll(x, "/", "~1")
	return x
}

func (t *snowflakeObject) UnmarshalText(text []byte) error {
	var bits = strings.Split(string(text), "/")
	if len(bits) != 2 {
		return fmt.Errorf("malformed object name %q", string(text))
	}
	t.Schema = unescapeTildes(bits[0])
	t.Name = unescapeTildes(bits[1])
	return nil
}

func unescapeTildes(x string) string {
	x = strings.ReplaceAll(x, "~1", "/")
	x = strings.ReplaceAll(x, "~0", "~")
	return x
}

// createStagingTable captures the latest changes from a change stream into a staging
// table with the specified sequence number. The operation is idempotent, if a staging
// table with the desired sequence number already exists then this does nothing.
func createStagingTable(ctx context.Context, cfg *config, db *sql.DB, table snowflakeObject, seqno int) (snowflakeObject, error) {
	var changeStreamName = changeStreamName(cfg, table)
	var stagingTableName = stagingTableName(cfg, table, seqno)
	var createStagingTableQuery = fmt.Sprintf(
		`CREATE TRANSIENT TABLE IF NOT EXISTS %s AS SELECT * FROM %s;`,
		stagingTableName.QuotedName(),
		changeStreamName.QuotedName(),
	)
	if _, err := db.ExecContext(ctx, createStagingTableQuery); err != nil {
		return snowflakeObject{}, fmt.Errorf("error reading stream %q into staging table %q: %w", changeStreamName, stagingTableName, err)
	}
	return stagingTableName, nil
}

// createInitialCloneTable clones the current state of the source table into a staging table
// with the specified sequence number. The operation is idempotent, if a staging table with
// the desired sequence number already exists then this does nothing.
func createInitialCloneTable(ctx context.Context, cfg *config, db *sql.DB, table snowflakeObject, seqno int) (snowflakeObject, error) {
	var sourceTableName = table
	var stagingTableName = stagingTableName(cfg, table, seqno)
	var createStagingTableQuery = fmt.Sprintf(
		`CREATE TRANSIENT TABLE IF NOT EXISTS %s CLONE %s;`,
		stagingTableName.QuotedName(),
		sourceTableName.QuotedName(),
	)
	if _, err := db.ExecContext(ctx, createStagingTableQuery); err != nil {
		return snowflakeObject{}, fmt.Errorf("error cloning source table %q into staging table %q: %w", sourceTableName, stagingTableName, err)
	}
	return stagingTableName, nil
}

// changeStream returns the table name of the change stream which corresponds to a
// specific source table.
func changeStreamName(cfg *config, table snowflakeObject) snowflakeObject {
	return snowflakeObject{
		Schema: cfg.Advanced.FlowSchema,
		Name:   fmt.Sprintf("flow_stream_%s", table.UniqueID()),
	}
}

// stagingTableName returns the table name of a staging table with the specified
// sequence number corresponding to a specific source table.
func stagingTableName(cfg *config, table snowflakeObject, seqno int) snowflakeObject {
	return snowflakeObject{
		Schema: cfg.Advanced.FlowSchema,
		Name:   fmt.Sprintf("flow_staging_%012d_%s", seqno, table.UniqueID()),
	}
}

// quoteSnowflakeIdentifier quotes an identifier so that it may be interpolated into
// a Snowflake SQL query.
//
// Per https://docs.snowflake.com/en/sql-reference/identifiers-syntax an identifier may
// be quoted by surrounding it in double-quotes, and replacing any double-quotes within
// the identifier with a `""` pair.
func quoteSnowflakeIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

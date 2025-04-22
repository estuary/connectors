package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/sirupsen/logrus"
)

type mysqlClient interface {
	Prepare(query string) (*client.Stmt, error)
	Execute(command string, args ...any) (*mysql.Result, error)
	Close() error
}

type mysqlConnection struct {
	inner *client.Conn
}

func (conn *mysqlConnection) Prepare(query string) (*client.Stmt, error) {
	return conn.inner.Prepare(query)
}

func (conn *mysqlConnection) Execute(command string, args ...any) (*mysql.Result, error) {
	return conn.inner.Execute(command, args...)
}

func (conn *mysqlConnection) Close() error {
	return conn.inner.Close()
}

type mysqlDatabase struct {
	versionString              string // The raw contents of the 'version' system variable
	versionProduct             string // Usually either "MySQL" or "MariaDB"
	versionMajor, versionMinor int    // The major/minor version the server is running

	config *Config
	conn   mysqlClient

	explained        map[string]struct{} // Tracks tables which have had an `EXPLAIN` run on them during this connector invocation.
	datetimeLocation *time.Location      // The location in which to interpret DATETIME column values as timestamps.
	includeTxIDs     map[string]bool     // Tracks which tables should have XID properties in their replication metadata.

	featureFlags          map[string]bool // Parsed feature flag settings with defaults applied
	initialBackfillCursor string          // When set, this cursor will be used instead of the current WAL end when a backfill resets the cursor
	forceResetCursor      string          // When set, this cursor will be used instead of the checkpointed one regardless of backfilling. DO NOT USE unless you know exactly what you're doing.
}

func (db *mysqlDatabase) HistoryMode() bool {
	return db.config.HistoryMode
}

// queryDatabaseVersion examines the server version string to figure out what product
// and release version we're talking to, and saves the results for later use.
func (db *mysqlDatabase) queryDatabaseVersion() error {
	var results, err = db.conn.Execute(`SELECT @@GLOBAL.version;`)
	if err != nil {
		return fmt.Errorf("unable to query database version: %w", err)
	} else if len(results.Values) != 1 || len(results.Values[0]) != 1 {
		return fmt.Errorf("unable to query database version: malformed response")
	}
	defer results.Close()

	db.versionString = string(results.Values[0][0].AsString())
	db.versionProduct = "MySQL"
	if strings.Contains(strings.ToLower(db.versionString), "mariadb") {
		db.versionProduct = "MariaDB"
	}
	major, minor, err := sqlcapture.ParseVersion(db.versionString)
	if err != nil {
		return fmt.Errorf("unable to parse database version from %q: %w", db.versionString, err)
	}
	db.versionMajor = major
	db.versionMinor = minor

	logrus.WithFields(logrus.Fields{
		"version": db.versionString,
		"product": db.versionProduct,
		"major":   db.versionMajor,
		"minor":   db.versionMinor,
	}).Info("queried database version")
	return nil
}

type binlogStatus struct {
	Position mysql.Position // The current binlog filename and offset
	Extra    map[string]any // Any other result columns from `SHOW MASTER STATUS`
}

// queryBinlogStatus fetches the current binary logging position and configuration using
// the SHOW MASTER STATUS / SHOW BINARY LOG STATUS query (depending on the server version).
func (db *mysqlDatabase) queryBinlogStatus() (*binlogStatus, error) {
	// The 'SHOW MASTER STATUS' query is the only form that works on MySQL <8.0.22, but
	// support was dropped in MySQL 8.4.0 so we have to select the appropriate query for
	// the server version we're connected to.
	var statusQuery = "SHOW MASTER STATUS;"
	if db.versionProduct == "MySQL" && ((db.versionMajor == 8 && db.versionMinor >= 4) || db.versionMajor > 8) {
		statusQuery = "SHOW BINARY LOG STATUS;"
	}

	var results, err = db.conn.Execute(statusQuery)
	if err != nil {
		return nil, fmt.Errorf("error querying binlog status: %w", err)
	}
	if len(results.Values) == 0 {
		return nil, fmt.Errorf("error querying binlog status: empty result set (is binary logging enabled?)")
	}
	defer results.Close()

	// Extract the binlog filename and offset
	var row = results.Values[0]
	var filename = string(row[0].AsString())
	var offset = uint32(row[1].AsInt64())

	// Copy any/all additional result columns into the 'extra' map
	var extra = make(map[string]any)
	for idx := 2; idx < len(row); idx++ {
		var key = string(results.Fields[idx].Name)
		var val = row[idx].Value()
		if bs, ok := val.([]byte); ok {
			val = string(bs)
		}
		extra[key] = val
	}

	return &binlogStatus{
		Position: mysql.Position{
			Name: filename,
			Pos:  offset,
		},
		Extra: extra,
	}, nil
}

func (db *mysqlDatabase) queryBinlogPosition() (mysql.Position, error) {
	var status, err = db.queryBinlogStatus()
	if err != nil {
		return mysql.Position{}, err
	}
	return status.Position, nil
}

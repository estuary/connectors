package main

import (
	"context"
	"fmt"
	"regexp"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/sirupsen/logrus"
)

func (db *cockroachdbDatabase) SetupPrerequisites(ctx context.Context) []error {
	if db.featureFlags["skip_prerequisites"] {
		logrus.Warn("skipping all prerequisite checks because the 'skip_prerequisites' feature flag is set")
		return nil
	}

	var errs []error
	db.logVersion(ctx)
	if err := db.prerequisiteRangefeed(ctx); err != nil {
		errs = append(errs, err)
	}
	return errs
}

func (db *cockroachdbDatabase) logVersion(ctx context.Context) {
	var version string
	if err := db.conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		logrus.WithField("err", err).Warn("unable to query database version")
		return
	}
	logrus.WithField("version", version).Info("queried database version")
}

// prerequisiteRangefeed verifies that rangefeeds are enabled cluster-wide. Changefeeds (and thus
// this connector) require `kv.rangefeed.enabled = true`; without it the changefeed statement fails
// at startup with an opaque error, so we surface a clear instruction instead.
func (db *cockroachdbDatabase) prerequisiteRangefeed(ctx context.Context) error {
	var enabled bool
	if err := db.conn.QueryRow(ctx, "SHOW CLUSTER SETTING kv.rangefeed.enabled").Scan(&enabled); err != nil {
		return cerrors.NewUserError(err, "unable to query the 'kv.rangefeed.enabled' cluster setting; the capture user may lack the necessary privileges")
	}
	if !enabled {
		return cerrors.NewUserError(nil, "changefeeds require rangefeeds to be enabled: run `SET CLUSTER SETTING kv.rangefeed.enabled = true;` as an admin user")
	}
	return nil
}

func (db *cockroachdbDatabase) SetupTablePrerequisites(ctx context.Context, tables []sqlcapture.TableID) map[sqlcapture.TableID]error {
	var errs = make(map[sqlcapture.TableID]error)
	if db.featureFlags["skip_prerequisites"] {
		return errs
	}

	// Discovery works out whether each table has a key this connector can capture with (see the
	// key logic in DiscoverTableDetails); surface a clear per-table error for the ones which
	// don't, rather than failing obscurely (or worse, capturing incorrectly) later.
	var discovered, err = db.DiscoverTableDetails(ctx, tables)
	if err != nil {
		logrus.WithField("err", err).Warn("unable to discover table details for prerequisite checks")
	}

	for _, table := range tables {
		var streamID = sqlcapture.JoinStreamID(table.Schema, table.Table)
		if info, ok := discovered[streamID]; ok {
			if details, ok := info.ExtraDetails.(*cockroachdbTableDetails); ok && details.unusableKeyReason != "" {
				errs[table] = fmt.Errorf("cannot capture table %q: %s", streamID, details.unusableKeyReason)
				continue
			}
		}

		// Verify that the capture user can read from the table. CHANGEFEED also requires the
		// CHANGEFEED privilege on the table, but that's awkward to probe portably; if it's missing
		// the changefeed statement will surface a clear permission error when streaming begins.
		var query = fmt.Sprintf("SELECT 1 FROM %s.%s LIMIT 0", quoteIdentifier(table.Schema), quoteIdentifier(table.Table))
		if _, err := db.conn.Exec(ctx, query); err != nil {
			errs[table] = fmt.Errorf("user %q cannot read from table %q: %w", db.config.User, streamID, err)
			continue
		}

		if err := db.prerequisiteSingleColumnFamily(ctx, table); err != nil {
			errs[table] = err
		}
	}
	return errs
}

// familyLineRegexp matches the column-family lines of a SHOW CREATE TABLE statement. Quoted
// identifiers can't false-match because a column definition line always starts with the (quoted)
// column name rather than the bare keyword.
var familyLineRegexp = regexp.MustCompile(`(?m)^\s*FAMILY\s`)

// prerequisiteSingleColumnFamily rejects tables with multiple column families at validation time.
// A changefeed on such a table requires the `split_column_families` option and then emits partial
// per-family rows under `<table>.<family>` names, neither of which this connector supports; the
// check surfaces that clearly instead of an opaque changefeed error mid-capture. The check parses
// SHOW CREATE TABLE (there's no supported catalog view for families), so if the statement fails
// we skip the check rather than block the capture.
func (db *cockroachdbDatabase) prerequisiteSingleColumnFamily(ctx context.Context, table sqlcapture.TableID) error {
	var streamID = sqlcapture.JoinStreamID(table.Schema, table.Table)
	var createStatement string
	var query = fmt.Sprintf("SELECT create_statement FROM [SHOW CREATE TABLE %s.%s]", quoteIdentifier(table.Schema), quoteIdentifier(table.Table))
	if err := db.conn.QueryRow(ctx, query).Scan(&createStatement); err != nil {
		logrus.WithFields(logrus.Fields{"table": streamID, "err": err}).Debug("unable to inspect column families")
		return nil
	}
	if families := len(familyLineRegexp.FindAllString(createStatement, -1)); families > 1 {
		return fmt.Errorf("cannot capture table %q: it has %d column families and changefeeds on multi-column-family tables are not supported", streamID, families)
	}
	return nil
}

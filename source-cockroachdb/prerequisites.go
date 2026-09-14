package main

import (
	"context"
	"fmt"
	"regexp"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
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
		return fmt.Errorf("unable to query the 'kv.rangefeed.enabled' cluster setting; the capture user may lack the necessary privileges: %w", err)
	}
	if !enabled {
		return fmt.Errorf("changefeeds require rangefeeds to be enabled: run `SET CLUSTER SETTING kv.rangefeed.enabled = true;` as an admin user")
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

	// The read-access and column-family checks below are independent per table, so they're run
	// concurrently rather than one table at a time.
	var results = make([]error, len(tables))
	var group, groupCtx = errgroup.WithContext(ctx)
	for i, table := range tables {
		var streamID = sqlcapture.JoinStreamID(table.Schema, table.Table)
		if info, ok := discovered[streamID]; ok {
			if details, ok := info.ExtraDetails.(*cockroachdbTableDiscoveryDetails); ok && details.unusableKeyReason != "" {
				results[i] = fmt.Errorf("cannot capture table %q: %s", streamID, details.unusableKeyReason)
				continue
			}
		}

		group.Go(func() error {
			// Verify that the capture user can read from the table. CHANGEFEED also requires the
			// CHANGEFEED privilege on the table, but that's awkward to probe portably; if it's
			// missing the changefeed statement will surface a clear permission error when
			// streaming begins.
			var query = fmt.Sprintf("SELECT 1 FROM %s.%s LIMIT 0", quoteIdentifier(table.Schema), quoteIdentifier(table.Table))
			if _, err := db.conn.Exec(groupCtx, query); err != nil {
				results[i] = fmt.Errorf("user %q cannot read from table %q: %w", db.config.User, streamID, err)
				return nil
			}
			if err := db.prerequisiteSingleColumnFamily(groupCtx, table); err != nil {
				results[i] = err
			}
			return nil
		})
	}
	group.Wait()

	for i, table := range tables {
		if results[i] != nil {
			errs[table] = results[i]
		}
	}
	return errs
}

// familyLineRegexp matches the column-family lines of a SHOW CREATE TABLE statement. Quoted
// identifiers can't false-match because a column definition line always starts with the (quoted)
// column name rather than the bare keyword.
var familyLineRegexp = regexp.MustCompile(`(?m)^\s*FAMILY\s`)

// prerequisiteSingleColumnFamily rejects tables with multiple column families at validation time,
// since a changefeed on such a table requires `split_column_families` and emits partial per-family
// rows this connector doesn't support. It parses SHOW CREATE TABLE (there's no catalog view for
// families), skipping the check rather than blocking the capture if that statement fails.
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

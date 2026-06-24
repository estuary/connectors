package main

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/segmentio/encoding/json"
	"github.com/sirupsen/logrus"
	"vitess.io/vitess/go/vt/sqlparser"
)

// Query Events in the MySQL binlog are normalized enough that we can use
// prefix matching to detect many types of query that we just completely
// don't care about. This is good, because the Vitess SQL parser disagrees
// with the binlog Query Events for some statements like GRANT and CREATE USER.
// TODO(johnny): SET STATEMENT is not safe in the general case, and we want to re-visit
// by extracting and ignoring a SET STATEMENT stanza prior to parsing.
var silentIgnoreQueriesRe = regexp.MustCompile(`(?i)^(BEGIN|COMMIT|ROLLBACK|SAVEPOINT .*|# [^\n]*)$`)
var createDefinerRegex = `CREATE\s*(OR REPLACE){0,1}\s*(ALGORITHM\s*=\s*[^ ]+)*\s*DEFINER`
var ignoreQueriesRe = regexp.MustCompile(`(?i)^(BEGIN|COMMIT|GRANT|REVOKE|CREATE USER|` + createDefinerRegex + `|DROP USER|ALTER USER|DROP PROCEDURE|DROP FUNCTION|DROP TRIGGER|SET STATEMENT|CREATE EVENT|ALTER EVENT|DROP EVENT)`)

// queryAnalyzer bundles the effectively-constant state needed to interpret
// binlog query events: the SQL parser plus the feature flags and database
// flavor which influence how DDL is translated into metadata changes. It's
// constructed once per replication stream and reused for every query event.
type queryAnalyzer struct {
	parser       *sqlparser.Parser
	featureFlags map[string]bool
	isMariaDB    bool
}

// queryEffect describes a single state change from applying a query event.
type queryEffect interface {
	isQueryEffect()
}

type dropTableEffect struct {
	StreamID sqlcapture.StreamID
	Cause    string
}

type updateMetadataEffect struct {
	StreamID sqlcapture.StreamID
	Metadata *mysqlTableMetadata
}

func (*dropTableEffect) isQueryEffect()      {}
func (*updateMetadataEffect) isQueryEffect() {}

func (rs *mysqlReplicationStream) handleQuery(ctx context.Context, qa *queryAnalyzer, schema, query string) error {
	var snapshot = rs.activeTables()
	var effects, err = qa.analyzeQuery(snapshot, schema, query)
	if err != nil {
		return err
	}

	for _, effect := range effects {
		switch effect := effect.(type) {
		case *dropTableEffect:
			// Indicate that change streaming for this table has failed and deactivate it.
			if err := rs.emitEvent(ctx, &sqlcapture.TableDropEvent{
				StreamID: effect.StreamID,
				Cause:    effect.Cause,
			}); err != nil {
				return err
			} else if err := rs.deactivateTable(effect.StreamID); err != nil {
				return err
			}
		case *updateMetadataEffect:
			// Update local metadata
			rs.tables.Lock()
			rs.tables.metadata[effect.StreamID] = effect.Metadata
			rs.tables.Unlock()

			// Emit metadata update event
			var bs, err = json.Marshal(effect.Metadata)
			if err != nil {
				return fmt.Errorf("error serializing metadata JSON for %q: %w", effect.StreamID, err)
			}
			if err := rs.emitEvent(ctx, &sqlcapture.MetadataEvent{
				StreamID: effect.StreamID,
				Metadata: json.RawMessage(bs),
			}); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unhandled query effect: %#v", effect)
		}
	}
	return nil
}

func (qa *queryAnalyzer) analyzeQuery(tables *activeTablesView, schema, query string) ([]queryEffect, error) {
	// There are basically three types of query events we might receive:
	//   * An INSERT/UPDATE/DELETE query is an error, we should never receive
	//     these if the server's `binlog_format` is set to ROW as it should be
	//     for CDC to work properly.
	//   * Various DDL queries like CREATE/ALTER/DROP/TRUNCATE/RENAME TABLE,
	//     which should in general be treated like errors *if they occur on
	//     a table we're capturing*, though we expect to eventually handle
	//     some subset of possible alterations like adding/renaming columns.
	//   * Some other queries like BEGIN and CREATE DATABASE and other things
	//     that we don't care about, either because they change things that
	//     don't impact our capture or because we get the relevant information
	//     by some other means.
	if silentIgnoreQueriesRe.MatchString(query) {
		logrus.WithField("query", query).Trace("silently ignoring query event")
		return nil, nil
	}
	if ignoreQueriesRe.MatchString(query) {
		logrus.WithField("query", query).Info("ignoring query event")
		return nil, nil
	}
	logrus.WithField("query", query).Info("handling query event")

	var stmt, err = qa.parser.Parse(query)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"query": query,
			"err":   err,
		}).Warn("failed to parse query event, ignoring it")
		return nil, nil
	}
	logrus.WithField("stmt", fmt.Sprintf("%#v", stmt)).Debug("parsed query")

	var effects []queryEffect

	switch stmt := stmt.(type) {
	case *sqlparser.CreateDatabase, *sqlparser.AlterDatabase, *sqlparser.CreateTable, *sqlparser.Savepoint, *sqlparser.Flush:
		logrus.WithField("query", query).Debug("ignoring benign query")
	case *sqlparser.CreateView, *sqlparser.AlterView, *sqlparser.DropView:
		// All view creation/deletion/alterations should be fine to ignore since we don't capture from views.
		logrus.WithField("query", query).Debug("ignoring benign query")
	case *sqlparser.DropDatabase:
		// Remember that In MySQL land "database" is a synonym for the usual SQL concept "schema"
		if streamIDs := tables.inSchema(stmt.GetDatabaseName()); len(streamIDs) > 0 {
			logrus.WithFields(logrus.Fields{
				"query":     query,
				"schema":    stmt.GetDatabaseName(),
				"streamIDs": streamIDs,
			}).Info("dropped all tables in schema")
			for _, streamID := range streamIDs {
				effects = append(effects, &dropTableEffect{
					StreamID: streamID,
					Cause:    fmt.Sprintf("schema %q was dropped by query %q", streamID, query),
				})
			}
		} else {
			logrus.WithField("query", query).Debug("ignorable dropped schema (not being captured from)")
		}
	case *sqlparser.AlterTable:
		if streamID := resolveTableName(schema, stmt.Table); tables.active(streamID) {
			// The Vitess SQL parser does not error on DDL it can't fully understand. Instead it
			// silently degrades the result into a partially parsed statement with FullyParsed set
			// to false, typically with empty or incomplete AlterOptions.
			//
			// If this happens we can't reliably apply the alteration to our cached table metadata,
			// so rather than silently capturing against incorrect metadata we mark the table as
			// dropped and rely on a subsequent backfill to reinitialize with correct metadata.
			if !stmt.FullyParsed {
				logrus.WithFields(logrus.Fields{
					"query":  query,
					"stream": streamID,
				}).Warn("unable to fully parse ALTER TABLE on active table, will backfill")

				effects = append(effects, &dropTableEffect{
					StreamID: streamID,
					Cause:    fmt.Sprintf("unable to fully parse alteration of table %q by query %q", streamID, query),
				})
			} else {
				logrus.WithFields(logrus.Fields{
					"query":         query,
					"partitionSpec": stmt.PartitionSpec,
					"alterOptions":  stmt.AlterOptions,
				}).Info("parsed components of ALTER TABLE statement")

				if stmt.PartitionSpec == nil || len(stmt.AlterOptions) != 0 {
					if effect, err := qa.analyzeAlterTable(tables, stmt, query, streamID); err != nil {
						return nil, fmt.Errorf("cannot handle table alteration %q: %w", query, err)
					} else {
						effects = append(effects, effect)
					}
				}
			}
		}
	case *sqlparser.DropTable:
		for _, table := range stmt.FromTables {
			if streamID := resolveTableName(schema, table); tables.active(streamID) {
				effects = append(effects, &dropTableEffect{
					StreamID: streamID,
					Cause:    fmt.Sprintf("table %q was dropped by query %q", streamID, query),
				})
			}
		}
	case *sqlparser.TruncateTable:
		if streamID := resolveTableName(schema, stmt.Table); tables.active(streamID) {
			// Once we have a concept of collection-level truncation we will probably
			// want to either handle this like a dropped-and-recreated table or else
			// use another mechanism to produce the appropriate "the collection is
			// now truncated" signals here. But for now ignoring is still the best
			// we can do.
			logrus.WithField("table", streamID).Warn("ignoring TRUNCATE on active table")
		}
	case *sqlparser.RenameTable:
		for _, pair := range stmt.TablePairs {
			if streamID := resolveTableName(schema, pair.FromTable); tables.active(streamID) {
				effects = append(effects, &dropTableEffect{
					StreamID: streamID,
					Cause:    fmt.Sprintf("table %q was renamed by query %q", streamID, query),
				})
			}
		}
	case *sqlparser.Insert:
		table, err := stmt.Table.TableName()
		if err != nil {
			return nil, fmt.Errorf("internal error: could no determine table name for Insert query %s: %w", query, err)
		}
		if streamID := resolveTableName(schema, table); tables.active(streamID) {
			return nil, fmt.Errorf("unsupported DML query (go.estuary.dev/IK5EVx): %s", query)
		}
	case *sqlparser.Update:
		// Determine whether any of the table expressions in the UPDATE statement refer
		// to an active table. The table expression grammar gets complicated, so if we
		// can't tell then we'll assume that the table is active and return an error.
		var possiblyActiveTables bool
		for _, table := range stmt.TableExprs {
			if tableExpr, ok := table.(*sqlparser.AliasedTableExpr); !ok {
				logrus.WithField("query", query).Warnf("unsupported table expression type %T in UPDATE statement", table)
				possiblyActiveTables = true
			} else if table, err := tableExpr.TableName(); err != nil {
				logrus.WithField("query", query).WithError(err).Warn("failed to resolve table name from UPDATE statement")
				possiblyActiveTables = true
			} else if streamID := resolveTableName(schema, table); tables.active(streamID) {
				logrus.WithField("streamID", streamID).WithField("query", query).Warn("UPDATE on active table")
				possiblyActiveTables = true
			} else {
				logrus.WithField("streamID", streamID).WithField("query", query).Debug("ignoring UPDATE on inactive table")
			}
		}

		// If any table(s) in the UPDATE statement are possibly active then it's a fatal error.
		if possiblyActiveTables {
			return nil, fmt.Errorf("unsupported DML query (go.estuary.dev/IK5EVx): %s", query)
		}
	case *sqlparser.Delete:
		for _, target := range stmt.Targets {
			if streamID := resolveTableName(schema, target); tables.active(streamID) {
				return nil, fmt.Errorf("unsupported DML query (go.estuary.dev/IK5EVx): %s", query)
			}
		}
	case *sqlparser.OtherAdmin, *sqlparser.Analyze:
		logrus.WithField("query", query).Debug("ignoring benign query")
	default:
		return nil, fmt.Errorf("unhandled query (go.estuary.dev/ceqr74): unhandled type %q: %q", reflect.TypeOf(stmt).String(), query)
	}

	return effects, nil
}

func (qa *queryAnalyzer) analyzeAlterTable(tables *activeTablesView, stmt *sqlparser.AlterTable, query string, streamID sqlcapture.StreamID) (queryEffect, error) {
	var meta, ok = tables.metadata(streamID)
	if !ok {
		return nil, fmt.Errorf("missing metadata for table %q", streamID)
	}

	for _, alterOpt := range stmt.AlterOptions {
		switch alter := alterOpt.(type) {
		// These should be all of the table alterations which might possibly impact our capture
		// in ways we don't currently support, so the default behavior can be to log and ignore.
		case *sqlparser.RenameColumn:
			var oldName = alter.OldName.Name.String()
			var newName = alter.NewName.Name.String()

			var colIndex = findColumnIndex(meta.Schema.Columns, oldName)
			if colIndex == -1 {
				return nil, fmt.Errorf("unknown column %q", oldName)
			}
			oldName = meta.Schema.Columns[colIndex] // Use the actual column name from the metadata
			meta.Schema.Columns[colIndex] = newName

			var colType = meta.Schema.ColumnTypes[oldName]
			meta.Schema.ColumnTypes[oldName] = nil
			meta.Schema.ColumnTypes[newName] = colType
			logrus.WithField("columns", meta.Schema.Columns).WithField("types", meta.Schema.ColumnTypes).Info("processed RENAME COLUMN alteration")
		case *sqlparser.RenameTableName:
			return nil, fmt.Errorf("unsupported table alteration (go.estuary.dev/eVVwet): %s", query)
		case *sqlparser.ChangeColumn:
			var oldName = alter.OldColumn.Name.String()
			var oldIndex = findColumnIndex(meta.Schema.Columns, oldName)
			if oldIndex == -1 {
				return nil, fmt.Errorf("unknown column %q", oldName)
			}
			oldName = meta.Schema.Columns[oldIndex] // Use the actual column name from the metadata
			meta.Schema.Columns = slices.Delete(meta.Schema.Columns, oldIndex, oldIndex+1)

			var newName = alter.NewColDefinition.Name.String()
			var newType = qa.translateDataType(meta, alter.NewColDefinition.Type)
			var newIndex = oldIndex
			if alter.First {
				newIndex = 0
			} else if alter.After != nil {
				var afterName = alter.After.Name.String()
				var afterIndex = findColumnIndex(meta.Schema.Columns, afterName)
				if afterIndex == -1 {
					return nil, fmt.Errorf("unknown column %q", afterName)
				}
				newIndex = afterIndex + 1
			}
			meta.Schema.Columns = slices.Insert(meta.Schema.Columns, newIndex, newName)
			meta.Schema.ColumnTypes[oldName] = nil // Set to nil rather than delete so that JSON patch merging deletes it
			meta.Schema.ColumnTypes[newName] = newType
			logrus.WithField("columns", meta.Schema.Columns).WithField("types", meta.Schema.ColumnTypes).Info("processed CHANGE COLUMN alteration")
		case *sqlparser.ModifyColumn:
			var colName = alter.NewColDefinition.Name.String()
			var oldIndex = findColumnIndex(meta.Schema.Columns, colName)
			if oldIndex == -1 {
				return nil, fmt.Errorf("unknown column %q", colName)
			}
			colName = meta.Schema.Columns[oldIndex] // Use the actual column name from the metadata
			meta.Schema.Columns = slices.Delete(meta.Schema.Columns, oldIndex, oldIndex+1)

			var newType = qa.translateDataType(meta, alter.NewColDefinition.Type)
			var newIndex = oldIndex
			if alter.First {
				newIndex = 0
			} else if alter.After != nil {
				var afterName = alter.After.Name.String()
				var afterIndex = findColumnIndex(meta.Schema.Columns, afterName)
				if afterIndex == -1 {
					return nil, fmt.Errorf("unknown column %q", afterName)
				}
				newIndex = afterIndex + 1
			}

			meta.Schema.Columns = slices.Insert(meta.Schema.Columns, newIndex, colName)
			meta.Schema.ColumnTypes[colName] = newType
			logrus.WithField("columns", meta.Schema.Columns).WithField("types", meta.Schema.ColumnTypes).Info("processed MODIFY COLUMN alteration")
		case *sqlparser.AddColumns:
			var insertAt = len(meta.Schema.Columns)
			if alter.First {
				insertAt = 0
			} else if after := alter.After; after != nil {
				var afterIndex = findColumnIndex(meta.Schema.Columns, after.Name.String())
				if afterIndex == -1 {
					return nil, fmt.Errorf("unknown column %q", after.Name.String())
				}
				insertAt = afterIndex + 1
			}

			var newCols []string
			for _, col := range alter.Columns {
				newCols = append(newCols, col.Name.String())
				var dataType = qa.translateDataType(meta, col.Type)
				meta.Schema.ColumnTypes[col.Name.String()] = dataType
			}

			meta.Schema.Columns = slices.Insert(meta.Schema.Columns, insertAt, newCols...)
			logrus.WithField("columns", meta.Schema.Columns).WithField("types", meta.Schema.ColumnTypes).Info("processed CHANGE COLUMN alteration")
		case *sqlparser.DropColumn:
			var colName = alter.Name.Name.String()
			var oldIndex = findColumnIndex(meta.Schema.Columns, colName)
			if oldIndex == -1 {
				return nil, fmt.Errorf("unknown column %q", colName)
			}
			colName = meta.Schema.Columns[oldIndex] // Use the actual column name from the metadata
			meta.Schema.Columns = slices.Delete(meta.Schema.Columns, oldIndex, oldIndex+1)
			meta.Schema.ColumnTypes[colName] = nil // Set to nil rather than delete so that JSON patch merging deletes it
			logrus.WithField("columns", meta.Schema.Columns).WithField("types", meta.Schema.ColumnTypes).Info("processed CHANGE COLUMN alteration")
		default:
			logrus.WithField("query", query).Info("ignorable table alteration")
		}
	}

	return &updateMetadataEffect{
		StreamID: streamID,
		Metadata: meta,
	}, nil
}

// findColumnIndex performs a case-insensitive search for a column name in a slice of column names.
// It returns the index of the first matching column, or -1 if no match is found.
//
// According to https://dev.mysql.com/doc/refman/8.4/en/identifier-case-sensitivity.html:
// > [...] column [...] names are not case-sensitive on any platform, nor are column aliases.
func findColumnIndex(columns []string, name string) int {
	for i, col := range columns {
		if strings.EqualFold(col, name) {
			return i
		}
	}
	return -1
}

func (qa *queryAnalyzer) translateDataType(meta *mysqlTableMetadata, t *sqlparser.ColumnType) any {
	switch typeName := strings.ToLower(t.Type); typeName {
	case "enum":
		return &mysqlColumnType{Type: typeName, EnumValues: append([]string{""}, unquoteEnumValues(t.EnumValues)...)}
	case "set":
		return &mysqlColumnType{Type: typeName, EnumValues: unquoteEnumValues(t.EnumValues)}
	case "boolean", "bool":
		// MySQL's BOOLEAN/BOOL is an alias for TINYINT(1). Since we're parsing the actual
		// DDL query here, we get the raw keyword, unlike in discovery where we receive the
		// resolved `tinyint` type. So we need to handle that mapping ourselves.
		if qa.featureFlags["tinyint1_as_bool"] {
			return &mysqlColumnType{Type: "boolean"}
		}
		return &mysqlColumnType{Type: "tinyint"}
	case "tinyint", "smallint", "mediumint", "int", "bigint":
		return &mysqlColumnType{Type: typeName, Unsigned: t.Unsigned}
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		return &mysqlColumnType{Type: typeName, Charset: columnCharset(meta, t)}
	case "json":
		// MySQL has a real JSON type, but in MariaDB the JSON type is an alias for
		// `LONGTEXT COLLATE utf8mb4_bin`, and discovery (which reads information_schema)
		// reports it as a longtext with charset utf8mb4. Thus we have to mirror that here
		// so a JSON column added via live DDL is captured as a string, consistent with the
		// discovered schema.
		if qa.isMariaDB {
			return &mysqlColumnType{Type: "longtext", Charset: "utf8mb4"}
		}
		return typeName
	case "binary":
		var columnLength int
		if t.Length == nil {
			columnLength = 1 // A type of just 'BINARY' is allowed and is a synonym for 'BINARY(1)'
		} else {
			columnLength = *t.Length
		}
		return &mysqlColumnType{Type: typeName, MaxLength: columnLength}
	default:
		return typeName
	}
}

// columnCharset resolves the character set of a text column declared in a DDL statement,
// preferring an explicit column charset, then the charset implied by an explicit collation,
// then the table default, and finally falling back to UTF-8.
func columnCharset(meta *mysqlTableMetadata, t *sqlparser.ColumnType) string {
	if t.Charset.Name != "" {
		return t.Charset.Name
	} else if t.Options.Collate != "" {
		return charsetFromCollation(t.Options.Collate)
	} else if meta.DefaultCharset != "" {
		return meta.DefaultCharset
	}
	return mysqlDefaultCharset
}

func resolveTableName(defaultSchema string, name sqlparser.TableName) sqlcapture.StreamID {
	var schema, table = name.Qualifier.String(), name.Name.String()
	if schema == "" {
		schema = defaultSchema
	}
	return sqlcapture.JoinStreamID(schema, table)
}

// activeTablesView is a read-only view of the table metadata for
// all active tables, used as an input to query event processing.
type activeTablesView struct {
	// Lazy loading closure so we can avoid constructing the tables map
	// in cases where the active tables view is unnecessary.
	load func() map[sqlcapture.StreamID]*mysqlTableMetadata

	// Cached tables map after the first load.
	cache map[sqlcapture.StreamID]*mysqlTableMetadata
}

func (t *activeTablesView) tables() map[sqlcapture.StreamID]*mysqlTableMetadata {
	if t.cache == nil {
		t.cache = t.load()
	}
	return t.cache
}

func (t *activeTablesView) active(streamID sqlcapture.StreamID) bool {
	_, ok := t.tables()[streamID]
	return ok
}

func (t *activeTablesView) inSchema(schema string) []sqlcapture.StreamID {
	var out []sqlcapture.StreamID
	for streamID := range t.tables() {
		if strings.EqualFold(streamID.Schema, schema) {
			out = append(out, streamID)
		}
	}
	return out
}

func (t *activeTablesView) metadata(streamID sqlcapture.StreamID) (*mysqlTableMetadata, bool) {
	var meta, ok = t.tables()[streamID]
	if !ok || meta == nil {
		return nil, false
	}
	var cloned = meta.Clone()
	return &cloned, true
}

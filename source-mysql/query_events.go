package main

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver" // Registers the parser driver which decodes literal values (DEFAULT expressions and the like).
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/segmentio/encoding/json"
	"github.com/sirupsen/logrus"
)

// Query Events in the MySQL binlog are normalized enough that we can use
// prefix matching to detect many types of query that we just completely
// don't care about.
//
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
	parser       *parser.Parser
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

	var stmt, err = qa.parser.ParseOneStmt(query, "", "")
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
	case *ast.CreateDatabaseStmt, *ast.AlterDatabaseStmt, *ast.CreateTableStmt, *ast.SavepointStmt, *ast.FlushStmt:
		logrus.WithField("query", query).Debug("ignoring benign query")
	case *ast.CreateIndexStmt, *ast.DropIndexStmt:
		// Index changes don't affect the columns we track, so they're benign. The Vitess
		// parser modeled CREATE INDEX as an ALTER TABLE, but TiDB has dedicated statement
		// types for them which we ignore directly.
		logrus.WithField("query", query).Debug("ignoring benign query")
	case *ast.CreateViewStmt:
		// All view creation/deletion/alterations should be fine to ignore since we don't capture from views.
		logrus.WithField("query", query).Debug("ignoring benign query")
	case *ast.DropDatabaseStmt:
		// Remember that In MySQL land "database" is a synonym for the usual SQL concept "schema"
		if streamIDs := tables.inSchema(stmt.Name.O); len(streamIDs) > 0 {
			logrus.WithFields(logrus.Fields{
				"query":     query,
				"schema":    stmt.Name.O,
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
	case *ast.AlterTableStmt:
		if streamID := resolveTableName(schema, stmt.Table); tables.active(streamID) {
			logrus.WithFields(logrus.Fields{
				"query": query,
				"specs": len(stmt.Specs),
			}).Info("parsed components of ALTER TABLE statement")

			if effect, err := qa.analyzeAlterTable(tables, stmt, query, streamID); err != nil {
				return nil, fmt.Errorf("cannot handle table alteration %q: %w", query, err)
			} else if effect != nil {
				effects = append(effects, effect)
			}
		}
	case *ast.DropTableStmt:
		// A `DROP VIEW` parses into a DropTableStmt with IsView set, and is benign since we don't capture views.
		if !stmt.IsView {
			for _, table := range stmt.Tables {
				if streamID := resolveTableName(schema, table); tables.active(streamID) {
					effects = append(effects, &dropTableEffect{
						StreamID: streamID,
						Cause:    fmt.Sprintf("table %q was dropped by query %q", streamID, query),
					})
				}
			}
		}
	case *ast.TruncateTableStmt:
		if streamID := resolveTableName(schema, stmt.Table); tables.active(streamID) {
			// Once we have a concept of collection-level truncation we will probably
			// want to either handle this like a dropped-and-recreated table or else
			// use another mechanism to produce the appropriate "the collection is
			// now truncated" signals here. But for now ignoring is still the best
			// we can do.
			logrus.WithField("table", streamID).Warn("ignoring TRUNCATE on active table")
		}
	case *ast.RenameTableStmt:
		for _, pair := range stmt.TableToTables {
			if streamID := resolveTableName(schema, pair.OldTable); tables.active(streamID) {
				effects = append(effects, &dropTableEffect{
					StreamID: streamID,
					Cause:    fmt.Sprintf("table %q was renamed by query %q", streamID, query),
				})
			}
		}
	case *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt:
		// We should never observe DML in the binlog when `binlog_format` is set to ROW as
		// required for CDC. If we do, and it touches a table we're capturing, that's a fatal
		// error. We collect every table referenced anywhere in the statement (including joins
		// and subqueries) and error if any of them is active.
		for _, table := range referencedTables(stmt) {
			if streamID := resolveTableName(schema, table); tables.active(streamID) {
				return nil, fmt.Errorf("unsupported DML query (go.estuary.dev/IK5EVx): %s", query)
			}
		}
	case *ast.AnalyzeTableStmt, *ast.OptimizeTableStmt, *ast.SelectStmt, *ast.SetOprStmt, *ast.SetStmt:
		logrus.WithField("query", query).Debug("ignoring benign query")
	default:
		return nil, fmt.Errorf("unhandled query (go.estuary.dev/ceqr74): unhandled type %q: %q", reflect.TypeOf(stmt).String(), query)
	}

	return effects, nil
}

// referencedTables returns every table name referenced anywhere within a statement,
// by walking the AST and collecting all TableName nodes. It's used by the DML guard,
// where we want to fail if a query touches any active table regardless of where in
// the statement (target, join, or subquery) that reference appears.
func referencedTables(stmt ast.Node) []*ast.TableName {
	var c tableNameCollector
	stmt.Accept(&c)
	return c.names
}

type tableNameCollector struct {
	names []*ast.TableName
}

func (c *tableNameCollector) Enter(n ast.Node) (ast.Node, bool) {
	if tn, ok := n.(*ast.TableName); ok {
		c.names = append(c.names, tn)
	}
	return n, false
}

func (c *tableNameCollector) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}

func (qa *queryAnalyzer) analyzeAlterTable(tables *activeTablesView, stmt *ast.AlterTableStmt, query string, streamID sqlcapture.StreamID) (queryEffect, error) {
	var meta, ok = tables.metadata(streamID)
	if !ok {
		return nil, fmt.Errorf("missing metadata for table %q", streamID)
	}

	for _, spec := range stmt.Specs {
		switch spec.Tp {
		// These should be all of the table alterations which might possibly impact our capture
		// in ways we don't currently support, so the default behavior can be to log and ignore.
		case ast.AlterTableRenameColumn:
			var oldName = spec.OldColumnName.Name.String()
			var newName = spec.NewColumnName.Name.String()

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
		case ast.AlterTableRenameTable:
			return nil, fmt.Errorf("unsupported table alteration (go.estuary.dev/eVVwet): %s", query)
		case ast.AlterTableChangeColumn:
			var oldName = spec.OldColumnName.Name.String()
			var oldIndex = findColumnIndex(meta.Schema.Columns, oldName)
			if oldIndex == -1 {
				return nil, fmt.Errorf("unknown column %q", oldName)
			}
			oldName = meta.Schema.Columns[oldIndex] // Use the actual column name from the metadata
			meta.Schema.Columns = slices.Delete(meta.Schema.Columns, oldIndex, oldIndex+1)

			var newCol = spec.NewColumns[0]
			var newName = newCol.Name.Name.String()
			var newType = qa.translateDataType(meta, newCol)
			var newIndex, err = columnPositionIndex(meta.Schema.Columns, spec.Position, oldIndex)
			if err != nil {
				return nil, err
			}
			meta.Schema.Columns = slices.Insert(meta.Schema.Columns, newIndex, newName)
			meta.Schema.ColumnTypes[oldName] = nil // Set to nil rather than delete so that JSON patch merging deletes it
			meta.Schema.ColumnTypes[newName] = newType
			logrus.WithField("columns", meta.Schema.Columns).WithField("types", meta.Schema.ColumnTypes).Info("processed CHANGE COLUMN alteration")
		case ast.AlterTableModifyColumn:
			var newCol = spec.NewColumns[0]
			var colName = newCol.Name.Name.String()
			var oldIndex = findColumnIndex(meta.Schema.Columns, colName)
			if oldIndex == -1 {
				return nil, fmt.Errorf("unknown column %q", colName)
			}
			colName = meta.Schema.Columns[oldIndex] // Use the actual column name from the metadata
			meta.Schema.Columns = slices.Delete(meta.Schema.Columns, oldIndex, oldIndex+1)

			var newType = qa.translateDataType(meta, newCol)
			var newIndex, err = columnPositionIndex(meta.Schema.Columns, spec.Position, oldIndex)
			if err != nil {
				return nil, err
			}
			meta.Schema.Columns = slices.Insert(meta.Schema.Columns, newIndex, colName)
			meta.Schema.ColumnTypes[colName] = newType
			logrus.WithField("columns", meta.Schema.Columns).WithField("types", meta.Schema.ColumnTypes).Info("processed MODIFY COLUMN alteration")
		case ast.AlterTableAddColumns:
			var insertAt, err = columnPositionIndex(meta.Schema.Columns, spec.Position, len(meta.Schema.Columns))
			if err != nil {
				return nil, err
			}

			var newCols []string
			for _, col := range spec.NewColumns {
				var colName = col.Name.Name.String()
				newCols = append(newCols, colName)
				meta.Schema.ColumnTypes[colName] = qa.translateDataType(meta, col)
			}

			meta.Schema.Columns = slices.Insert(meta.Schema.Columns, insertAt, newCols...)
			logrus.WithField("columns", meta.Schema.Columns).WithField("types", meta.Schema.ColumnTypes).Info("processed ADD COLUMN alteration")
		case ast.AlterTableDropColumn:
			var colName = spec.OldColumnName.Name.String()
			var oldIndex = findColumnIndex(meta.Schema.Columns, colName)
			if oldIndex == -1 {
				return nil, fmt.Errorf("unknown column %q", colName)
			}
			colName = meta.Schema.Columns[oldIndex] // Use the actual column name from the metadata
			meta.Schema.Columns = slices.Delete(meta.Schema.Columns, oldIndex, oldIndex+1)
			meta.Schema.ColumnTypes[colName] = nil // Set to nil rather than delete so that JSON patch merging deletes it
			logrus.WithField("columns", meta.Schema.Columns).WithField("types", meta.Schema.ColumnTypes).Info("processed DROP COLUMN alteration")
		default:
			logrus.WithField("query", query).Info("ignorable table alteration")
		}
	}

	return &updateMetadataEffect{
		StreamID: streamID,
		Metadata: meta,
	}, nil
}

// columnPositionIndex translates an optional column position clause (FIRST or AFTER x)
// into the index at which a column should be inserted. The defaultIndex is used when
// the position is unspecified (or absent), which corresponds to "leave it where it is"
// for CHANGE/MODIFY and "append to the end" for ADD.
func columnPositionIndex(columns []string, pos *ast.ColumnPosition, defaultIndex int) (int, error) {
	if pos == nil {
		return defaultIndex, nil
	}
	switch pos.Tp {
	case ast.ColumnPositionFirst:
		return 0, nil
	case ast.ColumnPositionAfter:
		var afterName = pos.RelativeColumn.Name.String()
		var afterIndex = findColumnIndex(columns, afterName)
		if afterIndex == -1 {
			return 0, fmt.Errorf("unknown column %q", afterName)
		}
		return afterIndex + 1, nil
	default:
		return defaultIndex, nil
	}
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

func (qa *queryAnalyzer) translateDataType(meta *mysqlTableMetadata, col *ast.ColumnDef) any {
	var ft = col.Tp
	// TypeToStr maps the parser's internal type code plus charset back to the canonical
	// MySQL type name (for example "int", "varchar", "longtext", "binary"), matching the
	// DATA_TYPE strings that discovery reads from information_schema.
	var typeName = types.TypeToStr(ft.GetType(), ft.GetCharset())
	switch typeName {
	case "enum":
		// Illegal values are represented internally by MySQL as the integer 0. Prepending
		// an empty string as the zero-th element allows everything else to flow naturally,
		// mirroring how enum columns are handled during discovery.
		return &mysqlColumnType{Type: typeName, EnumValues: append([]string{""}, ft.GetElems()...)}
	case "set":
		return &mysqlColumnType{Type: typeName, EnumValues: ft.GetElems()}
	case "tinyint", "smallint", "mediumint", "int", "bigint":
		// MySQL's BOOLEAN/BOOL is an alias for TINYINT(1), which the parser normalizes to a
		// tinyint with a display width of 1. As in discovery, we assume nobody uses TINYINT(1)
		// for other purposes, so when the feature flag is set we treat it as a boolean.
		if typeName == "tinyint" && ft.GetFlen() == 1 && qa.featureFlags["tinyint1_as_bool"] {
			return &mysqlColumnType{Type: "boolean"}
		}
		return &mysqlColumnType{Type: typeName, Unsigned: mysql.HasUnsignedFlag(ft.GetFlag())}
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		return &mysqlColumnType{Type: typeName, Charset: columnCharset(meta, col)}
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
		var columnLength = ft.GetFlen()
		if columnLength <= 0 {
			columnLength = 1 // A type of just 'BINARY' is allowed and is a synonym for 'BINARY(1)'
		}
		return &mysqlColumnType{Type: typeName, MaxLength: columnLength}
	default:
		return typeName
	}
}

// columnCharset resolves the character set of a text column declared in a DDL statement,
// preferring an explicit column charset, then the charset implied by an explicit collation,
// then the table default, and finally falling back to UTF-8.
func columnCharset(meta *mysqlTableMetadata, col *ast.ColumnDef) string {
	if charset := col.Tp.GetCharset(); charset != "" {
		return charset
	} else if collate := columnCollation(col); collate != "" {
		return charsetFromCollation(collate)
	} else if meta.DefaultCharset != "" {
		return meta.DefaultCharset
	}
	return mysqlDefaultCharset
}

// columnCollation returns the collation named by an explicit COLLATE clause on a column
// definition, or the empty string if none is present. The parser carries the collation
// as a column option rather than folding it into the field type.
func columnCollation(col *ast.ColumnDef) string {
	for _, opt := range col.Options {
		if opt.Tp == ast.ColumnOptionCollate {
			return opt.StrValue
		}
	}
	return ""
}

func resolveTableName(defaultSchema string, name *ast.TableName) sqlcapture.StreamID {
	var schema, table = name.Schema.O, name.Name.O
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

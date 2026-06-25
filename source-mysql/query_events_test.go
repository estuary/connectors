package main

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/stretchr/testify/require"
)

func TestIgnoreQueries(t *testing.T) {
	var cases = map[string]bool{
		`BEGIN`:                     true,
		`COMMIT`:                    true,
		`CREATE DEFINER`:            true,
		`CREATE OR REPLACE DEFINER`: true,
		`CREATE OR REPLACE ALGORITHM=UNDEFINED DEFINER`: true,
		`CREATE ALGORITHM = TEMPTABLE DEFINER`:          true,

		`CREATE USER IF NOT EXISTS flow_capture IDENTIFIED BY 'secret1234'`: true,

		"# This is a comment\n ALTER TABLE foobar ADD COLUMN x INTEGER;":        false,
		`/* This is also a comment */ ALTER TABLE foobar ADD COLUMN x INTEGER;`: false,

		`CREATE DATABASE IF NOT EXISTS test`:     false,
		`INSERT INTO foobar VALUES (1, 'hello')`: false,
		`DROP TABLE foobar`:                      false,
	}
	for input, expect := range cases {
		if ignoreQueriesRe.MatchString(input) != expect {
			t.Errorf("ignore result mismath for %q (expected %v)", input, expect)
		}
	}
}

// testDefaultSchema is the schema applied to unqualified table names when a
// test case doesn't specify one of its own.
const testDefaultSchema = "test"

// testActiveTables is the default fixture of "assumed to exist" active tables.
// Test cases which don't set their own `tables` are analyzed against this set,
// so most cases can align their (anonymized) queries with these names rather
// than redeclaring a fixture each time. It returns a fresh map per call so
// cases can't interfere with one another.
func testActiveTables() map[sqlcapture.StreamID]*mysqlTableMetadata {
	return map[sqlcapture.StreamID]*mysqlTableMetadata{
		sqlcapture.JoinStreamID(testDefaultSchema, "users"): {
			Schema: mysqlTableSchema{
				Columns: []string{"id", "name", "email"},
				ColumnTypes: map[string]any{
					"id":    &mysqlColumnType{Type: "int"},
					"name":  &mysqlColumnType{Type: "varchar", Charset: "utf8mb4"},
					"email": &mysqlColumnType{Type: "varchar", Charset: "utf8mb4"},
				},
			},
			DefaultCharset: "utf8mb4",
		},
		sqlcapture.JoinStreamID(testDefaultSchema, "orders"): {
			Schema: mysqlTableSchema{
				Columns: []string{"id", "total"},
				ColumnTypes: map[string]any{
					"id":    &mysqlColumnType{Type: "bigint"},
					"total": &mysqlColumnType{Type: "int"},
				},
			},
			DefaultCharset: "utf8mb4",
		},
		// A table in a second schema with a non-default charset, so we can
		// exercise schema-qualified name resolution, schema-scoped drops, and
		// the table-default charset branch of column type translation.
		sqlcapture.JoinStreamID("archive", "events"): {
			Schema: mysqlTableSchema{
				Columns: []string{"id", "payload"},
				ColumnTypes: map[string]any{
					"id":      &mysqlColumnType{Type: "int"},
					"payload": &mysqlColumnType{Type: "varchar", Charset: "latin1"},
				},
			},
			DefaultCharset: "latin1",
		},
	}
}

// formatEffects renders a list of query effects into a compact, line-oriented
// string for comparison against a test case's expected output. It deliberately
// renders only the semantically meaningful parts of each effect: dropTableEffect
// reduces to its stream ID (the human-facing Cause text is omitted, since pinning
// its exact wording would be churn), while updateMetadataEffect renders its full
// resulting table metadata via formatMetadata.
//
// Lines are sorted so the comparison is stable: the effects of a single query
// form an unordered set, and the only queries which produce multiple effects are
// multi-table drops (DROP DATABASE / DROP TABLE / RENAME TABLE) whose relative
// order is irrelevant and, in the DROP DATABASE case, derives from nondeterministic
// map iteration.
func formatEffects(effects []queryEffect) string {
	var lines []string
	for _, effect := range effects {
		switch effect := effect.(type) {
		case *dropTableEffect:
			lines = append(lines, fmt.Sprintf("DropTable(%q)", effect.StreamID))
		case *updateMetadataEffect:
			lines = append(lines, fmt.Sprintf("UpdateMetadata(%q, %s)", effect.StreamID, formatMetadata(effect.Metadata)))
		default:
			lines = append(lines, fmt.Sprintf("UnknownEffect(%#v)", effect))
		}
	}
	slices.Sort(lines)
	return strings.Join(lines, "; ")
}

// formatMetadata renders table metadata as an ordered list of "name=type" pairs.
// Columns are listed in their schema order, and any tombstoned column types (a
// nil entry in ColumnTypes which is no longer present in Columns, as left behind
// by a dropped or renamed column) are appended in sorted order as "name=DELETED"
// so that deletions remain visible in the comparison.
func formatMetadata(meta *mysqlTableMetadata) string {
	var parts []string
	for _, name := range meta.Schema.Columns {
		parts = append(parts, fmt.Sprintf("%s=%s", name, formatColumnType(meta.Schema.ColumnTypes[name])))
	}

	var tombstones []string
	for name, typ := range meta.Schema.ColumnTypes {
		if typ == nil && !slices.Contains(meta.Schema.Columns, name) {
			tombstones = append(tombstones, name)
		}
	}
	slices.Sort(tombstones)
	for _, name := range tombstones {
		parts = append(parts, fmt.Sprintf("%s=<deleted>", name))
	}

	return strings.Join(parts, ", ")
}

// formatColumnType renders a single column type value. Column types are stored
// as `any` and may be a *mysqlColumnType, a plain type-name string, or nil (a
// JSON-patch tombstone). Unlike mysqlColumnType.String(), this always renders
// the charset when one is set, so a column with an explicit default charset is
// distinguishable from one with no charset at all.
func formatColumnType(typ any) string {
	switch typ := typ.(type) {
	case nil:
		return "<deleted>"
	case string:
		return typ
	case *mysqlColumnType:
		var s = typ.Type
		if typ.Unsigned {
			s += " unsigned"
		}
		if typ.Charset != "" {
			s += fmt.Sprintf("(charset=%s)", typ.Charset)
		}
		if typ.MaxLength != 0 {
			s += fmt.Sprintf("(maxLen=%d)", typ.MaxLength)
		}
		if len(typ.EnumValues) > 0 {
			s += "{" + strings.Join(typ.EnumValues, "|") + "}"
		}
		return s
	default:
		return fmt.Sprintf("%#v", typ)
	}
}

// subtestNameSanitizeRe matches runs of characters which shouldn't appear in a
// subtest name, so they can be collapsed into single underscores.
var subtestNameSanitizeRe = regexp.MustCompile(`[^A-Za-z0-9]+`)

// subtestName builds a stable, readable subtest name from a case's index and
// query. The index keeps names unique and ordered even when queries are long
// or nearly identical, while a sanitized, length-capped snippet of the query
// keeps failures legible without dragging an entire multi-line DDL statement
// into the test name (or introducing subtest-path separators from a stray '/').
func subtestName(index int, query string) string {
	var snippet = strings.Trim(subtestNameSanitizeRe.ReplaceAllString(query, "_"), "_")
	if len(snippet) > 48 {
		snippet = strings.TrimRight(snippet[:48], "_")
	}
	return fmt.Sprintf("%04d_%s", index, snippet)
}

func TestAnalyzeQuery(t *testing.T) {
	var sqlParser = parser.New()

	var cases = []struct {
		tables  map[sqlcapture.StreamID]*mysqlTableMetadata // Active tables fixture, defaults to testActiveTables() when nil.
		schema  string                                      // Default schema for unqualified table names, defaults to testDefaultSchema when empty.
		flags   map[string]bool                             // Feature flags for the analyzer, defaults to none.
		mariadb bool                                        // Whether the analyzer should treat the server as MariaDB.
		query   string
		want    string // Expected effects, as rendered by formatEffects.
		wantErr string // Substring match against the error, empty means no error is expected.
	}{
		// Transaction control and other events which are silently ignored before parsing.
		{query: "BEGIN"},
		{query: "COMMIT"},
		{query: "ROLLBACK"},
		{query: "SAVEPOINT sp1"},
		{query: "# a freeform comment line"},

		// Queries dropped by the ignore-list regex before parsing.
		{query: "GRANT SELECT ON *.* TO 'flow_capture'@'%'"},
		{query: "CREATE USER 'someone'@'%' IDENTIFIED BY 'hunter2'"},
		{query: "DROP USER 'someone'@'%'"},

		// Statements which the parser can't handle at all are ignored.
		{query: "LOREM IPSUM DOLOR SIT AMET"},

		// Benign DDL which doesn't affect any captured table.
		{query: "CREATE DATABASE analytics"},
		{query: "ALTER DATABASE test CHARACTER SET = utf8mb4"},
		{query: "CREATE TABLE test.widgets (id INT PRIMARY KEY, label VARCHAR(50))"},
		{query: "CREATE VIEW test.user_names AS SELECT id, name FROM users"},
		{query: "ALTER VIEW test.user_names AS SELECT id FROM users"},
		{query: "DROP VIEW test.user_names"},

		// DROP DATABASE drops every active table in the affected schema.
		{
			query: "DROP DATABASE test",
			want:  `DropTable("test.orders"); DropTable("test.users")`,
		},
		{
			query: "DROP DATABASE archive",
			want:  `DropTable("archive.events")`,
		},
		{query: "DROP DATABASE nonexistent"},

		// DROP TABLE drops each named active table and ignores inactive ones.
		{
			query: "DROP TABLE users",
			want:  `DropTable("test.users")`,
		},
		{
			query: "DROP TABLE users, orders",
			want:  `DropTable("test.orders"); DropTable("test.users")`,
		},
		{
			query: "DROP TABLE test.users",
			want:  `DropTable("test.users")`,
		},
		{query: "DROP TABLE nonexistent"},
		{
			query: "DROP TABLE users, nonexistent",
			want:  `DropTable("test.users")`,
		},

		// TRUNCATE TABLE is a no-op even against an active table.
		{query: "TRUNCATE TABLE users"},
		{query: "TRUNCATE TABLE nonexistent"},

		// RENAME TABLE is treated as a drop of the original table.
		{
			query: "RENAME TABLE users TO users_archived",
			want:  `DropTable("test.users")`,
		},
		{
			query: "RENAME TABLE users TO a, orders TO b",
			want:  `DropTable("test.orders"); DropTable("test.users")`,
		},
		{query: "RENAME TABLE nonexistent TO whatever"},

		// DML against an active table is a fatal error; against an inactive table it's ignored.
		{
			query:   "INSERT INTO users (id, name) VALUES (1, 'alice')",
			wantErr: "unsupported DML query",
		},
		{query: "INSERT INTO nonexistent VALUES (1)"},
		{
			query:   "UPDATE users SET name = 'bob' WHERE id = 1",
			wantErr: "unsupported DML query",
		},
		{query: "UPDATE nonexistent SET x = 1"},
		{
			query:   "DELETE FROM users WHERE id = 1",
			wantErr: "unsupported DML query",
		},
		{query: "DELETE FROM nonexistent WHERE id = 1"},
		{
			query:   "UPDATE users u JOIN orders o ON u.id = o.id SET u.name = 'x'",
			wantErr: "unsupported DML query",
		},

		// ALTER TABLE against an inactive table is ignored.
		{query: "ALTER TABLE nonexistent ADD COLUMN x INT"},

		// ALTER TABLE ADD COLUMN, including positioning and multiple additions.
		{
			query: "ALTER TABLE users ADD COLUMN age INT",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), age=int)`,
		},
		{
			query: "ALTER TABLE users ADD COLUMN created_at DATETIME FIRST",
			want:  `UpdateMetadata("test.users", created_at=datetime, id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4))`,
		},
		{
			query: "ALTER TABLE users ADD COLUMN nickname VARCHAR(50) AFTER name",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), nickname=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4))`,
		},
		{
			query: "ALTER TABLE users ADD COLUMN a INT, ADD COLUMN b INT",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), a=int, b=int)`,
		},
		{
			query:   "ALTER TABLE users ADD COLUMN x INT AFTER nonexistent",
			wantErr: `unknown column "nonexistent"`,
		},

		// ADD COLUMN type translation variety.
		{
			query: "ALTER TABLE users ADD COLUMN is_active BOOLEAN",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), is_active=tinyint)`,
		},
		{
			query: "ALTER TABLE users ADD COLUMN is_active BOOLEAN",
			flags: map[string]bool{"tinyint1_as_bool": true},
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), is_active=boolean)`,
		},
		// The leading empty value in the enum reflects the "" element we prepend for the
		// zero/invalid enum index, mirroring how enum columns are handled during discovery.
		{
			query: "ALTER TABLE users ADD COLUMN status ENUM('active', 'inactive', 'pending')",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), status=enum{|active|inactive|pending})`,
		},
		{
			query: "ALTER TABLE users ADD COLUMN perms SET('read', 'write')",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), perms=set{read|write})`,
		},
		{
			query: "ALTER TABLE users ADD COLUMN metadata JSON",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), metadata=json)`,
		},
		{
			query:   "ALTER TABLE users ADD COLUMN metadata JSON",
			mariadb: true,
			want:    `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), metadata=longtext(charset=utf8mb4))`,
		},
		{
			query: "ALTER TABLE users ADD COLUMN counter INT UNSIGNED",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), counter=int unsigned)`,
		},
		{
			query: "ALTER TABLE users ADD COLUMN data BINARY(16)",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), data=binary(maxLen=16))`,
		},
		{
			query: "ALTER TABLE users ADD COLUMN flag BINARY",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), flag=binary(maxLen=1))`,
		},
		{
			query: "ALTER TABLE users ADD COLUMN bio TEXT CHARACTER SET latin1",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), bio=text(charset=latin1))`,
		},
		{
			query: "ALTER TABLE users ADD COLUMN note TEXT COLLATE latin1_swedish_ci",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), note=text(charset=latin1))`,
		},
		{
			query: "ALTER TABLE archive.events ADD COLUMN label TEXT",
			want:  `UpdateMetadata("archive.events", id=int, payload=varchar(charset=latin1), label=text(charset=latin1))`,
		},

		// DROP COLUMN.
		{
			query: "ALTER TABLE users DROP COLUMN email",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=<deleted>)`,
		},
		{
			query:   "ALTER TABLE users DROP COLUMN nonexistent",
			wantErr: `unknown column "nonexistent"`,
		},
		// DROP COLUMN IF EXISTS is a no-op when the column is already absent.
		{
			query: "ALTER TABLE users DROP COLUMN IF EXISTS nonexistent",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4))`,
		},

		// RENAME COLUMN.
		{
			query: "ALTER TABLE users RENAME COLUMN name TO full_name",
			want:  `UpdateMetadata("test.users", id=int, full_name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), name=<deleted>)`,
		},
		{
			query:   "ALTER TABLE users RENAME COLUMN nonexistent TO x",
			wantErr: `unknown column "nonexistent"`,
		},

		// CHANGE COLUMN (rename and retype, with optional repositioning).
		{
			query: "ALTER TABLE users CHANGE COLUMN name full_name VARCHAR(255)",
			want:  `UpdateMetadata("test.users", id=int, full_name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), name=<deleted>)`,
		},
		{
			query: "ALTER TABLE users CHANGE COLUMN email contact VARCHAR(100) FIRST",
			want:  `UpdateMetadata("test.users", contact=varchar(charset=utf8mb4), id=int, name=varchar(charset=utf8mb4), email=<deleted>)`,
		},
		{
			query: "ALTER TABLE users CHANGE COLUMN email contact VARCHAR(100) AFTER id",
			want:  `UpdateMetadata("test.users", id=int, contact=varchar(charset=utf8mb4), name=varchar(charset=utf8mb4), email=<deleted>)`,
		},
		{
			query:   "ALTER TABLE users CHANGE COLUMN nonexistent x INT",
			wantErr: `unknown column "nonexistent"`,
		},
		// CHANGE COLUMN IF EXISTS is a no-op when the column is already absent.
		{
			query: "ALTER TABLE users CHANGE COLUMN IF EXISTS nonexistent x INT",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4))`,
		},

		// MODIFY COLUMN (retype/reposition, keeping the same name). Note that a pure type
		// change which our metadata doesn't track (such as a VARCHAR length) produces an
		// unchanged-looking result, since charset is the only text-column attribute we model.
		{
			query: "ALTER TABLE users MODIFY COLUMN name VARCHAR(500)",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4))`,
		},
		{
			query: "ALTER TABLE users MODIFY COLUMN email VARCHAR(100) FIRST",
			want:  `UpdateMetadata("test.users", email=varchar(charset=utf8mb4), id=int, name=varchar(charset=utf8mb4))`,
		},
		{
			query:   "ALTER TABLE users MODIFY COLUMN nonexistent INT",
			wantErr: `unknown column "nonexistent"`,
		},
		// MODIFY COLUMN IF EXISTS is a no-op when the column is already absent.
		{
			query: "ALTER TABLE users MODIFY COLUMN IF EXISTS nonexistent INT",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4))`,
		},

		// ALTER TABLE ... RENAME TO is explicitly unsupported.
		{
			query:   "ALTER TABLE users RENAME TO users_v2",
			wantErr: "unsupported table alteration",
		},

		// Table alterations we don't model are ignored, but the analyzer still emits a
		// metadata update carrying the unchanged metadata.
		{
			query: "ALTER TABLE users ENGINE=InnoDB",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4))`,
		},
		{
			query: "ALTER TABLE users ADD INDEX idx_name (name)",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4))`,
		},
		// TODO(wgd): CONVERT TO CHARACTER SET rewrites the charset of every text column, but
		// we don't model it, so the emitted metadata keeps the stale charsets.
		{
			query: "ALTER TABLE users CONVERT TO CHARACTER SET utf8mb4",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4))`,
		},
		{
			query: "ALTER TABLE users ADD FULLTEXT INDEX ft_name (name)",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4))`,
		},

		// Partition-only operations parse as an ordinary (unmodeled) alter spec, so like the
		// other unmodeled alterations above they emit an unchanged metadata update.
		{
			query: "ALTER TABLE users DROP PARTITION p0",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4))`,
		},

		// These probably can't show up in the binlog but should be ignored if they do
		{query: "SELECT * FROM users"},
		{query: "SELECT id FROM users UNION SELECT id FROM orders"},
		{query: "SET autocommit = 1"},
		{query: "SET @@session.time_zone = '+00:00'"},

		// Statements we don't recognize still reach the unhandled-statement branch and fail,
		// so that something capable of affecting a captured table can't slip by unnoticed.
		{
			query:   "CALL my_procedure(1, 2)",
			wantErr: `unhandled type "*ast.CallStmt"`,
		},

		// Administrative statements which are ignored.
		{query: "ANALYZE TABLE users"},
		{query: "OPTIMIZE TABLE users"},
		{query: "CREATE INDEX idx_email ON users (email)"},

		// Sanitized equivalents of interesting queries observed in production. The
		// table/column names and comment text are anonymized, but structural variation
		// (identifier quoting, schema qualification, embedded newlines, IF [NOT] EXISTS
		// clauses, inline options like ALGORITHM/LOCK, and so on) is preserved.

		// DROP EVENT is dropped by the ignore-list regex.
		{query: "DROP EVENT IF EXISTS row_tracking_event"},

		// FLUSH parses to a benign statement; here with a backtick-quoted, schema-qualified name.
		{query: "FLUSH TABLE `backup_2024_08_17`.`accounts`"},

		// CREATE OR REPLACE DEFINER ... TRIGGER is dropped by the ignore-list regex before
		// parsing, even though it targets an active table.
		{query: "CREATE OR REPLACE DEFINER=`appuser`@`%` TRIGGER orders_deleted_trigger BEFORE DELETE ON orders FOR EACH ROW INSERT INTO orders_deleted ( id, total, created, modified, deleted ) VALUES ( OLD.id, OLD.total, OLD.created, OLD.modified, NOW() )"},

		// XA transaction control statements fail to parse and are ignored.
		{query: "XA COMMIT X'0123456789abcdef'"},
		{query: "XA END X'0123456789abcdef'"},

		// ADD COLUMN IF NOT EXISTS is a MariaDB-ism but fairly common in production.
		{
			query: "ALTER TABLE `users` ADD COLUMN IF NOT EXISTS `foobar_id` VARCHAR(255) NULL DEFAULT NULL",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), foobar_id=varchar(charset=utf8mb4))`,
		},

		// An ADD COLUMN IF NOT EXISTS against an inactive table is ignored regardless of whether
		// we could parse it, since the active-table check short-circuits first.
		{query: "ALTER TABLE `subscriptions` ADD COLUMN IF NOT EXISTS `foobar_id` VARCHAR(255) NULL DEFAULT NULL"},

		// Complicated ALTER TABLE with embedded newlines and a comment and some options.
		{
			query: "ALTER TABLE `users`\n ADD COLUMN `foobar_id` VARCHAR(255) NULL DEFAULT NULL COMMENT 'foobar id for this user',\n ALGORITHM=INSTANT, LOCK=NONE",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4), foobar_id=varchar(charset=utf8mb4))`,
		},

		// An ADD COLUMN IF NOT EXISTS when the column exists should be a no-op
		{
			query: "ALTER TABLE `users` ADD COLUMN IF NOT EXISTS `email` VARCHAR(255)",
			want:  `UpdateMetadata("test.users", id=int, name=varchar(charset=utf8mb4), email=varchar(charset=utf8mb4))`,
		},
	}

	for index, tc := range cases {
		t.Run(subtestName(index, tc.query), func(t *testing.T) {
			var tables = tc.tables
			if tables == nil {
				tables = testActiveTables()
			}
			var schema = tc.schema
			if schema == "" {
				schema = testDefaultSchema
			}

			var qa = &queryAnalyzer{parser: sqlParser, featureFlags: tc.flags, isMariaDB: tc.mariadb}
			var view = &activeTablesView{cache: tables}

			var got, err = qa.analyzeQuery(view, schema, tc.query)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr, "query: %s", tc.query)
				return
			}
			require.NoError(t, err, "query: %s", tc.query)
			require.Equal(t, tc.want, formatEffects(got), "query: %s", tc.query)
		})
	}
}

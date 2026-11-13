package connector

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// minimumMajorVersion is Oracle Database 23ai, the first release with the
// BOOLEAN type and CREATE TABLE IF NOT EXISTS.
const minimumMajorVersion = 23

type client struct {
	db *stdsql.DB
	ep *sql.Endpoint[config]
}

var _ sql.Client = (*client)(nil)

func newClient(ctx context.Context, materializationName string, ep *sql.Endpoint[config]) (sql.Client, error) {
	return &client{db: ep.Config.openDB(), ep: ep}, nil
}

func preReqs(ctx context.Context, cfg config, _ map[string]bool) *cerrors.PrereqErr {
	var errs = &cerrors.PrereqErr{}

	var db = cfg.openDB()
	defer db.Close()

	// A misconfigured address can hang for minutes before failing.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		errs.Err(connectError(err, cfg))
		return errs
	}

	var version string
	if err := db.QueryRowContext(ctx,
		"SELECT version_full FROM product_component_version WHERE product LIKE 'Oracle%' FETCH FIRST 1 ROWS ONLY",
	).Scan(&version); err != nil {
		errs.Err(fmt.Errorf("querying the database version: %w", err))
	} else if major, err := strconv.Atoi(strings.Split(version, ".")[0]); err != nil {
		errs.Err(fmt.Errorf("parsing the database version %q: %w", version, err))
	} else if major < minimumMajorVersion {
		errs.Err(fmt.Errorf("database version %s is not supported: Oracle Database 23ai or later is required", version))
	}

	var charset string
	if err := db.QueryRowContext(ctx,
		"SELECT value FROM nls_database_parameters WHERE parameter = 'NLS_CHARACTERSET'",
	).Scan(&charset); err != nil {
		errs.Err(fmt.Errorf("querying the database character set: %w", err))
	} else if charset != "AL32UTF8" {
		errs.Err(fmt.Errorf("database character set %s is not supported: the AL32UTF8 character set is required to store all Unicode text", charset))
	}

	var canCreate int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM session_privs WHERE privilege IN ('CREATE TABLE', 'CREATE ANY TABLE')",
	).Scan(&canCreate); err != nil {
		errs.Err(fmt.Errorf("querying privileges: %w", err))
	} else if canCreate == 0 {
		errs.Err(fmt.Errorf("user %q can't create tables: grant it the CREATE TABLE privilege", cfg.User))
	}

	// Tables in the user's own schema go to its default tablespace, where it
	// needs a quota (ORA-01950).
	if cfg.Schema == "" || strings.EqualFold(translateIdentifier(cfg.Schema), translateIdentifier(cfg.User)) {
		var tablespace string
		var quotas int
		if err := db.QueryRowContext(ctx, `
			SELECT u.default_tablespace,
				(SELECT COUNT(*) FROM session_privs WHERE privilege = 'UNLIMITED TABLESPACE') +
				(SELECT COUNT(*) FROM user_ts_quotas q WHERE q.tablespace_name = u.default_tablespace AND q.max_bytes != 0)
			FROM user_users u`,
		).Scan(&tablespace, &quotas); err != nil {
			errs.Err(fmt.Errorf("querying tablespace quotas: %w", err))
		} else if quotas == 0 {
			errs.Err(fmt.Errorf("user %q has no quota on its default tablespace %s: grant one with ALTER USER %s QUOTA UNLIMITED ON %s", cfg.User, tablespace, cfg.User, tablespace))
		}
	}

	return errs
}

// connectError maps a connection failure to a readable error.
func connectError(err error, cfg config) error {
	if dnsErr, ok := errors.AsType[*net.DNSError](err); ok {
		if dnsErr.IsNotFound {
			return fmt.Errorf("host at address %q cannot be found", cfg.Address)
		}
	} else if opErr, ok := errors.AsType[*net.OpError](err); ok {
		if opErr.Timeout() {
			return fmt.Errorf("connection to host at address %q timed out (incorrect host or port?)", cfg.Address)
		}
	}
	return err
}

func (c *client) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	if len(resourcePaths) == 0 {
		return nil
	}

	var schemas []string
	for _, p := range resourcePaths {
		schemas = append(schemas, c.ep.Dialect.Literal(c.ep.Dialect.TableLocator(p).TableSchema))
	}
	slices.Sort(schemas)
	schemas = slices.Compact(schemas)
	var owners = strings.Join(schemas, ", ")

	// Tables first, since a table may have no columns that we'd otherwise
	// learn it from.
	tables, err := c.db.QueryContext(ctx, fmt.Sprintf(
		"SELECT owner, table_name FROM all_tables WHERE owner IN (%s) AND dropped = 'NO'", owners))
	if err != nil {
		return fmt.Errorf("querying tables: %w", err)
	}
	defer tables.Close()
	for tables.Next() {
		var owner, name string
		if err := tables.Scan(&owner, &name); err != nil {
			return fmt.Errorf("scanning table: %w", err)
		}
		is.PushResource(owner, name)
	}
	if err := tables.Err(); err != nil {
		return fmt.Errorf("querying tables: %w", err)
	}

	columns, err := c.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT c.owner, c.table_name, c.column_name, c.nullable, c.data_type,
			c.data_precision, c.data_scale, c.data_length, c.char_length, c.char_used, c.default_length
		FROM all_tab_columns c
		JOIN all_tables t ON t.owner = c.owner AND t.table_name = c.table_name
		WHERE c.owner IN (%s) AND t.dropped = 'NO'`, owners))
	if err != nil {
		return fmt.Errorf("querying columns: %w", err)
	}
	defer columns.Close()
	for columns.Next() {
		var owner, table, name, nullable, dataType string
		var precision, scale, dataLength, charLength, defaultLength stdsql.NullInt64
		var charUsed stdsql.NullString
		if err := columns.Scan(&owner, &table, &name, &nullable, &dataType,
			&precision, &scale, &dataLength, &charLength, &charUsed, &defaultLength); err != nil {
			return fmt.Errorf("scanning column: %w", err)
		}

		var field = boilerplate.ExistingField{
			Name:       name,
			Nullable:   nullable == "Y",
			Type:       existingType(dataType, precision, scale),
			HasDefault: defaultLength.Valid && defaultLength.Int64 > 0,
		}
		if dataType == "VARCHAR2" || dataType == "NVARCHAR2" || dataType == "CHAR" {
			// A BYTE semantics column holds a quarter as many characters of
			// four bytes.
			field.CharacterMaxLength = int(charLength.Int64)
			if charUsed.String == "B" {
				field.CharacterMaxLength = int(dataLength.Int64) / 4
			}
		}
		is.PushResource(owner, table).PushField(field)
	}
	return columns.Err()
}

// existingType renders a column's type as it's written in DDL, so that it
// compares to mapped types.
func existingType(dataType string, precision, scale stdsql.NullInt64) string {
	if dataType != "NUMBER" || (!precision.Valid && !scale.Valid) {
		return dataType
	} else if !precision.Valid {
		return fmt.Sprintf("NUMBER(*,%d)", scale.Int64)
	}
	return fmt.Sprintf("NUMBER(%d,%d)", precision.Int64, scale.Int64)
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	_, err := c.db.ExecContext(ctx, tc.TableCreateSql)
	return err
}

func (c *client) DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	var stmt = fmt.Sprintf("DROP TABLE %s", c.ep.Dialect.Identifier(path...))
	return stmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, stmt)
		return err
	}, nil
}

func (c *client) TruncateTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	var stmt = fmt.Sprintf("TRUNCATE TABLE %s", c.ep.Dialect.Identifier(path...))
	return stmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, stmt)
		return err
	}, nil
}

// Each DDL statement commits, so every step of a column migration is durable
// on its own, and an interrupted migration resumes from the step it reached.
var columnMigrationSteps = []sql.ColumnMigrationStep{
	func(dialect sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		var cols []string
		for _, ins := range instructions {
			cols = append(cols, fmt.Sprintf("%s %s", ins.TempColumnIdentifier, ins.TypeMigration.NullableDDL))
		}
		return []string{fmt.Sprintf("ALTER TABLE %s ADD (%s)", table.Identifier, strings.Join(cols, ", "))}, nil
	},
	func(dialect sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		var sets []string
		for _, ins := range instructions {
			sets = append(sets, fmt.Sprintf("%s = %s", ins.TempColumnIdentifier, ins.TypeMigration.CastSQL(ins.TypeMigration)))
		}
		return []string{fmt.Sprintf("UPDATE %s SET %s", table.Identifier, strings.Join(sets, ", "))}, nil
	},
	func(dialect sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		var cols []string
		for _, ins := range instructions {
			cols = append(cols, ins.TypeMigration.Identifier)
		}
		return []string{fmt.Sprintf("ALTER TABLE %s DROP (%s)", table.Identifier, strings.Join(cols, ", "))}, nil
	},
	func(dialect sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		var out []string
		for _, ins := range instructions {
			out = append(out, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", table.Identifier, ins.TempColumnIdentifier, ins.TypeMigration.Identifier))
		}
		return out, nil
	},
	func(dialect sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		var mods []string
		for _, ins := range instructions {
			if ins.TypeMigration.NullableDDL != ins.TypeMigration.DDL {
				mods = append(mods, fmt.Sprintf("%s NOT NULL", ins.TypeMigration.Identifier))
			}
		}
		if len(mods) == 0 {
			return nil, nil
		}
		return []string{fmt.Sprintf("ALTER TABLE %s MODIFY (%s)", table.Identifier, strings.Join(mods, ", "))}, nil
	},
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var stmts []string

	if len(ta.AddColumns) > 0 {
		var cols []string
		for _, col := range ta.AddColumns {
			cols = append(cols, fmt.Sprintf("%s %s", col.Identifier, col.NullableDDL))
		}
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ADD (%s)", ta.Identifier, strings.Join(cols, ", ")))
	}

	if len(ta.DropNotNulls) > 0 {
		var cols []string
		for _, f := range ta.DropNotNulls {
			cols = append(cols, fmt.Sprintf("%s NULL", c.ep.Dialect.Identifier(f.Name)))
		}
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s MODIFY (%s)", ta.Identifier, strings.Join(cols, ", ")))
	}

	if len(ta.ColumnTypeChanges) > 0 {
		steps, err := sql.StdColumnTypeMigrations(ctx, c.ep.Dialect, ta.Table, ta.ColumnTypeChanges, columnMigrationSteps...)
		if err != nil {
			return "", nil, fmt.Errorf("rendering column migration steps: %w", err)
		}
		stmts = append(stmts, steps...)
	}

	return strings.Join(stmts, "\n"), func(ctx context.Context) error {
		for _, stmt := range stmts {
			if _, err := c.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("executing %q: %w", stmt, err)
			}
		}
		return nil
	}, nil
}

func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	return installFence(ctx, c.ep.Dialect, c.db, checkpoints, fence)
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

func (c *client) MustRecreateResource(req *pm.Request_Apply, lastBinding, newBinding *pf.MaterializationSpec_Binding) (bool, error) {
	return false, nil
}

func (c *client) checkpointsIdentifier() string {
	return c.ep.Dialect.Identifier(c.ep.MetaCheckpoints.Path...)
}

func (c *client) ListCheckpointsEntries(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM %s",
		c.ep.Dialect.Identifier("materialization"), c.checkpointsIdentifier()))
	if err != nil {
		return nil, fmt.Errorf("querying materializations from checkpoints table: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

func (c *client) DeleteCheckpointsEntry(ctx context.Context, taskName string) error {
	_, err := c.db.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s = :1",
		c.checkpointsIdentifier(), c.ep.Dialect.Identifier("materialization")), taskName)
	return err
}

// SnapshotTestTable names the columns of unquoted identifiers in lower case
// and reads integers as numbers rather than the driver's text, so that
// snapshots shared with other connectors compare equal.
func (c *client) SnapshotTestTable(ctx context.Context, path []string) ([]string, [][]any, error) {
	var ident = c.ep.Dialect.Identifier(path...)
	cols, rows, err := sql.SnapshotTestTableQuery(ctx, c.db, ident, "SELECT * FROM "+ident)
	if err != nil {
		return nil, nil, err
	}
	for i, col := range cols {
		if isSimpleIdentifier(col) && strings.ToUpper(col) == col {
			cols[i] = strings.ToLower(col)
		}
	}
	for _, row := range rows {
		for i, v := range row {
			if raw, ok := v.(json.RawMessage); ok {
				if n, err := strconv.ParseInt(string(raw), 10, 64); err == nil {
					row[i] = n
				}
			}
		}
	}
	sql.SortRows(rows)
	return cols, rows, nil
}

func (c *client) Close() {
	c.db.Close()
}

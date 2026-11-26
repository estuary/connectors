package main

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	log "github.com/sirupsen/logrus"
)

func (db *db2Database) SetupPrerequisites(ctx context.Context) []error {
	var errs []error
	for _, check := range []func(context.Context) error{
		db.prerequisiteArchiveLogging,
		db.prerequisiteControlTables,
		db.prerequisiteWatermarksTable,
	} {
		if err := check(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (db *db2Database) prerequisiteArchiveLogging(ctx context.Context) error {
	var method string
	if err := db.conn.QueryRowContext(ctx, `SELECT VALUE FROM SYSIBMADM.DBCFG WHERE NAME = 'logarchmeth1'`).Scan(&method); err != nil {
		return fmt.Errorf("unable to check archive logging: %w; run GRANT SELECT ON SYSIBMADM.DBCFG TO USER %s", err, quoteIdentifier(db.config.User))
	}
	if method = strings.TrimSpace(method); method == "" || strings.EqualFold(method, "OFF") {
		return fmt.Errorf("archive logging is disabled (LOGARCHMETH1 is OFF); run db2 \"UPDATE DB CFG FOR %s USING LOGARCHMETH1 DISK:/path/to/archive\" and db2 \"BACKUP DB %s TO /path/to/backup\" with your archive and backup directories", db.config.Database, db.config.Database)
	}
	return nil
}

func (db *db2Database) prerequisiteControlTables(ctx context.Context) error {
	for _, name := range []string{"IBMSNAP_REGISTER", "IBMSNAP_CAPPARMS"} {
		var table = quoteTableName(db.config.Advanced.CaptureSchema, name)
		var rows, err = db.conn.QueryContext(ctx, "SELECT * FROM "+table+" WHERE 1 = 0")
		if err != nil {
			return fmt.Errorf("SQL Replication control table %s is not readable: %w; if missing, run ASNCLP CREATE CONTROL TABLES FOR CAPTURE SERVER; then run GRANT SELECT ON TABLE %s TO USER %s", table, err, table, quoteIdentifier(db.config.User))
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (db *db2Database) prerequisiteWatermarksTable(ctx context.Context) error {
	var watermark = db.WatermarksTable()
	var requested = []sqlcapture.TableID{{Schema: watermark.Schema, Table: watermark.Table}}
	var tables, err = db.getTables(ctx, requested)
	if err != nil {
		return err
	}
	var table = quoteTableName(watermark.Schema, watermark.Table)
	if len(tables) == 0 {
		return fmt.Errorf("watermarks table %s does not exist; run CREATE TABLE %s (SLOT VARCHAR(1000) NOT NULL PRIMARY KEY, WATERMARK VARCHAR(1000)) DATA CAPTURE CHANGES; then register it with ASNCLP CREATE REGISTRATION (%s) DIFFERENTIAL REFRESH STAGE CDFLOW_WATERMARKS", table, table, table)
	}
	if _, err := db.inspectTable(ctx, tables[0], []string{"SLOT"}); err != nil {
		return err
	}

	// Check both privileges without creating a watermark or changing existing rows.
	for _, query := range []string{
		"UPDATE " + table + " SET WATERMARK = WATERMARK WHERE 1 = 0",
		"INSERT INTO " + table + " (SLOT, WATERMARK) SELECT 'check', 'check' FROM SYSIBM.SYSDUMMY1 WHERE 1 = 0",
	} {
		if _, err := db.conn.ExecContext(ctx, query); err != nil {
			return fmt.Errorf("watermarks table %s must have SLOT and WATERMARK columns and be writable: %w; run GRANT SELECT, INSERT, UPDATE ON TABLE %s TO USER %s", table, err, table, quoteIdentifier(db.config.User))
		}
	}
	return nil
}

func (db *db2Database) checkTableRegistration(info *sqlcapture.DiscoveryInfo) error {
	var table = quoteTableName(info.Schema, info.Name)
	if !info.BaseTable {
		return fmt.Errorf("%s is not a base table; bind a SQL Replication registered base table instead", table)
	}
	var details = info.ExtraDetails.(*db2TableDiscoveryDetails)
	if !details.DataCaptureChanges {
		return fmt.Errorf("DATA CAPTURE CHANGES is not enabled on %s; run ALTER TABLE %s DATA CAPTURE CHANGES", table, table)
	}
	if details.Registration == nil {
		return fmt.Errorf("%s has no SQL Replication registration in %s; run ASNCLP CREATE REGISTRATION (%s) DIFFERENTIAL REFRESH STAGE %s COLS ALL IMAGE BOTH PREFIX X", table, db.config.Advanced.CaptureSchema, table, quoteIdentifier("CD"+info.Name))
	}
	if details.Registration.State != "A" {
		return fmt.Errorf("SQL Replication registration for %s is not active; have the replication administrator run UPDATE %s SET STATE = 'A' WHERE SOURCE_OWNER = '%s' AND SOURCE_TABLE = '%s'; then run asnccmd capture_server=%s capture_schema=%s reinit", table,
			quoteTableName(db.config.Advanced.CaptureSchema, "IBMSNAP_REGISTER"), strings.ReplaceAll(info.Schema, "'", "''"), strings.ReplaceAll(info.Name, "'", "''"), db.config.Database, db.config.Advanced.CaptureSchema)
	}
	return nil
}

func (db *db2Database) SetupTablePrerequisites(ctx context.Context, requested []sqlcapture.TableID) map[sqlcapture.TableID]error {
	var errs = make(map[sqlcapture.TableID]error)
	if len(requested) == 0 {
		return errs
	}
	var discovered, err = db.DiscoverTableDetails(ctx, requested)
	if err != nil {
		for _, table := range requested {
			errs[table] = err
		}
		return errs
	}
	for _, table := range requested {
		var info = discovered[sqlcapture.JoinStreamID(table.Schema, table.Table)]
		if info == nil {
			errs[table] = fmt.Errorf("table %s does not exist; run CREATE TABLE with the intended columns and DATA CAPTURE CHANGES, then register it using ASNCLP", quoteTableName(table.Schema, table.Table))
			continue
		}
		if _, err := db.inspectTable(ctx, info, db.tableKey(info)); err != nil {
			errs[table] = err
		}
	}
	return errs
}

func checkOldKeyColumns(info *sqlcapture.DiscoveryInfo, keyColumns, cdColumns []string) error {
	var registration = info.ExtraDetails.(*db2TableDiscoveryDetails).Registration
	if registration.UpdateAsDeleteInsert {
		return nil
	}
	for _, key := range keyColumns {
		if registration.BeforeImagePrefix == "" || !slices.Contains(cdColumns, registration.BeforeImagePrefix+key) {
			return fmt.Errorf("table %s.%s cannot safely capture key changes: missing before-image column for %q; recreate its registration using ASNCLP CREATE REGISTRATION (%s) DIFFERENTIAL REFRESH STAGE %s OPTIONS UPDATE AS DELETE INSERT ON, or register before-images for every collection-key column with COLS ALL IMAGE BOTH PREFIX X", info.Schema, info.Name, key, quoteTableName(info.Schema, info.Name), quoteIdentifier(registration.CDTable))
		}
	}
	return nil
}

func (db *db2Database) PeriodicChecks(ctx context.Context) (err error) {
	// sqlcapture logs check errors; retain them to stop the next poll before its cursor moves.
	defer func() {
		if err != nil {
			db.registrationError = err
		}
	}()
	if db.registrationError != nil {
		return db.registrationError
	}
	if db.now().Before(db.registrationChecksAfter) {
		return nil
	}
	db.registrationChecksAfter = db.now().Add(time.Minute)
	var tables []*tableReplicationInfo
	for _, table := range db.activeTables {
		tables = append(tables, table)
	}
	slices.SortFunc(tables, func(a, b *tableReplicationInfo) int { return a.StreamID.Compare(b.StreamID) })
	for _, table := range tables {
		var state, err = db.registrationState(ctx, table)
		if err != nil {
			return err
		}

		if strings.TrimSpace(state) != "A" {
			return fmt.Errorf("SQL Replication registration for %s is missing or inactive (STATE %q); reactivate the registration and re-backfill the binding before restarting the capture", table.StreamID, strings.TrimSpace(state))
		}
		cdColumns, err := db.cdColumns(ctx, table.Registration)
		if err != nil {
			return err
		}
		var columns []string
		for _, column := range cdColumns {
			columns = append(columns, column.Name)
		}
		var added, removed = compareCDColumns(table.CatalogColumns, columns)
		if len(removed) > 0 {
			return fmt.Errorf("CD table columns removed for %s: %v; re-backfill the binding", table.StreamID, removed)
		}
		if len(added) > 0 {
			log.WithFields(log.Fields{"table": table.StreamID, "columns": added}).Info("CD table gained columns; re-backfill the binding to capture them")
			table.CatalogColumns = columns
		}
	}
	return nil
}

func (db *db2Database) registrationState(ctx context.Context, table *tableReplicationInfo) (string, error) {
	var registration = table.Registration
	var state string
	var register = quoteTableName(db.config.Advanced.CaptureSchema, "IBMSNAP_REGISTER")
	var err = db.conn.QueryRowContext(ctx, "SELECT STATE FROM "+register+" WHERE SOURCE_OWNER = ? AND SOURCE_TABLE = ? AND CD_OWNER = ? AND CD_TABLE = ?",
		table.StreamID.Schema, table.StreamID.Table, registration.CDSchema, registration.CDTable).Scan(&state)
	if err != nil && err != sql.ErrNoRows {
		return "", fmt.Errorf("error checking SQL Replication registration for %s: %w", table.StreamID, err)
	}
	return strings.TrimSpace(state), nil
}

func compareCDColumns(previous, current []string) (added, removed []string) {
	for _, name := range current {
		if !slices.Contains(previous, name) {
			added = append(added, name)
		}
	}
	for _, name := range previous {
		if !slices.Contains(current, name) {
			removed = append(removed, name)
		}
	}
	return
}

func (db *db2Database) cdColumns(ctx context.Context, registration *captureRegistration) ([]sqlcapture.ColumnInfo, error) {
	var columns, err = getColumns(ctx, db.conn, []sqlcapture.TableID{{Schema: registration.CDSchema, Table: registration.CDTable}})
	if err != nil {
		return nil, fmt.Errorf("error inspecting CD table %s.%s: %w", registration.CDSchema, registration.CDTable, err)
	}
	return columns, nil
}

// inspectTable uses the effective collection key for both prerequisites and activation.
// Db2 may register LOB/XML change flags instead of values; those columns are unusable.
func (db *db2Database) inspectTable(ctx context.Context, info *sqlcapture.DiscoveryInfo, key []string) (*tableReplicationInfo, error) {
	if err := db.checkTableRegistration(info); err != nil {
		return nil, err
	}
	for _, name := range key {
		if info.Columns[name].IsNullable {
			return nil, fmt.Errorf("key column %q of table %s.%s is nullable; pick a NOT NULL key and re-backfill the binding", name, info.Schema, info.Name)
		}
	}
	var registration = info.ExtraDetails.(*db2TableDiscoveryDetails).Registration
	var columns, err = db.cdColumns(ctx, registration)
	if err != nil {
		return nil, err
	}
	var table = &tableReplicationInfo{
		StreamID: sqlcapture.JoinStreamID(info.Schema, info.Name), Discovery: info, Registration: registration,
		KeyColumns: slices.Clone(key),
	}
	var projections []string
	for _, cdColumn := range columns {
		var name = cdColumn.Name
		table.CatalogColumns = append(table.CatalogColumns, name)
		var column, ok = info.Columns[name]
		if !ok && registration.BeforeImagePrefix != "" {
			column, ok = info.Columns[strings.TrimPrefix(name, registration.BeforeImagePrefix)]
		}
		var projection = quoteIdentifier(name)
		if ok {
			var ct = column.DataType.(db2ColumnType)
			if !cdColumnHasData(ct, cdColumn.DataType.(db2ColumnType).Name) {
				table.OmittedColumns = append(table.OmittedColumns, name)
				continue
			}
			projection = ct.projection(name)
			if ct.isLOB() {
				table.LOBColumns = append(table.LOBColumns, db2LOBColumn{Name: name, Index: len(table.CDColumns), Type: ct})
			}
		}
		projections = append(projections, projection)
		table.CDColumns = append(table.CDColumns, name)
	}
	var cdTable = quoteTableName(registration.CDSchema, registration.CDTable)
	var required = append([]string{"IBMSNAP_COMMITSEQ", "IBMSNAP_INTENTSEQ", "IBMSNAP_OPERATION"}, key...)
	if table.StreamID == db.WatermarksTable() {
		required = append(required, "SLOT", "WATERMARK")
	}
	for _, name := range required {
		if !slices.Contains(table.CDColumns, name) {
			return nil, fmt.Errorf("required column %q is missing from CD table %s; recreate the SQL Replication registration with COLS ALL including all key columns", name, cdTable)
		}
	}
	if table.StreamID != db.WatermarksTable() {
		if err := checkOldKeyColumns(info, key, table.CDColumns); err != nil {
			return nil, err
		}
	}
	for _, name := range info.ColumnNames {
		if !slices.Contains(table.CatalogColumns, name) {
			table.OmittedColumns = append(table.OmittedColumns, name)
		}
	}
	for _, column := range table.LOBColumns {
		projections = append(projections, column.lengthProjection())
	}
	table.CDProjection = strings.Join(projections, ", ")
	rows, err := db.conn.QueryContext(ctx, "SELECT "+table.CDProjection+" FROM "+cdTable+" WHERE 1 = 0")
	if err != nil {
		return nil, fmt.Errorf("CD table %s is not readable: %w; run GRANT SELECT ON TABLE %s TO USER %s", cdTable, err, cdTable, quoteIdentifier(db.config.User))
	}
	return table, rows.Close()
}

func cdColumnHasData(source db2ColumnType, cdType string) bool {
	switch source.Name {
	case "CLOB", "BLOB", "DBCLOB", "XML":
		return source.Name == strings.ToUpper(strings.TrimSpace(cdType))
	default:
		return true
	}
}

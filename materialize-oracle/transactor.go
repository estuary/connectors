package connector

import (
	"context"
	"crypto/sha256"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"text/template"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/sijms/go-ora/v2/network"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"go.gazette.dev/core/consumer/protocol"
)

var _ m.Transactor = (*transactor)(nil)

type transactor struct {
	cfg       config
	db        *stdsql.DB
	loadConn  *stdsql.Conn
	storeConn *stdsql.Conn
	storeTxn  *stdsql.Tx
	fence     sql.Fence
	fenceSQL  string
	bindings  []*binding
	be        *m.BindingEvents
	// stagingTables are the staging table names that bindings use.
	stagingTables map[string]bool
	// truncateAfterBackfill permits deleting the rows published before a
	// completed backfill.
	truncateAfterBackfill bool
	// truncations maps a binding index to the boundary of a completed
	// backfill, for deletion in this transaction's commit.
	truncations map[int]time.Time
}

type binding struct {
	target            sql.Table
	loadInsert        string
	loadQuery         string
	storeInsert       string
	mergeInto         string
	directCopy        string
	deleteFrom        string
	nullFieldsToStrip []string
	emptyStrings      []string
	// truncateSQL deletes the rows published before a boundary. It is empty
	// when truncateReason states why the table cannot be truncated.
	truncateSQL    string
	truncateReason error
}

func (b *binding) sourceDelete(it *m.StoreIterator) bool {
	if len(it.RawJSON) != 0 {
		return gjson.GetBytes(it.RawJSON, "_meta.op").Str == "d"
	}
	// Without a selected root document, Flow sends only keys and values.
	for i, col := range b.target.Keys {
		if col.Ptr == "/_meta/op" && it.Key[i] == "d" {
			return true
		}
	}
	for i, col := range b.target.Values {
		if col.Ptr == "/_meta/op" && it.Values[i] == "d" {
			return true
		}
	}
	return false
}

func prepareNewTransactor(templates templates) func(context.Context, string, map[string]bool, *sql.Endpoint[config], sql.Fence, []sql.Table, pm.Request_Open, *boilerplate.InfoSchema, *m.BindingEvents) (m.Transactor, error) {
	return func(ctx context.Context, materializationName string, featureFlags map[string]bool, ep *sql.Endpoint[config], fence sql.Fence, tables []sql.Table, open pm.Request_Open, is *boilerplate.InfoSchema, be *m.BindingEvents) (_ m.Transactor, err error) {
		if err := boilerplate.RequireSingleShard(open); err != nil {
			return nil, err
		}
		t := &transactor{
			cfg: ep.Config, fence: fence, be: be,
			stagingTables:         make(map[string]bool),
			truncateAfterBackfill: sql.TruncateAfterBackfill(featureFlags),
		}
		defer func() {
			if err != nil {
				t.Destroy()
			}
		}()
		if t.db, err = openDB(ep.Config); err != nil {
			return nil, err
		}
		if t.loadConn, err = t.db.Conn(ctx); err != nil {
			return nil, fmt.Errorf("connecting for loads: %w", err)
		}
		if t.storeConn, err = t.db.Conn(ctx); err != nil {
			return nil, fmt.Errorf("connecting for stores: %w", err)
		}
		var fenceSQL strings.Builder
		if err = templates.updateFence.Execute(&fenceSQL, fence); err != nil {
			return nil, err
		}
		t.fenceSQL = fenceSQL.String()

		for _, table := range tables {
			if err = t.addBinding(ctx, ep.Dialect, templates, materializationName, table, is); err != nil {
				return nil, fmt.Errorf("preparing table %s: %w", table.Identifier, err)
			}
		}
		t.dropStaleStagingTables(ctx, ep.Dialect, materializationName)
		return t, nil
	}
}

// stagingPrefix starts the names of a task's staging tables.
func stagingPrefix(materialization string) string {
	var sum = sha256.Sum256([]byte(materialization))
	return hex.EncodeToString(sum[:])[:12]
}

func (t *transactor) addBinding(ctx context.Context, dialect sql.Dialect, templates templates, materialization string, target sql.Table, is *boilerplate.InfoSchema) error {
	var deleteColumn = "_flow_delete"
	for {
		var collision bool
		for _, col := range target.Columns() {
			collision = collision || dialect.ColumnLocator(col.Field) == deleteColumn
		}
		if !collision {
			break
		}
		deleteColumn += "_"
	}
	var staged = stagedTable{Table: target, DeleteColumn: dialect.Identifier(deleteColumn)}
	// A global temporary table's definition is permanent while its rows are
	// session-private, so name staging tables after their task and
	// definitions, and reuse them across sessions instead of leaving one pair
	// per restart. dropStaleStagingTables removes earlier definitions.
	var hash = sha256.New()
	fmt.Fprintf(hash, "%s\x00%s", materialization, target.Identifier)
	for _, tpl := range []*template.Template{templates.createLoadTable, templates.createStoreTable} {
		if err := tpl.Execute(hash, &staged); err != nil {
			return err
		}
	}
	var suffix = stagingPrefix(materialization) + "_" + hex.EncodeToString(hash.Sum(nil))[:20]
	staged.LoadTable = dialect.Identifier(t.cfg.User, "flow_load_"+suffix)
	staged.StoreTable = dialect.Identifier(t.cfg.User, "flow_store_"+suffix)
	for _, name := range []string{"FLOW_LOAD_" + suffix, "FLOW_STORE_" + suffix} {
		t.stagingTables[strings.ToUpper(name)] = true
	}
	var b = &binding{target: target}
	if col, err := target.PublishedAtColumn(); err != nil {
		b.truncateReason = err
	} else {
		b.truncateReason = addTruncation(b, col, is)
	}
	var loadQuery = templates.loadQuery
	if t.cfg.Advanced.NoFlowDocument {
		loadQuery = templates.loadQueryNoFlowDocument
		b.nullFieldsToStrip = target.NullablePropertiesToStrip()
		for _, col := range target.RootPropertyColumns() {
			if col.MustExist && emptyStringIsNull(&col.Projection, col.BareDDL) {
				b.emptyStrings = append(b.emptyStrings, col.RootProperty())
			}
		}
	}
	for _, item := range []struct {
		out *string
		tpl *template.Template
	}{
		{&b.loadInsert, templates.loadInsert},
		{&b.loadQuery, loadQuery},
		{&b.storeInsert, templates.storeInsert},
		{&b.mergeInto, templates.mergeInto},
		{&b.directCopy, templates.directCopy},
		{&b.deleteFrom, templates.deleteFrom},
	} {
		var query strings.Builder
		if err := item.tpl.Execute(&query, &staged); err != nil {
			return err
		}
		*item.out = query.String()
	}
	for _, tpl := range []*template.Template{templates.createLoadTable, templates.createStoreTable} {
		var query strings.Builder
		if err := tpl.Execute(&query, &staged); err != nil {
			return err
		}
		// ORA-00955: an earlier session already created this definition.
		var oraErr *network.OracleError
		if _, err := t.storeConn.ExecContext(ctx, query.String()); err != nil && !(errors.As(err, &oraErr) && oraErr.ErrCode == 955) {
			return fmt.Errorf("creating staging table: %w", err)
		}
	}
	t.bindings = append(t.bindings, b)
	return nil
}

// dropStaleStagingTables drops this task's staging tables that no binding
// uses, such as those of an earlier column definition. A table that another
// session still uses fails to drop, and is left for a later session.
func (t *transactor) dropStaleStagingTables(ctx context.Context, dialect sql.Dialect, materialization string) {
	var prefix = strings.ToUpper(stagingPrefix(materialization))
	var stale []string
	rows, err := t.storeConn.QueryContext(ctx, `SELECT TABLE_NAME FROM USER_TABLES
WHERE TEMPORARY = 'Y' AND (TABLE_NAME LIKE :1 OR TABLE_NAME LIKE :2)`,
		"FLOW_LOAD_"+prefix+"%", "FLOW_STORE_"+prefix+"%")
	if err != nil {
		log.WithError(err).Warn("listing staging tables")
		return
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.WithError(err).Warn("listing staging tables")
			rows.Close()
			return
		}
		var ours = strings.HasPrefix(name, "FLOW_LOAD_"+prefix+"_") || strings.HasPrefix(name, "FLOW_STORE_"+prefix+"_")
		if ours && !t.stagingTables[name] {
			stale = append(stale, name)
		}
	}
	if err := rows.Err(); err != nil {
		log.WithError(err).Warn("listing staging tables")
	}
	rows.Close()

	for _, name := range stale {
		if _, err := t.storeConn.ExecContext(ctx, "DROP TABLE "+dialect.Identifier(t.cfg.User, name)); err != nil {
			log.WithError(err).WithField("table", name).Warn("dropping unused staging table")
		}
	}
}

// addTruncation prepares b to delete the rows whose col is earlier than a
// boundary. It returns why b cannot be truncated, as a clause suited for an
// operator.
func addTruncation(b *binding, col *sql.Column, is *boilerplate.InfoSchema) error {
	var res = is.GetResource(b.target.Path)
	if res == nil {
		return fmt.Errorf("table %s was not found in the database", b.target.Identifier)
	}
	var existing = res.GetField(col.Field)
	if existing == nil {
		return fmt.Errorf("column %s was not found in the table", col.Identifier)
	} else if !strings.EqualFold(existing.Type, "TIMESTAMP WITH TIME ZONE") {
		return fmt.Errorf("column %s is not a TIMESTAMP WITH TIME ZONE", col.Identifier)
	}
	b.truncateSQL = fmt.Sprintf("DELETE FROM %s WHERE %s < :1", b.target.Identifier, col.Identifier)
	return nil
}

func (t *transactor) RecoverCheckpoint(context.Context, pf.MaterializationSpec, pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return t.fence.Checkpoint, nil
}

func (t *transactor) UnmarshalState(json.RawMessage) error { return nil }

func (t *transactor) Flush(_ context.Context, completes map[int]time.Time) error {
	t.truncations = make(map[int]time.Time, len(completes))
	for binding, boundary := range completes {
		var b = t.bindings[binding]
		if b.target.DeltaUpdates {
			continue
		} else if !t.truncateAfterBackfill {
			log.WithFields(log.Fields{
				"table":    b.target.Identifier,
				"boundary": boundary,
			}).Info("rows published before the backfill were not deleted because feature flags disable it")
			continue
		}
		if b.truncateSQL == "" {
			if t.be != nil {
				t.be.Warn(b.target.Identifier, fmt.Sprintf("Rows published before the backfill of table %s were not deleted because %s.", b.target.Identifier, b.truncateReason))
			}
			continue
		}
		t.truncations[binding] = boundary
	}
	return nil
}

func (t *transactor) Acknowledge(context.Context, []json.RawMessage, []string) (*pf.ConnectorState, error) {
	return nil, nil
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	ctx := it.Context()
	txn, err := t.loadConn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer txn.Rollback()
	var active = make([]bool, len(t.bindings))
	for it.Next() {
		b := t.bindings[it.Binding]
		converted, err := b.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting load key: %w", err)
		}
		if _, err := txn.ExecContext(ctx, b.loadInsert, converted...); err != nil {
			return fmt.Errorf("staging load key: %w", err)
		}
		active[it.Binding] = true
	}
	if err := it.Err(); err != nil {
		return err
	}
	if t.be != nil {
		t.be.StartedEvaluatingLoads()
		defer t.be.FinishedEvaluatingLoads()
	}
	// Oracle cannot UNION CLOB columns, so evaluate each staged join separately.
	for index, b := range t.bindings {
		if !active[index] {
			continue
		}
		if err := loadBinding(ctx, txn, index, b, loaded); err != nil {
			return err
		}
	}
	return txn.Commit()
}

func loadBinding(ctx context.Context, txn *stdsql.Tx, index int, b *binding, loaded func(int, json.RawMessage) error) error {
	rows, err := txn.QueryContext(ctx, b.loadQuery)
	if err != nil {
		return fmt.Errorf("loading %s: %w", b.target.Identifier, err)
	}
	defer rows.Close()
	for rows.Next() {
		var document string
		if err := rows.Scan(&document); err != nil {
			return err
		}
		doc := json.RawMessage(document)
		if len(b.nullFieldsToStrip) > 0 {
			if doc, err = sql.StripNullFields(doc, b.nullFieldsToStrip); err != nil {
				return err
			}
		}
		if doc, err = restoreEmptyStrings(doc, b.emptyStrings); err != nil {
			return err
		}
		if err := loaded(index, doc); err != nil {
			return err
		}
	}
	return rows.Err()
}

// restoreEmptyStrings turns NULLs of required string properties back into the
// empty strings that Oracle stored as NULL.
func restoreEmptyStrings(doc json.RawMessage, fields []string) (json.RawMessage, error) {
	if len(fields) == 0 {
		return doc, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(doc, &m); err != nil {
		return nil, fmt.Errorf("unmarshalling loaded document: %w", err)
	}
	var restored bool
	for _, field := range fields {
		if v, ok := m[field]; ok && string(v) == "null" {
			m[field], restored = json.RawMessage(`""`), true
		}
	}
	if !restored {
		return doc, nil
	}
	return json.Marshal(m)
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	ctx := it.Context()
	txn, err := t.storeConn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	t.storeTxn = txn
	defuser := sql.NewTxDefuser(txn)
	defer defuser.MaybeRollback()
	staged := make([]int64, len(t.bindings))

	for it.Next(t.cfg.HardDelete) {
		b := t.bindings[it.Binding]
		// Source deletes can arrive without a protocol delete flag when the
		// collection's schema does not declare a delete reduction.
		flowDelete := t.cfg.HardDelete && !b.target.DeltaUpdates &&
			(it.Delete || b.sourceDelete(it))
		var converted []any
		if flowDelete {
			if converted, err = b.target.ConvertKey(it.Key); err == nil {
				converted = append(converted, make([]any, len(b.target.Columns())-len(converted))...)
			}
		} else {
			converted, err = b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		}
		if err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		}
		deleteValue, _ := boolToNumber(flowDelete)
		converted = append(converted, deleteValue)
		if _, err := txn.ExecContext(ctx, b.storeInsert, converted...); err != nil {
			return nil, fmt.Errorf("staging store for %s: %w", b.target.Identifier, err)
		}
		staged[it.Binding]++
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	round := it.Round
	truncations := t.truncations
	defuser.Defuse()
	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		defer txn.Rollback()
		defer func() { t.storeTxn = nil }()
		for index, b := range t.bindings {
			boundary, truncate := truncations[index]
			if staged[index] == 0 && !truncate {
				continue
			}
			if t.be != nil {
				t.be.StartedResourceCommit(b.target.Path)
			}
			var queries []string
			if staged[index] > 0 && b.target.DeltaUpdates {
				queries = []string{b.directCopy}
			} else if staged[index] > 0 {
				queries = []string{b.mergeInto}
				if t.cfg.HardDelete {
					queries = append(queries, b.deleteFrom)
				}
			}
			var affected int64
			for _, query := range queries {
				result, err := txn.ExecContext(ctx, query)
				if err != nil {
					return nil, m.FinishedOperation(fmt.Errorf("storing %s: %w", b.target.Identifier, err))
				}
				count, err := result.RowsAffected()
				if err != nil {
					return nil, m.FinishedOperation(err)
				}
				affected += count
			}
			var stats = m.TotalRowStats(affected).WithStaged(staged[index])
			// The truncation follows the binding's stores, so it removes only
			// the rows they did not rewrite.
			if truncate {
				result, err := txn.ExecContext(ctx, b.truncateSQL, go_ora.TimeStampTZ(boundary))
				if err != nil {
					return nil, m.FinishedOperation(fmt.Errorf("truncating %s after backfill: %w", b.target.Identifier, err))
				}
				deleted, err := result.RowsAffected()
				if err != nil {
					return nil, m.FinishedOperation(err)
				}
				stats = stats.WithTruncated(deleted)
				log.WithFields(log.Fields{
					"table":    b.target.Identifier,
					"boundary": boundary,
					"deleted":  deleted,
				}).Info("truncated rows published before the backfill")
			}
			if t.be != nil {
				t.be.ReportRowStats(round, b.target.Path, stats)
				t.be.FinishedResourceCommit(b.target.Path)
			}
		}

		checkpoint, err := runtimeCheckpoint.Marshal()
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("marshalling checkpoint: %w", err))
		}
		result, err := txn.ExecContext(ctx, t.fenceSQL,
			go_ora.Clob{String: base64.StdEncoding.EncodeToString(checkpoint), Valid: true},
			string(t.fence.Materialization), t.fence.KeyBegin, t.fence.KeyEnd, t.fence.Fence)
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("updating checkpoint: %w", err))
		} else if affected, err := result.RowsAffected(); err != nil {
			return nil, m.FinishedOperation(err)
		} else if affected != 1 {
			return nil, m.FinishedOperation(fmt.Errorf("this transactions session was fenced off by another"))
		}
		if err := txn.Commit(); err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("committing store: %w", err))
		}
		t.fence.Checkpoint = checkpoint
		return nil, nil
	}, nil
}

func (t *transactor) Destroy() {
	if t.storeTxn != nil {
		t.storeTxn.Rollback()
	}
	// Staging tables are kept for later sessions; another session of this
	// task may already be using them.
	for _, conn := range []*stdsql.Conn{t.loadConn, t.storeConn} {
		if conn != nil {
			conn.Close()
		}
	}
	if t.db != nil {
		t.db.Close()
	}
}

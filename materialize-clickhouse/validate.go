package main

import (
	"context"
	"crypto/rand"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

func (d *driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	resp, err := d.sqlDriver.Validate(ctx, req)
	if err != nil {
		return nil, err
	}

	var cfg config
	if err := json.Unmarshal(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)

	var db *stdsql.DB
	defer func() {
		if db != nil {
			db.Close()
		}
	}()

	for i, rb := range req.Bindings {
		var rc tableConfig
		if err := json.Unmarshal(rb.ResourceConfigJson, &rc); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		var newExpr = strings.TrimSpace(rc.PartitionBy)

		var last = findLastBinding([]string{rc.Table}, req.LastMaterialization)
		var lastExpr string
		if last != nil {
			var lastRC tableConfig
			if err := json.Unmarshal(last.ResourceConfigJson, &lastRC); err != nil {
				return nil, fmt.Errorf("parsing last resource config: %w", err)
			}
			lastExpr = strings.TrimSpace(lastRC.PartitionBy)
		}

		// ClickHouse only accepts PARTITION BY at CREATE TABLE time, so a
		// changed expression requires re-creating the table. Absent a
		// backfill counter bump, block the publication with INCOMPATIBLE
		// constraints, in the same way materialize-spanner handles its
		// key-distribution setting.
		// The constraint goes on every projection, not just key fields: an
		// INCOMPATIBLE constraint on an unselected field is non-fatal to the
		// control plane, and delta-updates bindings may deselect their key
		// fields entirely. Constraining every projection guarantees at least
		// one lands on a selected field, which is what blocks the publication.
		if last != nil && rb.Backfill == last.Backfill && newExpr != lastExpr {
			for _, p := range rb.Collection.Projections {
				resp.Bindings[i].ProjectionConstraints = append(resp.Bindings[i].ProjectionConstraints, &pm.Response_Validated_ProjectionConstraint{
					Field: p.Field,
					Constraint: &pm.Response_Validated_Constraint{
						Type:   pm.Response_Validated_Constraint_INCOMPATIBLE,
						Reason: fmt.Sprintf("'partition_by' changed from %q to %q; changing the partition key requires re-creating the table. Backfill the binding to proceed.", lastExpr, newExpr),
					},
				})
			}
			continue
		}

		// Dry-run only an expression that is about to be applied to a fresh
		// table: a brand-new binding, or a backfill re-creating the table for
		// a changed expression. An unchanged expression was verified when
		// first applied, and re-verifying it can fail spuriously after schema
		// evolution removes a projection it references -- the existing table
		// keeps its column and keeps working, and the failure would block
		// every publication touching this materialization.
		if newExpr == "" || (last != nil && newExpr == lastExpr) {
			continue
		}
		if db == nil {
			db = clickhouse.OpenDB(cfg.newClickhouseOptions())
		}
		if err := dryRunPartitionBy(ctx, db, dialect, tpls, cfg, req.Name.String(), i, rb, resp.Bindings[i], rc); err != nil {
			return nil, cerrors.NewUserError(err, fmt.Sprintf("'partition_by' expression %q is not valid for table %q", rc.PartitionBy, rc.Table))
		}
	}

	return resp, nil
}

// dryRunPartitionBy creates and immediately drops a scratch table having the
// binding's real column DDL and partition_by expression. The expression is
// too open-ended to evaluate statically, so the server is the only authority
// on whether it works: this surfaces its errors (bad syntax, missing columns,
// non-deterministic functions) at Validate time rather than at Apply.
func dryRunPartitionBy(ctx context.Context, db *stdsql.DB, dialect sql.Dialect, tpls templates, cfg config, materializationName string, bindingIdx int, rb *pm.Request_Validate_Binding, vb *pm.Response_Validated_Binding, rc tableConfig) error {
	var synth = &pf.MaterializationSpec_Binding{
		ResourceConfigJson: rb.ResourceConfigJson,
		Collection:         rb.Collection,
		FieldSelection:     synthesizeFieldSelection(rb, vb, rc.Delta || cfg.Advanced.NoFlowDocument),
	}

	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return fmt.Errorf("generating scratch table name: %w", err)
	}
	var scratch = fmt.Sprintf("flow_partition_dryrun_%x", nonce)

	var shape = sql.BuildTableShape(materializationName, synth, bindingIdx, []string{scratch}, rc.Delta)
	table, err := sql.ResolveTable(shape, dialect)
	if err != nil {
		return fmt.Errorf("resolving dry-run table: %w", err)
	}
	createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
	if err != nil {
		return fmt.Errorf("rendering dry-run CREATE TABLE: %w", err)
	}

	_, createErr := db.ExecContext(ctx, createSQL)
	if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", dialect.Identifier(scratch))); err != nil {
		log.WithFields(log.Fields{"table": scratch, "error": err}).Warn("failed to drop partition_by dry-run table")
	}
	return createErr
}

// synthesizeFieldSelection approximates the eventual field selection of a
// binding being validated, which carries no FieldSelection of its own. It
// deliberately over-includes: every field the inner validation didn't rule
// out becomes a column, so an expression referencing any selectable field
// passes the dry-run. The inverse error -- a false failure for a field that
// would be selected -- cannot happen.
func synthesizeFieldSelection(rb *pm.Request_Validate_Binding, vb *pm.Response_Validated_Binding, excludeDocument bool) pf.FieldSelection {
	var selectable = make(map[string]bool)
	for _, pc := range vb.ProjectionConstraints {
		switch pc.Constraint.Type {
		case pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
			pm.Response_Validated_Constraint_UNSATISFIABLE,
			pm.Response_Validated_Constraint_INCOMPATIBLE:
			selectable[pc.Field] = false
		default:
			if _, ok := selectable[pc.Field]; !ok {
				selectable[pc.Field] = true
			}
		}
	}
	var included = func(field string) bool { return selectable[field] }

	var out pf.FieldSelection
	out.Keys = append(out.Keys, rb.GroupBy...)
	if len(out.Keys) == 0 {
		for _, p := range rb.Collection.Projections {
			if p.IsPrimaryKey && included(p.Field) {
				out.Keys = append(out.Keys, p.Field)
			}
		}
	}

	var seen = make(map[string]bool)
	for _, k := range out.Keys {
		seen[k] = true
	}
	for _, p := range rb.Collection.Projections {
		if seen[p.Field] || !included(p.Field) {
			continue
		}
		seen[p.Field] = true
		if p.Ptr == "" {
			// A root document projection is only ever the document column.
			if out.Document == "" && !excludeDocument {
				out.Document = p.Field
			}
			continue
		}
		out.Values = append(out.Values, p.Field)
	}
	return out
}

// findLastBinding mirrors the unexported helper of materialize-boilerplate: a
// previously disabled binding still constrains the current one, so inactive
// bindings are consulted as well.
func findLastBinding(resourcePath []string, lastSpec *pf.MaterializationSpec) *pf.MaterializationSpec_Binding {
	if lastSpec == nil {
		return nil
	}
	for _, b := range lastSpec.Bindings {
		if slices.Equal(resourcePath, b.ResourcePath) {
			return b
		}
	}
	for _, b := range lastSpec.InactiveBindings {
		if slices.Equal(resourcePath, b.ResourcePath) {
			return b
		}
	}
	return nil
}

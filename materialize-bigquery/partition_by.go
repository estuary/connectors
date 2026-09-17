package connector

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/googleapi"
)

// partitionExpr extracts the trimmed partition_by expression from a binding's
// raw resource config. Empty or absent yields "", which renders no PARTITION
// BY clause. Mirrors materialize-clickhouse/sqlgen.go:313.
func partitionExpr(rawResourceConfig json.RawMessage) (string, error) {
	if len(rawResourceConfig) == 0 {
		return "", nil
	}
	var rc tableConfig
	if err := json.Unmarshal(rawResourceConfig, &rc); err != nil {
		return "", fmt.Errorf("parsing resource config for partition_by: %w", err)
	}
	return strings.TrimSpace(rc.PartitionBy), nil
}

// queryClient opens a client for the Validate dry-run only. It has no
// endpoint, so the table-management methods of client must not be called on it.
func (c config) queryClient(ctx context.Context) (*client, error) {
	bq, err := c.client(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery client: %w", err)
	}
	return bq, nil
}

func (d *driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	return d.sqlDriver.Spec(ctx, req)
}

func (d *driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	return d.sqlDriver.NewTransactor(ctx, req, be)
}

// Validate adds partition_by checks on top of the SQL driver's validation. The
// structure follows materialize-clickhouse/validate.go:21, which shipped the
// same option first.
func (d *driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	resp, err := d.sqlDriver.Validate(ctx, req)
	if err != nil {
		return nil, err
	}

	var cfg config
	if err := json.Unmarshal(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}
	var dialect = bqDialect(common.ParseFeatureFlags(cfg.Advanced.FeatureFlags, featureFlagDefaults))
	var tpls = renderTemplates(dialect)

	// Opened only if some binding needs a dry-run.
	var bq *client
	defer func() {
		if bq != nil {
			bq.Close()
		}
	}()

	for i, rb := range req.Bindings {
		var rc tableConfig
		if err := json.Unmarshal(rb.ResourceConfigJson, &rc); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		rc = rc.WithDefaults(cfg)
		path, _, err := rc.Parameters()
		if err != nil {
			return nil, err
		}
		var newExpr = strings.TrimSpace(rc.PartitionBy)

		var last = boilerplate.FindLastBinding(path, req.LastMaterialization)
		var lastExpr string
		if last != nil {
			if lastExpr, err = partitionExpr(last.ResourceConfigJson); err != nil {
				return nil, fmt.Errorf("parsing last resource config: %w", err)
			}
		}

		// BigQuery only accepts PARTITION BY at CREATE TABLE time, so a
		// changed expression requires re-creating the table. Absent a
		// backfill counter bump, block the publication with INCOMPATIBLE
		// constraints. The constraint goes on every projection, not just key
		// fields: an INCOMPATIBLE constraint on an unselected field is
		// non-fatal to the control plane, and delta-updates bindings may
		// deselect their key fields entirely. Constraining every projection
		// guarantees at least one lands on a selected field, which is what
		// blocks the publication.
		if last != nil && rb.Backfill == last.Backfill && newExpr != lastExpr {
			for _, p := range rb.Collection.Projections {
				resp.Bindings[i].ProjectionConstraints = append(resp.Bindings[i].ProjectionConstraints, &pm.Response_Validated_ProjectionConstraint{
					Field: p.Field,
					Constraint: &pm.Response_Validated_Constraint{
						Type:   pm.Response_Validated_Constraint_INCOMPATIBLE,
						Reason: fmt.Sprintf("'partition_by' changed from %q to %q; changing the partitioning requires re-creating the table. Backfill the binding to proceed.", lastExpr, newExpr),
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
		if bq == nil {
			if bq, err = cfg.queryClient(ctx); err != nil {
				return nil, err
			}
		}
		if err := dryRunPartitionBy(ctx, bq, dialect, tpls, cfg, req.Name.String(), i, rb, resp.Bindings[i], rc); err != nil {
			return nil, cerrors.NewUserError(err, fmt.Sprintf("'partition_by' expression %q is not valid for table %q", rc.PartitionBy, rc.Table))
		}
	}

	return resp, nil
}

// dryRunPartitionBy submits the binding's real CREATE TABLE DDL, with its
// partition_by expression and a scratch table name, as a BigQuery dry-run job,
// so that BigQuery's own errors for the expression surface at Validate rather
// than Apply. A dataset that does not exist yet is created by Apply, so the
// dry-run is skipped for it and the real CREATE TABLE surfaces any error
// instead. Adapted from materialize-clickhouse/validate.go:123.
func dryRunPartitionBy(ctx context.Context, bq *client, dialect sql.Dialect, tpls templates, cfg config, materializationName string, bindingIdx int, rb *pm.Request_Validate_Binding, vb *pm.Response_Validated_Binding, rc tableConfig) error {
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

	var shape = sql.BuildTableShape(materializationName, synth, bindingIdx, []string{rc.projectID, rc.Dataset, scratch}, rc.Delta)
	table, err := sql.ResolveTable(shape, dialect)
	if err != nil {
		return fmt.Errorf("resolving dry-run table: %w", err)
	}
	createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
	if err != nil {
		return fmt.Errorf("rendering dry-run CREATE TABLE: %w", err)
	}

	var q = bq.newQuery(createSQL)
	q.DryRun = true
	job, err := q.Run(ctx)
	if err == nil && job != nil && job.LastStatus() != nil {
		err = job.LastStatus().Err()
	}
	var gErr *googleapi.Error
	if errors.As(err, &gErr) && gErr.Code == 404 {
		log.WithFields(log.Fields{
			"dataset": rc.Dataset,
			"table":   rc.Table,
			"error":   err,
		}).Info("skipping partition_by dry-run because the dataset does not exist yet")
		return nil
	}
	return err
}

// synthesizeFieldSelection approximates the eventual field selection of a
// binding being validated, which carries no FieldSelection of its own. It
// deliberately over-includes every field the inner validation didn't rule
// out, so an expression referencing any selectable field passes the dry-run.
// Copied from materialize-clickhouse/validate.go:159.
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

func (d *driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return d.sqlDriver.Apply(ctx, req)
}

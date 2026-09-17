package connector

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
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

// normalizePartitionExpr reduces a partition expression to a form in which
// spelling differences that BigQuery ignores -- case, whitespace, identifier
// quoting, redundant wrapping parentheses, and the DATE(_PARTITIONTIME) alias
// of _PARTITIONDATE -- compare equal.
func normalizePartitionExpr(expr string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(expr) {
		switch {
		case r == '`', r == ' ', r == '\t', r == '\n', r == '\r':
		default:
			b.WriteRune(r)
		}
	}
	var out = b.String()
	for strings.HasPrefix(out, "(") && strings.HasSuffix(out, ")") && parensWrapWhole(out) {
		out = out[1 : len(out)-1]
	}
	if out == "DATE(_PARTITIONTIME)" {
		out = "_PARTITIONDATE"
	}
	return out
}

// parensWrapWhole reports whether the leading "(" of s closes at its final
// character, as opposed to "(a),(b)" where the first group closes early.
func parensWrapWhole(s string) bool {
	var depth int
	for i, r := range s {
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(s)-1 {
				return false
			}
		}
	}
	return depth == 0
}

// partitionByFromDDL extracts the PARTITION BY expression from the canonical
// CREATE TABLE statement BigQuery reports in INFORMATION_SCHEMA.TABLES.ddl.
// Top-level clauses there start at the beginning of a line; column definitions
// are indented, so a column option mentioning "PARTITION BY" is not mistaken
// for the clause.
func partitionByFromDDL(ddl string) (string, bool) {
	const prefix = "PARTITION BY "
	var lines = strings.Split(ddl, "\n")
	for i, line := range lines {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		var parts = []string{strings.TrimPrefix(line, prefix)}
		for _, next := range lines[i+1:] {
			if next == "" || strings.HasPrefix(next, "CLUSTER BY") || strings.HasPrefix(next, "OPTIONS") ||
				strings.HasPrefix(next, "DEFAULT COLLATE") || strings.HasPrefix(next, ";") {
				break
			}
			parts = append(parts, next)
		}
		return strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(strings.Join(parts, "\n")), ";")), true
	}
	return "", false
}

// queryClient opens a client for running ad-hoc queries only. It has no
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
	return boilerplate.RunApply(ctx, req, func(ctx context.Context, name string, cfg config, featureFlags map[string]bool) (boilerplate.Materializer[config, sql.FieldConfig, tableConfig, sql.MappedType], error) {
		inner, err := d.sqlDriver.NewMaterializer(ctx, name, cfg, featureFlags)
		if err != nil {
			return nil, err
		}
		return &applyMaterializer{Materializer: inner, cfg: cfg, spec: req.Materialization, lastSpec: req.LastMaterialization, dialect: bqDialect(featureFlags)}, nil
	})
}

// applyMaterializer wraps the SQL materializer for the Apply RPC to check the
// partitioning of tables that this Apply keeps rather than re-creates. The
// boilerplate invokes UpdateResource for exactly those bindings; new tables
// and drop-and-recreate backfills go through CreateResource instead.
type applyMaterializer struct {
	boilerplate.Materializer[config, sql.FieldConfig, tableConfig, sql.MappedType]
	cfg      config
	spec     *pf.MaterializationSpec
	lastSpec *pf.MaterializationSpec
	dialect  sql.Dialect

	mu sync.Mutex
	bq *client
	// ddls caches, per dataset, the canonical DDL of every table in that
	// dataset which a binding of this spec partitions, so that the billed
	// INFORMATION_SCHEMA query runs at most once per dataset.
	ddls map[string]map[string]string
}

func (a *applyMaterializer) FlushDDL(ctx context.Context) error {
	if flusher, ok := a.Materializer.(boilerplate.DDLFlusher); ok {
		return flusher.FlushDDL(ctx)
	}
	return nil
}

func (a *applyMaterializer) Close(ctx context.Context) {
	if a.bq != nil {
		a.bq.Close()
	}
	a.Materializer.Close(ctx)
}

func (a *applyMaterializer) UpdateResource(ctx context.Context, path []string, existing boilerplate.ExistingResource, update boilerplate.BindingUpdate[config, tableConfig, sql.MappedType]) (string, boilerplate.ActionApplyFn, error) {
	if err := a.checkPartitioning(ctx, path, existing, &update.Binding.MaterializationSpec_Binding, update.Binding.Config); err != nil {
		return "", nil, err
	}
	return a.Materializer.UpdateResource(ctx, path, existing, update)
}

// checkPartitioning compares an existing table's partitioning with the
// binding's partition_by. A configured expression that the table does not
// match is an error, since the connector would otherwise keep materializing
// into a table whose partitioning silently disagrees with the catalog. A
// partitioned table with no expression configured only warns: the connector
// created or adopted it as-is, but will not reproduce the partitioning if a
// backfill ever re-creates it.
//
// The remedy offered for a mismatch depends on whether this Apply is already
// a backfill of the binding: an unchanged expression makes the backfill
// truncate the table, so re-creating it needs the always_drop_tables_on_backfill
// feature flag rather than another backfill.
func (a *applyMaterializer) checkPartitioning(ctx context.Context, path []string, existing boilerplate.ExistingResource, binding *pf.MaterializationSpec_Binding, rc tableConfig) error {
	var identifier = a.dialect.Identifier(path...)
	var expr = strings.TrimSpace(rc.PartitionBy)

	if expr == "" {
		if md, ok := existing.Meta.(*bigquery.TableMetadata); ok {
			var field string
			switch {
			case md.TimePartitioning != nil && md.TimePartitioning.Field != "":
				field = md.TimePartitioning.Field
			case md.TimePartitioning != nil:
				field = "_PARTITIONTIME"
			case md.RangePartitioning != nil:
				field = md.RangePartitioning.Field
			default:
				return nil
			}
			log.WithFields(log.Fields{
				"table": identifier,
				"field": field,
			}).Warn("table is partitioned but the binding has no 'partition_by'; the partitioning will be lost if a backfill re-creates the table. Set 'partition_by' on the binding to preserve it.")
		}
		return nil
	}

	actual, partitioned, err := a.existingPartitionBy(ctx, path)
	if err != nil {
		return err
	}
	if partitioned && normalizePartitionExpr(actual) == normalizePartitionExpr(expr) {
		return nil
	}

	var mismatch = fmt.Sprintf("table %s is not partitioned", identifier)
	if partitioned {
		mismatch = fmt.Sprintf("table %s is partitioned by %q", identifier, actual)
	}
	var recreate = "Backfill the binding to re-create the table with the configured partitioning"
	if last := boilerplate.FindLastBinding(path, a.lastSpec); last != nil && last.Backfill != binding.Backfill {
		recreate = "This backfill truncates the table because 'partition_by' is unchanged; enable the 'always_drop_tables_on_backfill' feature flag to re-create it with the configured partitioning instead"
	}
	return cerrors.NewUserError(nil, fmt.Sprintf(
		"%s, but the binding's 'partition_by' is %q. %s, or change 'partition_by' to match the existing table.",
		mismatch, expr, recreate))
}

// existingPartitionBy returns the PARTITION BY expression of the table at
// path as BigQuery canonically reports it, loading the dataset's DDLs on first
// use.
func (a *applyMaterializer) existingPartitionBy(ctx context.Context, path []string) (string, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var loc = a.dialect.TableLocator(path)
	if a.ddls == nil {
		a.ddls = make(map[string]map[string]string)
	}
	ddls, ok := a.ddls[loc.TableSchema]
	if !ok {
		var err error
		if ddls, err = a.queryDDLs(ctx, path[0], loc.TableSchema); err != nil {
			return "", false, err
		}
		a.ddls[loc.TableSchema] = ddls
	}

	ddl, ok := ddls[loc.TableName]
	if !ok {
		return "", false, fmt.Errorf("table %s not found in INFORMATION_SCHEMA.TABLES of dataset %q", a.dialect.Identifier(path...), loc.TableSchema)
	}
	expr, partitioned := partitionByFromDDL(ddl)
	return expr, partitioned, nil
}

// queryDDLs reads the canonical DDL of every table in the dataset that a
// binding of this spec configures partition_by for. This is the feature's only
// billed query, so it covers all such tables of the dataset at once and is
// never issued for a dataset without an opted-in binding.
func (a *applyMaterializer) queryDDLs(ctx context.Context, project, dataset string) (map[string]string, error) {
	var tables []string
	for _, b := range a.spec.Bindings {
		expr, err := partitionExpr(b.ResourceConfigJson)
		if err != nil {
			return nil, err
		}
		if loc := a.dialect.TableLocator(b.ResourcePath); expr != "" && loc.TableSchema == dataset {
			tables = append(tables, loc.TableName)
		}
	}

	if a.bq == nil {
		var err error
		if a.bq, err = a.cfg.queryClient(ctx); err != nil {
			return nil, err
		}
	}

	job, err := a.bq.query(ctx, fmt.Sprintf(
		"SELECT table_name, ddl FROM `%s.%s.INFORMATION_SCHEMA.TABLES` WHERE table_name IN UNNEST(?)",
		project, dataset,
	), tables)
	if err != nil {
		return nil, fmt.Errorf("querying INFORMATION_SCHEMA.TABLES of dataset %q: %w", dataset, err)
	}
	it, err := job.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading INFORMATION_SCHEMA.TABLES of dataset %q: %w", dataset, err)
	}

	var out = make(map[string]string)
	for {
		var row struct {
			TableName string `bigquery:"table_name"`
			DDL       string `bigquery:"ddl"`
		}
		if err := it.Next(&row); err == iterator.Done {
			break
		} else if err != nil {
			return nil, fmt.Errorf("reading INFORMATION_SCHEMA.TABLES row: %w", err)
		}
		out[row.TableName] = row.DDL
	}
	return out, nil
}

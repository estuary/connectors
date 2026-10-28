package connector

import (
	"context"
	"crypto/rand"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
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

	var db = clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	if cfg.distributed() {
		if err := checkCluster(ctx, cfg); err != nil {
			return nil, err
		}
	}
	if req.LastMaterialization != nil {
		var lastCfg config
		if err := json.Unmarshal(req.LastMaterialization.ConfigJson, &lastCfg); err != nil {
			return nil, fmt.Errorf("parsing last endpoint config: %w", err)
		}
		// Dropping the tables of a clustered binding is a cluster-wide
		// operation, and the single-server path only ever drops one table on
		// one node, so the switch is not something a backfill can perform.
		if lastCfg.distributed() && !cfg.distributed() {
			return nil, cerrors.NewUserError(nil, fmt.Sprintf(
				"the 'cluster' setting cannot be removed once a materialization has used cluster %q; create a new materialization instead, after dropping the tables on the cluster", lastCfg.Cluster))
		}
	}

	tableMetas, err := readTableKeyMeta(ctx, db, dialect, cfg.Database)
	if err != nil {
		return nil, err
	}

	for i, rb := range req.Bindings {
		var rc tableConfig
		if err := json.Unmarshal(rb.ResourceConfigJson, &rc); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		var newExpr = strings.TrimSpace(rc.PartitionBy)

		// Surface an unusable sorting key while the materialization is being
		// published, so it is seen before the task runs. This is a hard error
		// because a sorting key survives everything a user can do from the
		// catalog: a backfill truncates the table and keeps its DDL, so there is
		// no republication that would make the binding valid.
		var meta, exists = tableMetas[rc.Table]
		// Deduplicated because synthesizeFieldSelection approximates the eventual
		// key selection, and a collection projecting one location under several
		// names can name it more than once. The sorting key holds each column
		// once, so a repeat would read as a mismatch.
		var keys = slices.Compact(slices.Sorted(slices.Values(
			synthesizeFieldSelection(rb, resp.Bindings[i], rc.Delta || cfg.Advanced.NoFlowDocument).Keys)))
		if warning, err := checkSortingKey(dialect.Identifier(rc.Table), keys, meta, exists); err != nil {
			return nil, err
		} else if warning != "" {
			log.WithField("table", rc.Table).Warn(warning)
		}

		if err := checkShardingKey(rc, cfg, keys); err != nil {
			return nil, err
		}
		if cfg.distributed() {
			requireShardingKeyFields(resp.Bindings[i], rc, keys)
		}

		var last = boilerplate.FindLastBinding([]string{rc.Table}, req.LastMaterialization)
		var lastExpr string
		var lastShardingKey []string
		if last != nil {
			var lastRC tableConfig
			if err := json.Unmarshal(last.ResourceConfigJson, &lastRC); err != nil {
				return nil, fmt.Errorf("parsing last resource config: %w", err)
			}
			lastExpr = strings.TrimSpace(lastRC.PartitionBy)
			lastShardingKey = lastRC.ShardingKey
		}

		// Each of these settings is fixed at CREATE TABLE time, so a change
		// requires re-creating the table. Absent a backfill counter bump,
		// block the publication with INCOMPATIBLE constraints, in the same
		// way materialize-spanner handles its key-distribution setting.
		// The constraint goes on every projection, not just key fields: an
		// INCOMPATIBLE constraint on an unselected field is non-fatal to the
		// control plane, and delta-updates bindings may deselect their key
		// fields entirely. Constraining every projection guarantees at least
		// one lands on a selected field, which is what blocks the publication.
		var recreateReason string
		switch {
		case last == nil || rb.Backfill != last.Backfill:
		case newExpr != lastExpr:
			recreateReason = fmt.Sprintf("'partition_by' changed from %q to %q; changing the partition key requires re-creating the table", lastExpr, newExpr)
		case !slices.Equal(rc.ShardingKey, lastShardingKey):
			recreateReason = fmt.Sprintf("'sharding_key' changed from %v to %v; changing the sharding key moves rows between shards, which requires re-creating the table", lastShardingKey, rc.ShardingKey)
		}
		if recreateReason != "" {
			for _, p := range rb.Collection.Projections {
				resp.Bindings[i].ProjectionConstraints = append(resp.Bindings[i].ProjectionConstraints, &pm.Response_Validated_ProjectionConstraint{
					Field: p.Field,
					Constraint: &pm.Response_Validated_Constraint{
						Type:   pm.Response_Validated_Constraint_INCOMPATIBLE,
						Reason: recreateReason + ". Backfill the binding to proceed.",
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
		if err := dryRunPartitionBy(ctx, db, dialect, tpls, cfg, req.Name.String(), i, rb, resp.Bindings[i], rc); err != nil {
			return nil, cerrors.NewUserError(err, fmt.Sprintf("'partition_by' expression %q is not valid for table %q", rc.PartitionBy, rc.Table))
		}
	}

	return resp, nil
}

// checkCluster rejects a cluster that cannot host Distributed tables: any
// cluster on ClickHouse Cloud, where the Distributed engine is unavailable
// and there are no shards to target, or one the server does not know.
func checkCluster(ctx context.Context, cfg config) error {
	conn, err := clickhouse.Open(cfg.newClickhouseOptions())
	if err != nil {
		return fmt.Errorf("connecting to check cluster %q: %w", cfg.Cluster, err)
	}
	defer conn.Close()

	var cloudMode string
	if err := conn.QueryRow(ctx, "SELECT value FROM system.settings WHERE name = 'cloud_mode'").Scan(&cloudMode); err != nil && !errors.Is(err, stdsql.ErrNoRows) {
		return fmt.Errorf("querying cloud_mode: %w", err)
	}
	if cloudMode == "1" {
		return cerrors.NewUserError(nil, "ClickHouse Cloud does not support Distributed tables. Remove the 'cluster' setting to materialize to ClickHouse Cloud.")
	}
	if _, err := readTopology(ctx, conn, cfg.Cluster); err != nil {
		return cerrors.NewUserError(err, fmt.Sprintf("cluster %q is not usable: %v", cfg.Cluster, err))
	}
	return nil
}

// requireShardingKeyFields marks the fields the Distributed table hashes as
// required, so that a field selection can never leave the sharding
// expression without a column. Without a configured sharding key every key
// field is hashed.
func requireShardingKeyFields(vb *pm.Response_Validated_Binding, rc tableConfig, keys []string) {
	var required = rc.ShardingKey
	if len(required) == 0 {
		required = keys
	}
	for _, pc := range vb.ProjectionConstraints {
		if !slices.Contains(required, pc.Field) {
			continue
		}
		switch pc.Constraint.Type {
		case pm.Response_Validated_Constraint_FIELD_OPTIONAL, pm.Response_Validated_Constraint_LOCATION_RECOMMENDED:
			pc.Constraint = &pm.Response_Validated_Constraint{
				Type:   pm.Response_Validated_Constraint_LOCATION_REQUIRED,
				Reason: "This field is part of the table's sharding key on the cluster and must be materialized",
			}
		}
	}
}

// checkShardingKey enforces that a sharding key is only set in distributed
// mode and names only fields of the binding's key. A sharding-key field
// outside the key would let rows with equal keys land on different shards,
// where ReplacingMergeTree could never deduplicate them.
func checkShardingKey(rc tableConfig, cfg config, keys []string) error {
	if len(rc.ShardingKey) == 0 {
		return nil
	}
	if !cfg.distributed() {
		return cerrors.NewUserError(nil, fmt.Sprintf("table %q sets 'sharding_key' but the endpoint has no 'cluster'; a sharding key only applies when materializing to a cluster", rc.Table))
	}
	var missing []string
	for _, f := range rc.ShardingKey {
		if !slices.Contains(keys, f) {
			missing = append(missing, f)
		}
	}
	if len(missing) > 0 {
		return cerrors.NewUserError(nil, fmt.Sprintf("table %q: 'sharding_key' fields %v are not part of the table's key %v; every sharding key field must be a key field", rc.Table, missing, keys))
	}
	return nil
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

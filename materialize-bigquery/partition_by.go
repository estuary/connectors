package connector

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/googleapi"
)

// partitionExpr extracts the trimmed partition_by expression from a binding's
// raw resource config. Also see materialize-clickhouse/sqlgen.go:313.
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

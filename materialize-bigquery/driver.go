package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

type driver struct {
	sqlDriver *sql.Driver[config, tableConfig]
}

var _ boilerplate.Connector = &driver{}

func NewDriver() *driver {
	return &driver{sqlDriver: newSQLDriver()}
}

func NewMaterializer(ctx context.Context, materializationName string, cfg config, featureFlags map[string]bool) (boilerplate.Materializer[config, sql.FieldConfig, tableConfig, sql.MappedType], error) {
	return newSQLDriver().NewMaterializer(ctx, materializationName, cfg, featureFlags)
}

func (d *driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	return d.sqlDriver.Spec(ctx, req)
}

func (d *driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	return d.sqlDriver.NewTransactor(ctx, req, be)
}

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
		// constraints. Constraining every projection
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
		// table. Existing tables will work as-is.
		if newExpr == "" || (last != nil && newExpr == lastExpr) {
			continue
		}
		if bq == nil {
			if bq, err = cfg.client(ctx, nil); err != nil {
				return nil, fmt.Errorf("creating bigquery client: %w", err)
			}
		}
		if err := dryRunPartitionBy(ctx, bq, dialect, tpls, cfg, req.Name.String(), i, rb, resp.Bindings[i], rc); err != nil {
			return nil, cerrors.NewUserError(err, fmt.Sprintf("'partition_by' expression %q is not valid for table %q", rc.PartitionBy, rc.Table))
		}
	}

	return resp, nil
}

func (d *driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return d.sqlDriver.Apply(ctx, req)
}

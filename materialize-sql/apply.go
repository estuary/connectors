package sql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

// TableCreate is a new table that needs to be created.
type TableCreate struct {
	Table
	TableCreateSql string

	ResourceConfigJson json.RawMessage
}

// TableAlter is the alterations for a table that are needed, including new columns that should be
// added and existing columns that should have their nullability constraints dropped.
type TableAlter struct {
	Table
	AddColumns []Column

	// DropNotNulls is a list of existing columns that need their nullability dropped, either
	// because the projection is not required anymore, or because the column exists in the
	// materialized table and is required but is not included in the field selection for the
	// materialization.
	DropNotNulls []boilerplate.EndpointField
}

// MetaSpecsUpdate is an endpoint-specific parameterized query and parameters needed to persist a
// new or updated materialization spec.
type MetaSpecsUpdate struct {
	ParameterizedQuery string
	Parameters         []interface{}
	QueryString        string // For endpoints that do not support parameterized queries.
}

var _ boilerplate.Applier = (*sqlApplier)(nil)

type sqlApplier struct {
	client   Client
	is       *boilerplate.InfoSchema
	endpoint *Endpoint
}

func newSqlApplier(client Client, is *boilerplate.InfoSchema, endpoint *Endpoint) *sqlApplier {
	return &sqlApplier{
		client:   client,
		is:       is,
		endpoint: endpoint,
	}
}

func (a *sqlApplier) CreateMetaTables(ctx context.Context, spec *pf.MaterializationSpec) (string, boilerplate.ActionApplyFn, error) {
	if (a.endpoint.MetaCheckpoints == nil || a.is.HasResource(a.endpoint.MetaCheckpoints.Path)) &&
		(a.endpoint.MetaSpecs == nil || a.is.HasResource(a.endpoint.MetaSpecs.Path)) {
		// If this materialization does not use the checkpoints or specs table OR it does and
		// they already exist, there is nothing more to do here.
		return "", nil, nil
	}

	var creates []TableCreate
	var actionDesc []string

	for _, meta := range []*TableShape{a.endpoint.MetaSpecs, a.endpoint.MetaCheckpoints} {
		if meta == nil {
			continue
		}
		resolved, err := ResolveTable(*meta, a.endpoint.Dialect)
		if err != nil {
			return "", nil, err
		}
		createStatement, err := RenderTableTemplate(resolved, a.endpoint.CreateTableTemplate)
		if err != nil {
			return "", nil, err
		}
		creates = append(creates, TableCreate{
			Table:              resolved,
			TableCreateSql:     createStatement,
			ResourceConfigJson: nil, // not applicable for meta tables
		})
		actionDesc = append(actionDesc, createStatement)
	}

	if len(creates) == 0 {
		return "", nil, nil
	}

	return strings.Join(actionDesc, "\n"), func(ctx context.Context) error {
		for _, c := range creates {
			if err := a.client.CreateTable(ctx, c); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

func (a *sqlApplier) CreateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int) (string, boilerplate.ActionApplyFn, error) {
	table, err := getTable(a.endpoint, spec, bindingIndex)
	if err != nil {
		return "", nil, err
	}

	createStatement, err := RenderTableTemplate(table, a.endpoint.CreateTableTemplate)
	if err != nil {
		return "", nil, err
	}

	return createStatement, func(ctx context.Context) error {
		if err := a.client.CreateTable(ctx, TableCreate{
			Table:              table,
			TableCreateSql:     createStatement,
			ResourceConfigJson: spec.Bindings[bindingIndex].ResourceConfigJson,
		}); err != nil {
			log.WithFields(log.Fields{
				"table":          table.Identifier,
				"tableCreateSql": createStatement,
			}).Error("table creation failed")
			return fmt.Errorf("failed to create table %q: %w", table.Identifier, err)
		}

		return nil
	}, nil
}

func (a *sqlApplier) LoadSpec(ctx context.Context, materialization pf.Materialization) (*pf.MaterializationSpec, error) {
	spec, _, err := loadSpec(ctx, a.client, a.endpoint, materialization)
	if err != nil {
		return nil, err
	}

	return spec, nil
}

func (a *sqlApplier) PutSpec(ctx context.Context, spec *pf.MaterializationSpec, version string, exists bool) (string, boilerplate.ActionApplyFn, error) {
	specBytes, err := spec.Marshal()
	if err != nil {
		panic(err) // Cannot fail to marshal.
	}

	var description string
	var specUpdate MetaSpecsUpdate
	if a.endpoint.MetaSpecs != nil {
		// Insert or update the materialization specification. Both parameterized queries and
		// literal query strings are supported. A parameterized query is generally preferable, but
		// some endpoints don't have support for those.
		var paramArgs = []interface{}{
			a.endpoint.Identifier(a.endpoint.MetaSpecs.Path...),
			a.endpoint.Placeholder(0),
			a.endpoint.Placeholder(1),
			a.endpoint.Placeholder(2),
		}
		var params = []interface{}{
			version,
			base64.StdEncoding.EncodeToString(specBytes),
			spec.Name.String(),
		}
		var queryStringArgs = []interface{}{
			paramArgs[0],
			a.endpoint.Literal(params[0].(string)),
			a.endpoint.Literal(params[1].(string)),
			a.endpoint.Literal(params[2].(string)),
		}
		var descriptionArgs = []interface{}{
			paramArgs[0],
			a.endpoint.Literal(params[0].(string)),
			a.endpoint.Literal("(a-base64-encoded-value)"),
			a.endpoint.Literal(params[2].(string)),
		}

		var q string
		if exists {
			q = "UPDATE %[1]s SET version = %[2]s, spec = %[3]s WHERE materialization = %[4]s;"
		} else {
			q = "INSERT INTO %[1]s (version, spec, materialization) VALUES (%[2]s, %[3]s, %[4]s);"
		}

		specUpdate.ParameterizedQuery = fmt.Sprintf(q, paramArgs...)
		specUpdate.Parameters = params
		specUpdate.QueryString = fmt.Sprintf(q, queryStringArgs...)
		description = fmt.Sprintf(q, descriptionArgs...)
	}

	return description, func(ctx context.Context) error {
		return a.client.PutSpec(ctx, specUpdate)
	}, nil
}

func (a *sqlApplier) DeleteResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	return a.client.DeleteTable(ctx, path)
}

func (a *sqlApplier) UpdateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int, bindingUpdate boilerplate.BindingUpdate) (string, boilerplate.ActionApplyFn, error) {
	table, err := getTable(a.endpoint, spec, bindingIndex)
	if err != nil {
		return "", nil, err
	}

	getColumn := func(field string) (Column, error) {
		for _, c := range table.Columns() {
			if field == c.Field {
				return *c, nil
			}
		}
		return Column{}, fmt.Errorf("could not find column for field %q in table %q", field, table.Identifier)
	}

	alter := TableAlter{
		Table:        table,
		DropNotNulls: bindingUpdate.NewlyNullableFields,
	}

	for _, newProjection := range bindingUpdate.NewProjections {
		col, err := getColumn(newProjection.Field)
		if err != nil {
			return "", nil, err
		}
		alter.AddColumns = append(alter.AddColumns, col)
	}

	// We only currently handle adding columns or dropping nullability constraints for SQL
	// materializations.
	if len(alter.AddColumns) == 0 && len(alter.DropNotNulls) == 0 {
		return "", nil, nil
	}

	return a.client.AlterTable(ctx, alter)
}

func getTable(endpoint *Endpoint, spec *pf.MaterializationSpec, bindingIndex int) (Table, error) {
	binding := spec.Bindings[bindingIndex]
	resource := endpoint.NewResource(endpoint)
	if err := pf.UnmarshalStrict(binding.ResourceConfigJson, resource); err != nil {
		return Table{}, fmt.Errorf("unmarshalling resource binding for collection %q: %w", binding.Collection.Name.String(), err)
	}

	tableShape := BuildTableShape(spec, bindingIndex, resource)
	return ResolveTable(tableShape, endpoint.Dialect)
}

// loadSpec loads the named MaterializationSpec and its version that's stored within the Endpoint,
// if any.
func loadSpec(ctx context.Context, client Client, endpoint *Endpoint, materialization pf.Materialization) (*pf.MaterializationSpec, string, error) {
	var (
		err              error
		metaSpecs        Table
		spec             = new(pf.MaterializationSpec)
		specB64, version string
	)

	if endpoint.MetaSpecs == nil {
		return nil, "", nil
	}
	if metaSpecs, err = ResolveTable(*endpoint.MetaSpecs, endpoint.Dialect); err != nil {
		return nil, "", fmt.Errorf("resolving specifications table: %w", err)
	}
	specB64, version, err = client.FetchSpecAndVersion(ctx, metaSpecs, materialization)

	if err == sql.ErrNoRows {
		return nil, "", nil
	} else if err != nil {
		log.WithFields(log.Fields{
			"table": endpoint.MetaSpecs.Path,
			"err":   err,
		}).Info("failed to query materialization spec (the table may not be initialized?)")
		return nil, "", nil
	} else if specBytes, err := base64.StdEncoding.DecodeString(specB64); err != nil {
		return nil, version, fmt.Errorf("base64.Decode: %w", err)
	} else if err = spec.Unmarshal(specBytes); err != nil {
		return nil, version, fmt.Errorf("spec.Unmarshal: %w", err)
	} else if err = spec.Validate(); err != nil {
		return nil, version, fmt.Errorf("validating spec: %w", err)
	}

	return spec, version, nil
}

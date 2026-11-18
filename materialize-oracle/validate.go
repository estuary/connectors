package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

func (d *oracleDriver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	var materializer *validationMaterializer
	var endpointCfg config
	validated, err := boilerplate.RunValidate(ctx, req, func(ctx context.Context, name string, cfg config, flags map[string]bool) (boilerplate.Materializer[config, sql.FieldConfig, tableConfig, sql.MappedType], error) {
		m, err := d.NewMaterializer(ctx, name, cfg, flags)
		if err != nil {
			return nil, err
		}
		endpointCfg = cfg
		materializer = &validationMaterializer{Materializer: m}
		return materializer, nil
	})
	if err != nil {
		return nil, err
	}
	dialect := createOracleDialect(endpointCfg.DefaultNamespace())
	for idx, binding := range validated.Bindings {
		// A backfill may drop and recreate an existing table, so its keys
		// must fit as well.
		last := boilerplate.FindLastBinding(binding.ResourcePath, req.LastMaterialization)
		exists := materializer.infoSchema.GetResource(binding.ResourcePath) != nil
		if binding.DeltaUpdates || (exists && (last == nil || last.Backfill == req.Bindings[idx].Backfill)) {
			continue
		}
		if err := checkKeyTextSize(dialect, req.Bindings[idx]); err != nil {
			return nil, err
		}
	}
	if !endpointCfg.Advanced.NoFlowDocument {
		return validated, forbidLongIdentifiers(req, validated)
	}
	for idx, binding := range validated.Bindings {
		if binding.DeltaUpdates {
			continue
		}
		if err := checkCastMultipleTypes(req.Bindings[idx]); err != nil {
			return nil, err
		}
	}

	// Without a prior specification, the shared validator assumes an existing
	// table has a root document. Oracle can instead rebuild it from columns if
	// no_flow_document is enabled and none of the root projections has a column.
	for idx, binding := range validated.Bindings {
		if binding.DeltaUpdates || boilerplate.FindLastBinding(binding.ResourcePath, req.LastMaterialization) != nil {
			continue
		}
		resource := materializer.infoSchema.GetResource(binding.ResourcePath)
		if resource == nil {
			continue
		}
		collection := req.Bindings[idx].Collection
		var hasDocument bool
		for _, p := range collection.Projections {
			if p.IsRootDocumentProjection() && resource.GetField(p.Field) != nil {
				hasDocument = true
				break
			}
		}
		if hasDocument {
			continue
		}
		for _, pc := range binding.ProjectionConstraints {
			p := collection.GetProjection(pc.Field)
			if p.IsRootDocumentProjection() && !p.IsPrimaryKey && pc.Constraint.Type == pm.Response_Validated_Constraint_FIELD_REQUIRED {
				pc.Constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
				pc.Constraint.Reason = "Root document projection is optional when flow_document is disabled"
			}
		}
	}
	if endpointCfg.HardDelete {
		for idx, binding := range validated.Bindings {
			if !binding.DeltaUpdates {
				requireDeleteProjection(req.Bindings[idx].Collection, binding)
			}
		}
	}
	return validated, forbidLongIdentifiers(req, validated)
}

func requireDeleteProjection(collection pf.CollectionSpec, binding *pm.Response_Validated_Binding) {
	var document, operation *pm.Response_Validated_ProjectionConstraint
	for _, pc := range binding.ProjectionConstraints {
		p := collection.GetProjection(pc.Field)
		if p.IsRootDocumentProjection() {
			if pc.Constraint.Type == pm.Response_Validated_Constraint_LOCATION_REQUIRED || pc.Constraint.Type == pm.Response_Validated_Constraint_FIELD_REQUIRED {
				return
			}
			document = pc
		} else if p.Ptr == "/_meta/op" {
			operation = pc
		}
	}

	required := operation
	reason := "Source deletes require a projection of /_meta/op when the root document is excluded"
	if required == nil {
		required = document
		reason = "Source deletes require the root document unless the collection projects /_meta/op"
	}
	if required != nil {
		// Keep compatibility constraints intact while requiring the delete marker.
		binding.ProjectionConstraints = append(binding.ProjectionConstraints, &pm.Response_Validated_ProjectionConstraint{
			Field: required.Field,
			Constraint: &pm.Response_Validated_Constraint{
				Type:        pm.Response_Validated_Constraint_LOCATION_REQUIRED,
				Reason:      reason,
				FoldedField: required.Constraint.FoldedField,
			},
		})
	}
}

type validationMaterializer struct {
	boilerplate.Materializer[config, sql.FieldConfig, tableConfig, sql.MappedType]
	infoSchema *boilerplate.InfoSchema
}

func (m *validationMaterializer) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, paths [][]string) error {
	m.infoSchema = is
	return m.Materializer.PopulateInfoSchema(ctx, is, paths)
}

var varcharSize = regexp.MustCompile(`^VARCHAR2\((\d+)( BYTE)?\)$`)

// checkKeyTextSize rejects new tables whose string keys exceed the index budget.
func checkKeyTextSize(dialect sql.Dialect, binding *pm.Request_Validate_Binding) error {
	// The table's primary key is the group-by, which may name alternate
	// projections of the collection key.
	var keys []*pf.Projection
	if len(binding.GroupBy) != 0 {
		for _, field := range binding.GroupBy {
			if p := binding.Collection.GetProjection(field); p != nil {
				keys = append(keys, p)
			}
		}
	} else {
		for _, ptr := range binding.Collection.Key {
			var key *pf.Projection
			for i := range binding.Collection.Projections {
				if p := &binding.Collection.Projections[i]; p.Ptr == ptr && (key == nil || p.IsPrimaryKey && !key.IsPrimaryKey) {
					key = p
				}
			}
			if key != nil {
				keys = append(keys, key)
			}
		}
	}

	var total int
	for _, key := range keys {
		p := *key
		p.IsPrimaryKey = true
		var fc sql.FieldConfig
		if raw := binding.FieldConfigJsonMap[p.Field]; len(raw) != 0 {
			if err := json.Unmarshal(raw, &fc); err != nil {
				return fmt.Errorf("parsing field configuration for %q: %w", p.Field, err)
			}
		}
		if size := varcharSize.FindStringSubmatch(dialect.MapType(&sql.Projection{Projection: p}, fc).BareDDL); size != nil {
			bytes, _ := strconv.Atoi(size[1])
			total += bytes
		}
	}
	if total > keyTextBudget {
		return fmt.Errorf("collection %s: string key columns would need %d bytes, over the %d bytes that fit Oracle's primary key index; set maxLength on the string key fields (%d bytes per character) or use delta updates",
			binding.Collection.Name, total, keyTextBudget, maxCharBytes)
	}
	return nil
}

// checkCastMultipleTypes rejects castToString on fields of more than one type
// when documents are loaded from columns, since the text of a string value
// cannot be told apart from that of another type.
func checkCastMultipleTypes(binding *pm.Request_Validate_Binding) error {
	for field, raw := range binding.FieldConfigJsonMap {
		var fc sql.FieldConfig
		if len(raw) == 0 {
			continue
		} else if err := json.Unmarshal(raw, &fc); err != nil {
			return fmt.Errorf("parsing field configuration for %q: %w", field, err)
		}
		p := binding.Collection.GetProjection(field)
		if p == nil || !fc.CastToString() {
			continue
		}
		if ft, _ := (&sql.Projection{Projection: *p}).AsFlatType(); ft == sql.MULTIPLE {
			return fmt.Errorf("collection %s: field %q has more than one type and cannot use castToString without the root document; remove castToString or disable no_flow_document",
				binding.Collection.Name, field)
		}
	}
	return nil
}

// invalidIdentifier describes why Oracle cannot use name as an identifier, or
// returns "" if it can.
func invalidIdentifier(name string) string {
	if strings.ContainsAny(name, "\"\x00") {
		return "cannot contain a double quote or NUL"
	} else if len(locateIdentifier(name)) > maxIdentifierBytes {
		return fmt.Sprintf("exceeds Oracle's %d-byte identifier limit", maxIdentifierBytes)
	}
	return ""
}

// forbidLongIdentifiers forbids fields whose column names Oracle cannot use:
// names over its 128-byte identifier limit, which the shared validator counts
// in characters, and names with a double quote or NUL, which Oracle
// identifiers cannot contain even when quoted.
func forbidLongIdentifiers(req *pm.Request_Validate, validated *pm.Response_Validated) error {
	for idx, binding := range validated.Bindings {
		collection := req.Bindings[idx].Collection
		required, allowed := make(map[string]bool), make(map[string]bool)
		for _, pc := range binding.ProjectionConstraints {
			ptr := collection.GetProjection(pc.Field).Ptr
			invalid := invalidIdentifier(pc.Field)
			required[ptr] = required[ptr] || pc.Constraint.Type == pm.Response_Validated_Constraint_LOCATION_REQUIRED
			allowed[ptr] = allowed[ptr] || invalid == ""
			if invalid == "" {
				continue
			} else if pc.Constraint.Type == pm.Response_Validated_Constraint_FIELD_REQUIRED {
				return fmt.Errorf("field %q is required but its name %s", pc.Field, invalid)
			}
			pc.Constraint = &pm.Response_Validated_Constraint{
				Type:        pm.Response_Validated_Constraint_FIELD_FORBIDDEN,
				Reason:      fmt.Sprintf("Field name %s. Use an alternate projection with a different name to materialize this location", invalid),
				FoldedField: pc.Constraint.FoldedField,
			}
		}
		for ptr := range required {
			if required[ptr] && !allowed[ptr] {
				return fmt.Errorf("location %q is required, but none of its field names are valid Oracle identifiers: add a projection with a shorter name that has no double quote", ptr)
			}
		}
	}
	return nil
}

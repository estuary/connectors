package connector

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestValidateWithoutFlowDocument(t *testing.T) {
	for _, tc := range []struct {
		name           string
		newTable       bool
		noFlowDocument bool
		documentType   string
		lastSpec       bool
		lastDocument   bool
		unrelatedLast  bool
		backfill       uint32
		delta          bool
		incompatibleID bool
		flags          string
		wantDocument   pm.Response_Validated_Constraint_Type
		wantError      string
	}{
		{name: "new table", newTable: true, wantDocument: pm.Response_Validated_Constraint_LOCATION_REQUIRED},
		{name: "new document-free table", newTable: true, noFlowDocument: true, wantDocument: pm.Response_Validated_Constraint_FIELD_OPTIONAL},
		{name: "fresh runtime on document-free table", noFlowDocument: true, wantDocument: pm.Response_Validated_Constraint_FIELD_OPTIONAL},
		{name: "document required by default", wantDocument: pm.Response_Validated_Constraint_FIELD_REQUIRED},
		{name: "existing document retained", noFlowDocument: true, documentType: "CLOB", wantDocument: pm.Response_Validated_Constraint_FIELD_REQUIRED},
		{name: "incompatible document rejected", noFlowDocument: true, documentType: "NUMBER", wantDocument: pm.Response_Validated_Constraint_INCOMPATIBLE},
		{name: "previous document selection retained", noFlowDocument: true, lastSpec: true, lastDocument: true, wantDocument: pm.Response_Validated_Constraint_FIELD_REQUIRED},
		{name: "previous document-free selection retained", noFlowDocument: true, lastSpec: true, wantDocument: pm.Response_Validated_Constraint_INCOMPATIBLE},
		{name: "unrelated previous binding", noFlowDocument: true, lastSpec: true, unrelatedLast: true, wantDocument: pm.Response_Validated_Constraint_FIELD_OPTIONAL},
		{name: "backfill", noFlowDocument: true, lastSpec: true, lastDocument: true, backfill: 1, wantDocument: pm.Response_Validated_Constraint_FIELD_OPTIONAL},
		{name: "delta updates", noFlowDocument: true, delta: true, wantDocument: pm.Response_Validated_Constraint_FIELD_OPTIONAL},
		{name: "incompatible key rejected", noFlowDocument: true, incompatibleID: true, wantDocument: pm.Response_Validated_Constraint_FIELD_OPTIONAL},
		{name: "existing tables disabled by default", noFlowDocument: true, flags: "", wantError: "already exists for new binding"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.Advanced.NoFlowDocument = tc.noFlowDocument
			if tc.flags != "" || tc.wantError != "" {
				cfg.Advanced.FeatureFlags = tc.flags
			}
			spec := testMaterialization(t, cfg)
			resource, err := json.Marshal(tableConfig{Table: "orders", Delta: tc.delta})
			require.NoError(t, err)
			req := &pm.Request_Validate{
				Name: spec.Name, ConfigJson: spec.ConfigJson, ConnectorType: spec.ConnectorType,
				Bindings: []*pm.Request_Validate_Binding{{
					ResourceConfigJson: resource, Collection: spec.Bindings[0].Collection, Backfill: tc.backfill,
				}},
			}
			if tc.lastSpec {
				req.LastMaterialization = spec
				if !tc.lastDocument {
					spec.Bindings[0].FieldSelection.Document = ""
				}
				if tc.unrelatedLast {
					spec.Bindings[0].ResourcePath = []string{"test", "other_orders"}
				}
			}

			client := &validationTestClient{tables: make(map[string][]boilerplate.ExistingField)}
			if !tc.newTable {
				idType := "NUMBER"
				if tc.incompatibleID {
					idType = "CLOB"
				}
				client.tables["ORDERS"] = []boilerplate.ExistingField{
					{Name: "ID", Type: idType},
					{Name: "STATUS", Type: "CLOB", Nullable: true},
				}
				if tc.documentType != "" {
					client.tables["ORDERS"] = append(client.tables["ORDERS"], boilerplate.ExistingField{Name: "FLOW_DOCUMENT", Type: tc.documentType})
				}
			}
			d := testDriver(t, client)
			validated, err := d.Validate(t.Context(), req)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			require.Len(t, validated.Bindings, 1)
			require.Equal(t, []string{"test", "orders"}, validated.Bindings[0].ResourcePath)
			require.Equal(t, tc.delta, validated.Bindings[0].DeltaUpdates)
			constraints := make(map[string][]pm.Response_Validated_Constraint_Type)
			for _, pc := range validated.Bindings[0].ProjectionConstraints {
				constraints[pc.Field] = append(constraints[pc.Field], pc.Constraint.Type)
				if pc.Field == "flow_document" {
					require.Equal(t, "FLOW_DOCUMENT", pc.Constraint.FoldedField)
				}
			}
			require.Equal(t, []pm.Response_Validated_Constraint_Type{tc.wantDocument}, constraints["flow_document"])
			if tc.incompatibleID {
				require.Contains(t, constraints["id"], pm.Response_Validated_Constraint_INCOMPATIBLE)
			} else if !tc.newTable && tc.backfill == 0 {
				require.Equal(t, []pm.Response_Validated_Constraint_Type{pm.Response_Validated_Constraint_FIELD_REQUIRED}, constraints["id"])
			}
		})
	}
}

func TestValidateMixedDocumentTables(t *testing.T) {
	cfg := testConfig()
	cfg.Advanced.NoFlowDocument = true
	spec := testMaterialization(t, cfg)
	client := &validationTestClient{tables: map[string][]boilerplate.ExistingField{
		"ORDERS":               {{Name: "ID", Type: "NUMBER"}},
		"ORDERS_WITH_DOCUMENT": {{Name: "ID", Type: "NUMBER"}, {Name: "FLOW_DOCUMENT", Type: "CLOB"}},
	}}
	d := testDriver(t, client)
	validated, err := d.Validate(t.Context(), &pm.Request_Validate{
		Name: spec.Name, ConfigJson: spec.ConfigJson, ConnectorType: spec.ConnectorType,
		Bindings: []*pm.Request_Validate_Binding{
			{ResourceConfigJson: json.RawMessage(`{"table":"orders"}`), Collection: spec.Bindings[0].Collection},
			{ResourceConfigJson: json.RawMessage(`{"table":"orders_with_document"}`), Collection: spec.Bindings[0].Collection},
		},
	})
	require.NoError(t, err)
	require.Len(t, validated.Bindings, 2)
	for idx, binding := range validated.Bindings {
		var constraints []pm.Response_Validated_Constraint_Type
		for _, pc := range binding.ProjectionConstraints {
			if pc.Field == "flow_document" {
				constraints = append(constraints, pc.Constraint.Type)
			}
		}
		want := pm.Response_Validated_Constraint_FIELD_OPTIONAL
		if idx == 1 {
			want = pm.Response_Validated_Constraint_FIELD_REQUIRED
		}
		require.Equal(t, []pm.Response_Validated_Constraint_Type{want}, constraints)
	}
}

func TestValidateHardDeleteWithoutFlowDocument(t *testing.T) {
	for _, tc := range []struct {
		name           string
		softDelete     bool
		flowDocument   bool
		delta          bool
		existing       bool
		existingDoc    bool
		operationField string
		operationPtr   string
		operationType  string
		wantOperation  bool
		wantDocument   bool
	}{
		{name: "new table", operationField: "_meta/op", wantOperation: true},
		{name: "existing table", existing: true, operationField: "_meta/op", operationType: "CLOB", wantOperation: true},
		{name: "aliased projection", operationField: "source_operation", wantOperation: true},
		{name: "incompatible column", existing: true, operationField: "source_operation", operationType: "NUMBER", wantOperation: true},
		{name: "missing projection", wantDocument: true},
		{name: "existing table missing projection", existing: true, wantDocument: true},
		{name: "unrelated projection", operationField: "_meta/op", operationPtr: "/op", wantDocument: true},
		{name: "root document", flowDocument: true, operationField: "_meta/op", wantDocument: true},
		{name: "existing root document", existing: true, existingDoc: true, operationField: "_meta/op", wantDocument: true},
		{name: "soft deletes", softDelete: true, operationField: "_meta/op"},
		{name: "soft deletes without projection", softDelete: true},
		{name: "delta updates", delta: true, operationField: "_meta/op"},
		{name: "delta updates without projection", delta: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.HardDelete = !tc.softDelete
			cfg.Advanced.NoFlowDocument = !tc.flowDocument
			spec := testMaterialization(t, cfg)
			collection := spec.Bindings[0].Collection
			if tc.operationField != "" {
				ptr := tc.operationPtr
				if ptr == "" {
					ptr = "/_meta/op"
				}
				collection.Projections = append(collection.Projections, pf.Projection{
					Field: tc.operationField, Ptr: ptr, Inference: pf.Inference{Types: []string{"string"}},
				})
				slices.SortFunc(collection.Projections, func(a, b pf.Projection) int { return strings.Compare(a.Field, b.Field) })
			}
			client := &validationTestClient{tables: make(map[string][]boilerplate.ExistingField)}
			if tc.existing {
				client.tables["ORDERS"] = []boilerplate.ExistingField{{Name: "ID", Type: "NUMBER"}}
				if tc.operationType != "" {
					client.tables["ORDERS"] = append(client.tables["ORDERS"], boilerplate.ExistingField{
						Name: locateIdentifier(tc.operationField), Type: tc.operationType, Nullable: true,
					})
				}
				if tc.existingDoc {
					client.tables["ORDERS"] = append(client.tables["ORDERS"], boilerplate.ExistingField{Name: "FLOW_DOCUMENT", Type: "CLOB"})
				}
			}
			resource, err := json.Marshal(tableConfig{Table: "orders", Delta: tc.delta})
			require.NoError(t, err)
			validated, err := testDriver(t, client).Validate(t.Context(), &pm.Request_Validate{
				Name: spec.Name, ConfigJson: spec.ConfigJson, ConnectorType: spec.ConnectorType,
				Bindings: []*pm.Request_Validate_Binding{{ResourceConfigJson: resource, Collection: collection}},
			})
			require.NoError(t, err)
			require.NoError(t, validated.Validate())
			var documentRequired, operationRequired, incompatible bool
			for _, pc := range validated.Bindings[0].ProjectionConstraints {
				required := pc.Constraint.Type == pm.Response_Validated_Constraint_FIELD_REQUIRED || pc.Constraint.Type == pm.Response_Validated_Constraint_LOCATION_REQUIRED
				if pc.Field == "flow_document" {
					documentRequired = documentRequired || required
				} else if pc.Field == tc.operationField {
					operationRequired = operationRequired || required
					incompatible = incompatible || pc.Constraint.Type == pm.Response_Validated_Constraint_INCOMPATIBLE
				}
				if folded := locateIdentifier(pc.Field); folded != pc.Field {
					require.Equal(t, folded, pc.Constraint.FoldedField)
				}
			}
			require.Equal(t, tc.wantDocument, documentRequired)
			require.Equal(t, tc.wantOperation, operationRequired)
			require.Equal(t, tc.operationType == "NUMBER", incompatible)
		})
	}
}

func TestValidateKeyTextSize(t *testing.T) {
	for _, tc := range []struct {
		name      string
		skuLength uint32
		existing  bool
		lastSpec  bool
		backfill  uint32
		delta     bool
		wantError string
	}{
		{name: "sized second key", skuLength: 36},
		{name: "unsized keys", wantError: "string key columns would need 8000 bytes"},
		{name: "existing table", existing: true},
		{name: "existing table of the last specification", existing: true, lastSpec: true},
		// A backfill may drop and recreate the table with the new keys.
		{name: "backfilled existing table", existing: true, lastSpec: true, backfill: 1, wantError: "string key columns would need 8000 bytes"},
		{name: "delta updates", delta: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			spec := testMaterialization(t, cfg)
			collection := spec.Bindings[0].Collection
			collection.Key = []string{"/tenant_id", "/sku"}
			collection.Projections = []pf.Projection{
				collection.Projections[0],
				{Field: "sku", Ptr: "/sku", IsPrimaryKey: true, Inference: pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{MaxLength: tc.skuLength}, Exists: pf.Inference_MUST}},
				{Field: "tenant_id", Ptr: "/tenant_id", IsPrimaryKey: true, Inference: pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MUST}},
			}
			collection.WriteSchemaJson = json.RawMessage(`{"type":"object","properties":{"tenant_id":{"type":"string"},"sku":{"type":"string"}},"required":["tenant_id","sku"]}`)
			client := &validationTestClient{tables: make(map[string][]boilerplate.ExistingField)}
			if tc.existing {
				client.tables["ORDERS"] = []boilerplate.ExistingField{{Name: "TENANT_ID", Type: "VARCHAR2", CharacterMaxLength: 4000}, {Name: "SKU", Type: "VARCHAR2", CharacterMaxLength: 4000}}
			}
			resource, err := json.Marshal(tableConfig{Table: "orders", Delta: tc.delta})
			require.NoError(t, err)
			req := &pm.Request_Validate{
				Name: spec.Name, ConfigJson: spec.ConfigJson, ConnectorType: spec.ConnectorType,
				Bindings: []*pm.Request_Validate_Binding{{ResourceConfigJson: resource, Collection: collection, Backfill: tc.backfill}},
			}
			if tc.lastSpec {
				spec.Bindings[0].Collection = collection
				spec.Bindings[0].FieldSelection.Keys = []string{"sku", "tenant_id"}
				req.LastMaterialization = spec
			}
			_, err = testDriver(t, client).Validate(t.Context(), req)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCheckKeyTextSizeAlternateGroupBy(t *testing.T) {
	str := func(field, ptr string, maxLength uint32, key bool) pf.Projection {
		return pf.Projection{Field: field, Ptr: ptr, IsPrimaryKey: key, Inference: pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{MaxLength: maxLength}, Exists: pf.Inference_MUST}}
	}
	for _, tc := range []struct {
		name      string
		maxLength uint32
		wantError string
	}{
		// Unbounded alternate keys need two VARCHAR2(4000) columns.
		{name: "unbounded", wantError: "would need 8000 bytes"},
		// Canonical non-key fields would map to VARCHAR2(4000), but as keys they need 2000 bytes each.
		{name: "sized", maxLength: 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := &pm.Request_Validate_Binding{
				Collection: pf.CollectionSpec{
					Name: "acmeCo/orders",
					Key:  []string{"/a", "/b"},
					Projections: []pf.Projection{
						str("a", "/a", tc.maxLength, false),
						str("a_key", "/a", tc.maxLength, true),
						str("b", "/b", tc.maxLength, false),
						str("b_key", "/b", tc.maxLength, true),
					},
				},
			}
			// Without a group-by, the projections marked as keys still count.
			for _, groupBy := range [][]string{{"a_key", "b_key"}, nil} {
				binding.GroupBy = groupBy
				err := checkKeyTextSize(testDialect, binding)
				if tc.wantError != "" {
					require.ErrorContains(t, err, tc.wantError)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestValidateQuotedFieldNames(t *testing.T) {
	spec := testMaterialization(t, testConfig())
	collection := spec.Bindings[0].Collection
	collection.Projections = append(collection.Projections,
		pf.Projection{Field: `a"b`, Ptr: `/a"b`, Inference: pf.Inference{Types: []string{"string"}}},
		pf.Projection{Field: "a_b", Ptr: `/a"b`, Inference: pf.Inference{Types: []string{"string"}}},
	)
	slices.SortFunc(collection.Projections, func(a, b pf.Projection) int { return strings.Compare(a.Field, b.Field) })
	constraints, err := validateNewOrders(t, spec, collection)
	require.NoError(t, err)
	require.Equal(t, pm.Response_Validated_Constraint_FIELD_FORBIDDEN, constraints[`a"b`])
	require.NotEqual(t, pm.Response_Validated_Constraint_FIELD_FORBIDDEN, constraints["a_b"])
}

func TestValidateIdentifierBytes(t *testing.T) {
	spec := testMaterialization(t, testConfig())
	collection := spec.Bindings[0].Collection
	fits, tooLong := strings.Repeat("α", 64), strings.Repeat("α", 70)
	collection.Projections = append(collection.Projections,
		pf.Projection{Field: fits, Ptr: "/fits", Inference: pf.Inference{Types: []string{"string"}}},
		pf.Projection{Field: tooLong, Ptr: "/too_long", Inference: pf.Inference{Types: []string{"string"}}},
		pf.Projection{Field: tooLong + "_id", Ptr: "/id", IsPrimaryKey: true, Inference: pf.Inference{Types: []string{"integer"}, Exists: pf.Inference_MUST}},
	)
	slices.SortFunc(collection.Projections, func(a, b pf.Projection) int { return strings.Compare(a.Field, b.Field) })
	constraints, err := validateNewOrders(t, spec, collection)
	require.NoError(t, err)
	require.NotEqual(t, pm.Response_Validated_Constraint_FIELD_FORBIDDEN, constraints[fits])
	require.Equal(t, pm.Response_Validated_Constraint_FIELD_FORBIDDEN, constraints[tooLong])
	require.Equal(t, pm.Response_Validated_Constraint_FIELD_FORBIDDEN, constraints[tooLong+"_id"])
	require.Equal(t, pm.Response_Validated_Constraint_LOCATION_REQUIRED, constraints["id"])

	// A required location must keep at least one short enough name.
	collection.Projections = slices.DeleteFunc(collection.Projections, func(p pf.Projection) bool { return p.Field == "id" })
	_, err = validateNewOrders(t, spec, collection)
	require.ErrorContains(t, err, `location "/id" is required`)
}

// validateNewOrders validates collection as a new orders table and returns the
// constraint type of each field.
func validateNewOrders(t *testing.T, spec *pf.MaterializationSpec, collection pf.CollectionSpec) (map[string]pm.Response_Validated_Constraint_Type, error) {
	validated, err := testDriver(t, &validationTestClient{}).Validate(t.Context(), &pm.Request_Validate{
		Name: spec.Name, ConfigJson: spec.ConfigJson, ConnectorType: spec.ConnectorType,
		Bindings: []*pm.Request_Validate_Binding{{ResourceConfigJson: json.RawMessage(`{"table":"orders"}`), Collection: collection}},
	})
	if err != nil {
		return nil, err
	}
	constraints := make(map[string]pm.Response_Validated_Constraint_Type)
	for _, pc := range validated.Bindings[0].ProjectionConstraints {
		constraints[pc.Field] = pc.Constraint.Type
	}
	return constraints, nil
}

type validationTestClient struct {
	sql.Client
	tables map[string][]boilerplate.ExistingField
}

func (c *validationTestClient) PopulateInfoSchema(_ context.Context, is *boilerplate.InfoSchema, _ [][]string) error {
	is.PushNamespace("TEST")
	for name, fields := range c.tables {
		resource := is.PushResource("TEST", name)
		for _, field := range fields {
			resource.PushField(field)
		}
	}
	return nil
}

func (c *validationTestClient) Close() {}

func TestCheckCastMultipleTypes(t *testing.T) {
	cast := json.RawMessage(`{"castToString": true}`)
	for _, tc := range []struct {
		name      string
		types     []string
		config    json.RawMessage
		wantError string
	}{
		{name: "multiple types cast", types: []string{"integer", "string"}, config: cast, wantError: `field "value" has more than one type`},
		{name: "multiple types with null cast", types: []string{"integer", "string", "null"}, config: cast, wantError: `field "value" has more than one type`},
		{name: "legacy alias", types: []string{"integer", "string"}, config: json.RawMessage(`{"ignoreStringFormat": true}`), wantError: `field "value" has more than one type`},
		{name: "multiple types stored as JSON", types: []string{"integer", "string"}, config: json.RawMessage(`{"DDL": "CLOB"}`)},
		{name: "single type cast", types: []string{"boolean"}, config: cast},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binding := &pm.Request_Validate_Binding{
				Collection: pf.CollectionSpec{Name: "acmeCo/values", Projections: []pf.Projection{
					{Field: "value", Ptr: "/value", Inference: pf.Inference{Types: tc.types}},
				}},
				FieldConfigJsonMap: map[string]json.RawMessage{"value": tc.config},
			}
			err := checkCastMultipleTypes(binding)
			if tc.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantError)
			}
		})
	}
}

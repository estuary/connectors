package connector

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-iceberg/python"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestAcknowledgeSubsetLeavesOtherKeysPending(t *testing.T) {
	tr := &transactor{
		cp: map[string]*pendingMerge{
			"a_table.v1": {MergeBinding: python.MergeBinding{Binding: 0, Query: "MERGE INTO a"}},
		},
		bindings: []binding{{Mapped: &boilerplate.MappedBinding[config, resource, mapped]{
			MaterializationSpec_Binding: pf.MaterializationSpec_Binding{StateKey: "a_table.v1"},
		}}},
	}

	// The staged entry's state key is not requested: no merge job may run
	// (the nil compute runner would panic otherwise) and no state update is
	// returned, so the entry remains pending in the persisted state.
	state, err := tr.Acknowledge(context.Background(), nil, []string{"other_table.v1"})
	require.NoError(t, err)
	require.Nil(t, state)
	require.NotNil(t, tr.cp["a_table.v1"])
}

func TestSpec(t *testing.T) {
	var resp, err = (Driver{}).
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func TestResourceParameters(t *testing.T) {
	for _, tt := range []struct {
		name      string
		mode      identifierCase
		namespace string
		table     string
		wantPath  []string
	}{
		{name: "unset lowercases path", mode: "", namespace: "Ns", table: "MyTable", wantPath: []string{"ns", "mytable"}},
		{name: "lowercase lowercases path", mode: identifierCaseLowercase, namespace: "Ns", table: "MyTable", wantPath: []string{"ns", "mytable"}},
		{name: "uppercase uppercases namespace and table", mode: identifierCaseUppercase, namespace: "Ns", table: "MyTable", wantPath: []string{"NS", "MYTABLE"}},
		{name: "preserve keeps case", mode: identifierCasePreserve, namespace: "Ns", table: "MyTable", wantPath: []string{"Ns", "MyTable"}},
		{name: "uppercase sanitizes", mode: identifierCaseUppercase, namespace: "My-Ns", table: "My-Table", wantPath: []string{"MY_NS", "MY_TABLE"}},
		{name: "preserve sanitizes", mode: identifierCasePreserve, namespace: "My-Ns", table: "My-Table", wantPath: []string{"My_Ns", "My_Table"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config{Namespace: "Default_Ns", Advanced: advancedConfig{TableIdentifierCase: tt.mode}}
			res := resource{Table: tt.table, Namespace: tt.namespace}.WithDefaults(cfg)
			path, _, err := res.Parameters()
			require.NoError(t, err)
			require.Equal(t, tt.wantPath, path)
		})
	}
}

func TestDefaultNamespace(t *testing.T) {
	for _, tt := range []struct {
		name      string
		mode      identifierCase
		namespace string
		want      string
	}{
		{name: "unset lowercases and sanitizes", mode: "", namespace: "Default_Ns", want: "default_ns"},
		{name: "lowercase lowercases and sanitizes", mode: identifierCaseLowercase, namespace: "Default_Ns", want: "default_ns"},
		{name: "uppercase uppercases and sanitizes", mode: identifierCaseUppercase, namespace: "Default_Ns", want: "DEFAULT_NS"},
		{name: "preserve keeps case", mode: identifierCasePreserve, namespace: "Default_Ns", want: "Default_Ns"},
		{name: "uppercase sanitizes", mode: identifierCaseUppercase, namespace: "Default-Ns", want: "DEFAULT_NS"},
		{name: "preserve sanitizes", mode: identifierCasePreserve, namespace: "Default-Ns", want: "Default_Ns"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config{Namespace: tt.namespace, Advanced: advancedConfig{TableIdentifierCase: tt.mode}}
			require.Equal(t, tt.want, cfg.DefaultNamespace())
		})
	}
}

func TestDeprecatedLowercaseColumnNamesIsHidden(t *testing.T) {
	resp, err := (Driver{}).Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)
	require.NotContains(t, string(resp.ConfigSchemaJson), "lowercase_column_names",
		"a deprecated option must not be offered to new users in the config schema")

	// Specs that already set it must still decode, since hiding the option from
	// the schema does not remove it from published specs.
	var cfg config
	require.NoError(t, json.Unmarshal([]byte(`{"advanced":{"lowercase_column_names":true}}`), &cfg))
	require.True(t, cfg.Advanced.LowercaseColumnNames)
	require.Equal(t, identifierCaseLowercase, cfg.Advanced.fieldNameCase())
}

func TestFieldNameCase(t *testing.T) {
	for _, tt := range []struct {
		name                 string
		mode                 identifierCase
		lowercaseColumnNames bool
		want                 string
	}{
		{name: "unset preserves case", mode: "", want: "MyField"},
		{name: "preserve keeps case", mode: identifierCasePreserve, want: "MyField"},
		{name: "lowercase folds down", mode: identifierCaseLowercase, want: "myfield"},
		{name: "uppercase folds up", mode: identifierCaseUppercase, want: "MYFIELD"},
		{name: "deprecated lowercase_column_names means lowercase", lowercaseColumnNames: true, want: "myfield"},
		{name: "deprecated lowercase_column_names agreeing with field_name_case", mode: identifierCaseLowercase, lowercaseColumnNames: true, want: "myfield"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d := &materialization{cfg: config{Advanced: advancedConfig{
				FieldNameCase:        tt.mode,
				LowercaseColumnNames: tt.lowercaseColumnNames,
			}}}

			m, _ := d.MapType(boilerplate.Projection{
				Projection: pf.Projection{Field: "MyField"},
				FlatType:   boilerplate.FlatTypeBoolean{},
			}, fieldConfig{})
			require.Equal(t, tt.want, m.Name)

			// The name a column is created with must match the name the
			// boilerplate translates the field to when looking for that column.
			translate := d.Config().TranslateField
			if translate == nil {
				translate = func(f string) string { return f }
			}
			require.Equal(t, tt.want, translate("MyField"))
		})
	}
}

func TestValidate(t *testing.T) {
	// The required fields are stubbed so that Validate reaches the
	// table_identifier_case check.
	cfg := config{
		URL:       "https://example.com/api/catalog",
		Warehouse: "warehouse",
		Namespace: "ns",
		Advanced:  advancedConfig{TableIdentifierCase: "camelCase"},
	}
	err := cfg.Validate()
	require.ErrorContains(t, err, "table_identifier_case")

	// Valid values pass the table_identifier_case check. Validate may still
	// error on the stubbed credentials, but never about table_identifier_case.
	for _, v := range []identifierCase{"", identifierCaseLowercase, identifierCaseUppercase, identifierCasePreserve} {
		cfg.Advanced.TableIdentifierCase = v
		if err := cfg.Validate(); err != nil {
			require.NotContains(t, err.Error(), "table_identifier_case")
		}
	}
}

func TestValidateFieldNameCase(t *testing.T) {
	// The required fields are stubbed so that Validate reaches the
	// field_name_case checks.
	baseCfg := config{
		URL:       "https://example.com/api/catalog",
		Warehouse: "warehouse",
		Namespace: "ns",
	}

	cfg := baseCfg
	cfg.Advanced.FieldNameCase = "camelCase"
	require.ErrorContains(t, cfg.Validate(), "field_name_case")

	for _, v := range []identifierCase{"", identifierCaseLowercase, identifierCaseUppercase, identifierCasePreserve} {
		cfg := baseCfg
		cfg.Advanced.FieldNameCase = v
		if err := cfg.Validate(); err != nil {
			require.NotContains(t, err.Error(), "field_name_case")
		}
	}

	// lowercase_column_names is the deprecated spelling of field_name_case:
	// lowercase, so any other setting of field_name_case contradicts it.
	for _, tt := range []struct {
		mode     identifierCase
		conflict bool
	}{
		{mode: "", conflict: false},
		{mode: identifierCaseLowercase, conflict: false},
		{mode: identifierCaseUppercase, conflict: true},
		{mode: identifierCasePreserve, conflict: true},
	} {
		t.Run("lowercase_column_names with "+string(tt.mode), func(t *testing.T) {
			cfg := baseCfg
			cfg.Advanced.LowercaseColumnNames = true
			cfg.Advanced.FieldNameCase = tt.mode

			err := cfg.Validate()
			if tt.conflict {
				require.ErrorContains(t, err, "lowercase_column_names")
				require.ErrorContains(t, err, "field_name_case")
			} else if err != nil {
				require.NotContains(t, err.Error(), "lowercase_column_names")
			}
		})
	}
}

package connector

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

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
			cfg := config{Namespace: "Default_Ns", Advanced: advancedConfig{IdentifierCase: tt.mode}}
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
			cfg := config{Namespace: tt.namespace, Advanced: advancedConfig{IdentifierCase: tt.mode}}
			require.Equal(t, tt.want, cfg.DefaultNamespace())
		})
	}
}

func TestValidate(t *testing.T) {
	// The required fields are stubbed so that Validate reaches the
	// identifier_case check.
	cfg := config{
		URL:       "https://example.com/api/catalog",
		Warehouse: "warehouse",
		Namespace: "ns",
		Advanced:  advancedConfig{IdentifierCase: "camelCase"},
	}
	err := cfg.Validate()
	require.ErrorContains(t, err, "identifier_case")

	// Valid values pass the identifier_case check. Validate may still error on
	// the stubbed credentials, but never about identifier_case.
	for _, v := range []identifierCase{"", identifierCaseLowercase, identifierCaseUppercase, identifierCasePreserve} {
		cfg.Advanced.IdentifierCase = v
		if err := cfg.Validate(); err != nil {
			require.NotContains(t, err.Error(), "identifier_case")
		}
	}
}

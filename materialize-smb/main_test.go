package main

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/filesink"
	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/connectors/go/smb"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestSpec(t *testing.T) {
	var resp, err = driver.Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, formatted)
}

func testConfig() map[string]any {
	return map[string]any{
		"host":           "smb.example.test",
		"share":          "data",
		"username":       "test",
		"password":       "testpassword",
		"directory":      "exports/flow",
		"uploadInterval": "5m",
	}
}

func TestConfig(t *testing.T) {
	tests := []struct {
		name  string
		field string
		value any
		err   string
	}{
		{name: "defaults"},
		{name: "custom port", field: "port", value: 1446},
		{name: "share root", field: "directory", value: ""},
		{name: "domain", field: "domain", value: "EXAMPLE"},
		{name: "size limit", field: "fileSizeLimit", value: 1024},
		{name: "zero port", field: "port", value: 0, err: "port must be between"},
		{name: "negative port", field: "port", value: -1, err: "port must be between"},
		{name: "large port", field: "port", value: 65536, err: "port must be between"},
		{name: "share path", field: "share", value: "data/folder", err: "share must be a share name"},
		{name: "UNC share", field: "share", value: `\\server\data`, err: "share must be a share name"},
		{name: "share parent", field: "share", value: "..", err: "share must be a share name"},
		{name: "bad interval", field: "uploadInterval", value: "soon", err: "parsing upload interval"},
		{name: "zero interval", field: "uploadInterval", value: "0s", err: "must be positive"},
		{name: "negative interval", field: "uploadInterval", value: "-1m", err: "must be positive"},
		{name: "negative size with directory", field: "fileSizeLimit", value: -1, err: "cannot be negative"},
		{name: "unknown field", field: "typo", value: "value", err: "unknown field"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := testConfig()
			if tt.field != "" {
				raw[tt.field] = tt.value
			}
			encoded, err := json.Marshal(raw)
			require.NoError(t, err)
			c, err := driver.NewConfig(encoded)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			cfg := c.(config)
			if tt.field == "port" {
				require.Equal(t, tt.value, cfg.Port)
			} else {
				require.Equal(t, 445, cfg.Port)
			}
			require.Equal(t, raw["directory"], cfg.CommonConfig().Prefix)
			require.Equal(t, ".csv.gz", cfg.CommonConfig().Extension)
			require.Equal(t, "5m", cfg.CommonConfig().UploadInterval)
			if tt.field == "fileSizeLimit" {
				require.Equal(t, tt.value, cfg.CommonConfig().FileSizeLimit)
			} else {
				require.Equal(t, 10*1024*1024*1024, cfg.CommonConfig().FileSizeLimit)
			}
		})
	}

	for _, field := range []string{"host", "share", "username", "password", "uploadInterval"} {
		t.Run("missing "+field, func(t *testing.T) {
			raw := testConfig()
			delete(raw, field)
			encoded, err := json.Marshal(raw)
			require.NoError(t, err)
			_, err = driver.NewConfig(encoded)
			require.ErrorContains(t, err, "missing '"+field+"'")
		})
	}
}

func TestDirectory(t *testing.T) {
	for _, tt := range []struct {
		name       string
		directory  any
		normalized string
	}{
		{"omitted", nil, "/"},
		{"empty", "", "/"},
		{"root", "/", "/"},
		{"backslash root", `\`, "/"},
		{"relative", "exports/flow", "/exports/flow"},
		{"leading slash", "/exports/flow", "/exports/flow"},
		{"backslashes", `exports\flow`, "/exports/flow"},
		{"leading backslash", `\exports\flow`, "/exports/flow"},
		{"mixed separators", `/exports\flow/`, "/exports/flow"},
		{"repeated separators", "//exports///flow//", "/exports/flow"},
		{"dot segments", "exports/./old/../flow", "/exports/flow"},
		{"parent within share", "exports/../flow", "/flow"},
		{"parent above root", "../../exports/flow", "/exports/flow"},
		{"current directory", ".", "/"},
		{"unicode", `My Files\日次`, "/My Files/日次"},
		{"literal percent", "exports/%Y", "/exports/%Y"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw := testConfig()
			if tt.directory == nil {
				delete(raw, "directory")
			} else {
				raw["directory"] = tt.directory
			}
			encoded, err := json.Marshal(raw)
			require.NoError(t, err)
			c, err := driver.NewConfig(encoded)
			require.NoError(t, err)
			cfg := c.(config)
			require.Equal(t, tt.normalized, cfg.Directory)
			require.Equal(t, strings.TrimPrefix(tt.normalized, "/"), cfg.CommonConfig().Prefix)

			s, share := newTestStore(t)
			require.NoError(t, s.checkWritable(t.Context(), cfg.Directory))
			key := path.Join(cfg.CommonConfig().Prefix, "orders/v0000000000/00000000000000000000.csv.gz")
			upload, err := s.StageObject(t.Context(), strings.NewReader("contents"), key)
			require.NoError(t, err)
			require.Equal(t, key, upload.FileKey())
			require.NoError(t, s.CompleteObject(t.Context(), upload))
			contents, err := os.ReadFile(share.local(smb.Path(path.Join(tt.normalized, "orders/v0000000000/00000000000000000000.csv.gz"))))
			require.NoError(t, err)
			require.Equal(t, "contents", string(contents))
		})
	}
	for _, directory := range []string{"exports\x00/flow", "\x00/../exports/flow"} {
		raw := testConfig()
		raw["directory"] = directory
		encoded, err := json.Marshal(raw)
		require.NoError(t, err)
		_, err = driver.NewConfig(encoded)
		require.ErrorContains(t, err, "directory:")
		require.ErrorContains(t, err, "NUL")
	}
}

func TestPaths(t *testing.T) {
	for _, p := range []string{"orders", "exports/flow", "events/raw", "a..b", "folder with spaces"} {
		require.NoError(t, checkBindingPaths([]string{p}))
	}
	for _, p := range []string{".", "..", "../orders", "a/../orders", "/orders", "a//b", "a/./b", "orders/", `a\b`, `C:\data`, "a/%Y", "a\x00b", "a\nb", "a/.. ", "a/..foo/..", "a:stream"} {
		t.Run(p, func(t *testing.T) {
			raw := testConfig()
			raw["directory"] = ""
			encoded, err := json.Marshal(raw)
			require.NoError(t, err)
			resource, err := json.Marshal(filesink.Resource{Path: p})
			require.NoError(t, err)
			d := driver
			d.NewStore = func(context.Context, filesink.Config, map[string]bool) (filesink.Store[*smbUpload], error) {
				t.Fatal("invalid paths must fail before connecting")
				return nil, nil
			}
			_, err = d.Validate(context.Background(), &pm.Request_Validate{
				ConfigJson: encoded,
				Bindings:   []*pm.Request_Validate_Binding{{ResourceConfigJson: resource}},
			})
			require.ErrorContains(t, err, "resource path:")
		})
	}
}

func TestPathCase(t *testing.T) {
	d := driver
	d.NewStore = func(context.Context, filesink.Config, map[string]bool) (filesink.Store[*smbUpload], error) {
		t.Fatal("paths differing only by case must fail before connecting")
		return nil, nil
	}
	encoded, err := json.Marshal(testConfig())
	require.NoError(t, err)
	paths := []string{"orders", "events/raw", "Events/Raw"}

	var validate []*pm.Request_Validate_Binding
	var open []*pf.MaterializationSpec_Binding
	for _, p := range paths {
		resource, err := json.Marshal(filesink.Resource{Path: p})
		require.NoError(t, err)
		validate = append(validate, &pm.Request_Validate_Binding{ResourceConfigJson: resource})
		open = append(open, &pf.MaterializationSpec_Binding{ResourceConfigJson: resource, ResourcePath: []string{p}, StateKey: p})
	}
	_, err = d.Validate(context.Background(), &pm.Request_Validate{ConfigJson: encoded, Bindings: validate})
	require.ErrorContains(t, err, `resource paths "events/raw" and "Events/Raw"`)
	_, _, _, err = d.NewTransactor(context.Background(), pm.Request_Open{
		Materialization: &pf.MaterializationSpec{ConfigJson: encoded, Bindings: open},
	}, m.NewBindingEvents())
	require.ErrorContains(t, err, `resource paths "events/raw" and "Events/Raw"`)
}

func TestValidatePathCaseEdit(t *testing.T) {
	// Guard against case-only edits resetting counters and overwriting files,
	// including when a binding is disabled and then re-enabled.
	for _, previous := range []string{"active", "inactive"} {
		t.Run(previous, func(t *testing.T) {
			for _, tt := range []struct {
				name     string
				path     string
				backfill uint32
				err      string
			}{
				{name: "unchanged", path: "orders", backfill: 1},
				{name: "case only", path: "Orders", backfill: 1, err: `resource path "Orders" differs from previous path "orders" only by letter case; keep the original casing or increment backfill`},
				{name: "lower backfill", path: "Orders", err: `resource path "Orders" differs from previous path "orders" only by letter case; keep the original casing or increment backfill`},
				{name: "nested case only", path: "Events/Raw", backfill: 7, err: `resource path "Events/Raw" differs from previous path "events/raw" only by letter case; keep the original casing or increment backfill`},
				{name: "nested lower backfill", path: "Events/Raw", backfill: 6, err: `resource path "Events/Raw" differs from previous path "events/raw" only by letter case; keep the original casing or increment backfill`},
				{name: "backfill", path: "Orders", backfill: 2},
				{name: "nested backfill", path: "Events/Raw", backfill: 8},
				{name: "different path", path: "customers"},
			} {
				t.Run(tt.name, func(t *testing.T) {
					store, _ := newTestStore(t)
					d := driver
					var connected bool
					d.NewStore = func(context.Context, filesink.Config, map[string]bool) (filesink.Store[*smbUpload], error) {
						connected = true
						return store, nil
					}
					encoded, err := json.Marshal(testConfig())
					require.NoError(t, err)
					resource, err := json.Marshal(filesink.Resource{Path: tt.path})
					require.NoError(t, err)
					last := &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{
						{ResourcePath: []string{"orders"}, Backfill: 1},
						{ResourcePath: []string{"events/raw"}, Backfill: 7},
					}}
					if previous == "inactive" {
						last.InactiveBindings, last.Bindings = last.Bindings, nil
					}
					resp, err := d.Validate(context.Background(), &pm.Request_Validate{
						ConfigJson:          encoded,
						LastMaterialization: last,
						Bindings: []*pm.Request_Validate_Binding{
							{ResourceConfigJson: json.RawMessage(`{"path":"other"}`)},
							{ResourceConfigJson: resource, Backfill: tt.backfill},
						},
					})
					if tt.err != "" {
						require.EqualError(t, err, tt.err)
						require.False(t, connected, "invalid edits must fail before connecting")
						return
					}
					require.NoError(t, err)
					require.True(t, connected)
					require.Equal(t, []string{tt.path}, resp.Bindings[1].ResourcePath)
				})
			}
		})
	}
}

func TestValidate(t *testing.T) {
	for _, operation := range []string{"success", "mkdir", "open", "write", "remove"} {
		t.Run(operation, func(t *testing.T) {
			store, share := newTestStore(t)
			if operation != "success" {
				share.failures[operation] = []error{os.ErrPermission}
			}
			d := driver
			d.NewStore = func(context.Context, filesink.Config, map[string]bool) (filesink.Store[*smbUpload], error) {
				return store, nil
			}
			encoded, err := json.Marshal(testConfig())
			require.NoError(t, err)
			resp, err := d.Validate(context.Background(), &pm.Request_Validate{
				ConfigJson: encoded,
				Bindings: []*pm.Request_Validate_Binding{{
					ResourceConfigJson: json.RawMessage(`{"path":"events/raw"}`),
					Collection: pf.CollectionSpec{Projections: []pf.Projection{{
						Field: "region", IsPrimaryKey: true,
					}}},
				}},
			})
			require.Equal(t, 1, share.closes)
			if operation != "success" {
				require.ErrorContains(t, err, "writable")
				require.ErrorIs(t, err, os.ErrPermission)
				return
			}
			require.NoError(t, err)
			require.True(t, resp.Bindings[0].DeltaUpdates)
			require.Equal(t, []string{"events/raw"}, resp.Bindings[0].ResourcePath)
			require.Equal(t, pm.Response_Validated_Constraint_LOCATION_REQUIRED, resp.Bindings[0].ProjectionConstraints[0].Constraint.Type)
			entries, err := os.ReadDir(filepath.Join(share.root, "exports", "flow"))
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
}

func TestTransactorClosesStore(t *testing.T) {
	store, share := newTestStore(t)
	d := driver
	d.NewStore = func(context.Context, filesink.Config, map[string]bool) (filesink.Store[*smbUpload], error) {
		return store, nil
	}
	encoded, err := json.Marshal(testConfig())
	require.NoError(t, err)
	transactor, _, _, err := d.NewTransactor(context.Background(), pm.Request_Open{
		Materialization: &pf.MaterializationSpec{ConfigJson: encoded},
	}, m.NewBindingEvents())
	require.NoError(t, err)
	transactor.Destroy()
	require.Equal(t, 1, share.closes)
	require.NoError(t, store.Close())
	require.Equal(t, 1, share.closes)
}

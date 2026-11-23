package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/estuary/connectors/filesink"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/estuary/connectors/go/smb"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
)

var featureFlagDefaults = map[string]bool{
	"retain_existing_data_on_backfill": false,
}

type config struct {
	Host      string `json:"host" jsonschema:"title=Host,description=Hostname or IP address of your SMB server." jsonschema_extras:"order=0"`
	Port      int    `json:"port,omitempty" jsonschema:"title=Port,description=Port of your SMB server.,default=445,minimum=1,maximum=65535" jsonschema_extras:"order=1,nonsensitive=true"`
	Share     string `json:"share" jsonschema:"title=Share,description=Name of the SMB share to write to. Specify a share name without a path." jsonschema_extras:"order=2"`
	Username  string `json:"username" jsonschema:"title=Username,description=Username for NTLM authentication." jsonschema_extras:"order=3"`
	Password  string `json:"password" jsonschema:"title=Password,description=Password for NTLM authentication." jsonschema_extras:"order=4,secret=true"`
	Domain    string `json:"domain,omitempty" jsonschema:"title=Domain,description=Optional NTLM domain for your account." jsonschema_extras:"order=5"`
	Directory string `json:"directory,omitempty" jsonschema:"title=Directory,description=Folder inside the share to write files under. Accepts forward slashes or backslashes with or without a leading separator. Paths are normalized. Omit or leave empty for the share root.,default=/" jsonschema_extras:"order=6"`

	UploadInterval string             `json:"uploadInterval" jsonschema:"title=Upload Interval,description=Frequency at which files will be uploaded. Must be a valid Go duration string.,enum=1s,enum=1m,enum=5m,enum=15m,enum=30m,enum=1h,default=5m" jsonschema_extras:"order=7,nonsensitive=true"`
	FileSizeLimit  int                `json:"fileSizeLimit,omitempty" jsonschema:"title=File Size Limit,description=Approximate maximum size of materialized files in bytes. Defaults to 10737418240 (10 GiB) if blank." jsonschema_extras:"order=8,nonsensitive=true"`
	CsvConfig      filesink.CsvConfig `json:"csvConfig,omitempty" jsonschema:"title=CSV Configuration,description=Configuration specific to materializing CSV files."`
	Advanced       advancedConfig     `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support." jsonschema_extras:"nonsensitive=true"`
}

func (c config) Validate() error {
	var requiredProperties = [][]string{
		{"host", c.Host},
		{"share", c.Share},
		{"username", c.Username},
		{"password", c.Password},
		{"uploadInterval", c.UploadInterval},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	} else if strings.ContainsAny(c.Share, "/\\") || checkBindingPaths([]string{c.Share}) != nil {
		return fmt.Errorf("share must be a share name, not a path")
	} else if _, err := smb.NormalizePath(c.Directory); err != nil {
		return fmt.Errorf("directory: %w", err)
	}

	if interval, err := time.ParseDuration(c.UploadInterval); err != nil {
		return fmt.Errorf("parsing upload interval %q: %w", c.UploadInterval, err)
	} else if interval <= 0 {
		return fmt.Errorf("uploadInterval must be positive")
	} else if c.FileSizeLimit < 0 {
		return fmt.Errorf("fileSizeLimit '%d' cannot be negative", c.FileSizeLimit)
	}

	return c.CsvConfig.Validate()
}

// SMB servers usually ignore letter case in names, so bindings whose paths
// differ only by case would write to the same files.
func checkBindingPaths(paths []string) error {
	for _, p := range paths {
		if err := smb.ValidatePath("/" + p); err != nil {
			return fmt.Errorf("resource path: %w", err)
		}
		if p == "" || strings.ContainsAny(p, ":<>\"|?*%") || strings.ContainsFunc(p, func(r rune) bool { return r < 32 }) {
			return fmt.Errorf("resource path: %q must be nonempty and contain no SMB-invalid characters or date patterns", p)
		}
		for _, part := range strings.Split(p, "/") {
			if strings.HasSuffix(part, ".") || strings.HasSuffix(part, " ") {
				return fmt.Errorf("resource path: %q component %q must not end with a dot or space", p, part)
			}
		}
	}
	for i := range paths {
		for j := i + 1; j < len(paths); j++ {
			if strings.EqualFold(paths[i], paths[j]) {
				return fmt.Errorf("resource paths %q and %q must differ by more than letter case", paths[i], paths[j])
			}
		}
	}
	return nil
}

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

func (c config) CommonConfig() filesink.CommonConfig {
	limit := c.FileSizeLimit
	if limit == 0 {
		limit = 10 * 1024 * 1024 * 1024
	}
	return filesink.CommonConfig{
		Prefix:                strings.TrimPrefix(c.Directory, "/"),
		Extension:             ".csv.gz",
		UploadInterval:        c.UploadInterval,
		FileSizeLimit:         limit,
		CaseInsensitiveFields: false,
	}
}

type smbDriver struct {
	filesink.FileDriver[*smbUpload, filesink.Resource]
}

var driver = smbDriver{filesink.FileDriver[*smbUpload, filesink.Resource]{
	NewConfig: func(raw json.RawMessage) (filesink.Config, error) {
		var cfg = config{Port: 445}
		if err := pf.UnmarshalStrict(raw, &cfg); err != nil {
			return nil, err
		}
		var err error
		cfg.Directory, err = smb.NormalizePath(cfg.Directory)
		return cfg, err
	},
	NewStore: func(ctx context.Context, c filesink.Config, featureFlags map[string]bool) (filesink.Store[*smbUpload], error) {
		return newStore(ctx, c.(config))
	},
	NewWriter: func(c filesink.Config, featureFlags map[string]bool, b *pf.MaterializationSpec_Binding, w io.WriteCloser) (filesink.StreamWriter, error) {
		return filesink.NewCsvStreamWriter(c.(config).CsvConfig, b, w), nil
	},
	NewConstraints: filesink.StdConstraints,
	DocumentationURL: func() string {
		return "https://go.estuary.dev/materialize-smb"
	},
	ConfigSchema: func() ([]byte, error) {
		return schemagen.GenerateSchema("EndpointConfig", config{}).MarshalJSON()
	},
}}

func (d smbDriver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	resp, err := d.FileDriver.Validate(ctx, req)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, b := range resp.Bindings {
		paths = append(paths, b.ResourcePath[0])
	}
	if err := checkBindingPaths(paths); err != nil {
		return nil, err
	}
	// A case-only edit resets the state key but still targets the same SMB files.
	if last := req.LastMaterialization; last != nil {
		for _, previous := range slices.Concat(last.Bindings, last.InactiveBindings) {
			for i, p := range paths {
				if req.Bindings[i].Backfill <= previous.Backfill && p != previous.ResourcePath[0] && strings.EqualFold(p, previous.ResourcePath[0]) {
					return nil, fmt.Errorf("resource path %q differs from previous path %q only by letter case; keep the original casing or increment backfill", p, previous.ResourcePath[0])
				}
			}
		}
	}

	c, err := d.NewConfig(req.ConfigJson)
	if err != nil {
		return nil, err
	}
	s, err := d.NewStore(ctx, c, nil)
	if err != nil {
		return nil, err
	}
	store := s.(*smbStore)
	defer closeStore(store)
	if err := store.checkWritable(ctx, c.(config).Directory); err != nil {
		return nil, fmt.Errorf("checking directory %q is writable: %w", c.(config).Directory, err)
	}
	return resp, nil
}

func (d smbDriver) NewTransactor(ctx context.Context, open pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	var paths []string
	for _, b := range open.Materialization.Bindings {
		var res filesink.Resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return nil, nil, nil, err
		}
		paths = append(paths, res.Path)
	}
	if err := checkBindingPaths(paths); err != nil {
		return nil, nil, nil, err
	}

	var store *smbStore
	fd := d.FileDriver
	fd.NewStore = func(ctx context.Context, c filesink.Config, flags map[string]bool) (filesink.Store[*smbUpload], error) {
		s, err := d.NewStore(ctx, c, flags)
		if err == nil {
			store = s.(*smbStore)
		}
		return s, err
	}
	t, opened, opts, err := fd.NewTransactor(ctx, open, be)
	if err != nil {
		if store != nil {
			closeStore(store)
		}
		return nil, nil, nil, err
	}
	return &smbTransactor{Transactor: t, store: store}, opened, opts, nil
}

type smbTransactor struct {
	m.Transactor
	store *smbStore
}

// Store checks that the transaction's staged files are still on the share
// before it commits, so a file lost in a failover makes the runtime replay the
// transaction instead of recording a file that no longer exists.
func (t *smbTransactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	startCommit, err := t.Transactor.Store(it)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, checkpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		if err := t.store.checkStaged(ctx); err != nil {
			return nil, m.FinishedOperation(err)
		}
		return startCommit(ctx, checkpoint)
	}, nil
}

func (t *smbTransactor) Destroy() {
	t.Transactor.Destroy()
	closeStore(t.store)
}

func closeStore(s *smbStore) {
	if err := s.Close(); err != nil {
		log.WithError(err).Warn("closing SMB store")
	}
}

func main() {
	boilerplate.RunMain(driver)
}

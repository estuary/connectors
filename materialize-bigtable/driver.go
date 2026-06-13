package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"cloud.google.com/go/bigtable"
	"github.com/estuary/connectors/go/auth/iam"
	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/invopop/jsonschema"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var featureFlagDefaults = map[string]common.FlagDefault{}

type AuthType string

const (
	CredentialsJSON AuthType = "CredentialsJSON"
	GCPIAM          AuthType = "GCPIAM"
)

type CredentialsJSONConfig struct {
	CredentialsJSON string `json:"credentials_json" jsonschema:"title=Service Account JSON,description=The JSON credentials of the service account to use for authorization." jsonschema_extras:"secret=true,multiline=true,order=0"`
}

type CredentialsConfig struct {
	AuthType AuthType `json:"auth_type"`

	CredentialsJSONConfig
	iam.IAMConfig
}

func (CredentialsConfig) JSONSchema() *jsonschema.Schema {
	subSchemas := []schemagen.OneOfSubSchemaT{
		schemagen.OneOfSubSchema("Credentials JSON", CredentialsJSONConfig{}, string(CredentialsJSON)),
		schemagen.OneOfSubSchema("GCP IAM", iam.GCPConfig{}, string(GCPIAM)),
	}

	return schemagen.OneOfSchema("Authentication", "", "auth_type", string(CredentialsJSON), subSchemas...)
}

func (c *CredentialsConfig) Validate() error {
	switch c.AuthType {
	case CredentialsJSON:
		if c.CredentialsJSON == "" {
			return errors.New("missing 'credentials_json'")
		}
		if !json.Valid([]byte(c.CredentialsJSON)) {
			return fmt.Errorf("service account credentials must be valid JSON, and the provided credentials were not")
		}
		return nil
	case GCPIAM:
		return c.ValidateIAM()
	default:
		return fmt.Errorf("unknown 'auth_type'")
	}
}

type config struct {
	ProjectID   string             `json:"project_id" jsonschema:"title=Project ID,description=Google Cloud Project ID that owns the Bigtable instance." jsonschema_extras:"order=0"`
	InstanceID  string             `json:"instance_id" jsonschema:"title=Instance ID,description=Bigtable instance ID for the materialized tables." jsonschema_extras:"order=1"`
	Credentials *CredentialsConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"x-iam-auth=true,order=2"`
	HardDelete  bool               `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=3"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	Endpoint     string `json:"endpoint,omitempty" jsonschema:"title=Bigtable Endpoint,description=The Bigtable endpoint URI to connect to. Use if you're materializing to a compatible API that isn't provided by Google."`
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

func (c config) Validate() error {
	var requiredProperties = [][]string{
		{"project_id", c.ProjectID},
		{"instance_id", c.InstanceID},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.Credentials == nil {
		return errors.New("missing 'credentials'")
	}

	return c.Credentials.Validate()
}

func (c config) DefaultNamespace() string { return "" }

func (c config) FeatureFlags() (string, map[string]common.FlagDefault) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

func (c config) CredentialsClientOption() (option.ClientOption, error) {
	switch c.Credentials.AuthType {
	case CredentialsJSON:
		return option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(c.Credentials.CredentialsJSON)), nil
	case GCPIAM:
		return option.WithTokenSource(oauth2.StaticTokenSource(
			&oauth2.Token{AccessToken: c.Credentials.GoogleToken()},
		)), nil
	default:
		return nil, fmt.Errorf("unknown 'auth_type'")
	}
}

func (c config) buildClientOptions() ([]option.ClientOption, error) {
	var clientOpts []option.ClientOption

	if c.Advanced.Endpoint != "" {
		clientOpts = append(clientOpts,
			option.WithEndpoint(c.Advanced.Endpoint),
			option.WithoutAuthentication(),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		)
	} else {
		credOption, err := c.CredentialsClientOption()
		if err != nil {
			return nil, err
		}
		clientOpts = append(clientOpts, credOption)
	}

	return clientOpts, nil
}

func (c config) dataClient(ctx context.Context) (*bigtable.Client, error) {
	opts, err := c.buildClientOptions()
	if err != nil {
		return nil, err
	}

	cfg := bigtable.ClientConfig{MetricsProvider: bigtable.NoopMetricsProvider{}}
	client, err := bigtable.NewClientWithConfig(ctx, c.ProjectID, c.InstanceID, cfg, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating bigtable data client: %w", err)
	}

	return client, nil
}

func (c config) adminClient(ctx context.Context) (*bigtable.AdminClient, error) {
	opts, err := c.buildClientOptions()
	if err != nil {
		return nil, err
	}

	client, err := bigtable.NewAdminClient(ctx, c.ProjectID, c.InstanceID, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating bigtable admin client: %w", err)
	}

	return client, nil
}

type resource struct {
	Table string `json:"table" jsonschema:"title=Table Name,description=The name of the Bigtable table to materialize to." jsonschema_extras:"x-collection-name=true"`
}

func (r resource) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing 'table'")
	}

	return nil
}

func (r resource) WithDefaults(cfg config) resource { return r }

func (r resource) Parameters() ([]string, bool, error) {
	return []string{normalizeTableName(r.Table)}, false, nil
}

// Bigtable table IDs match the pattern `[_a-zA-Z0-9][-_.a-zA-Z0-9]*` and are
// capped at 50 bytes. The first character cannot be `-` or `.`, so we strip
// any such leading runs after the body sanitization.
// Ref: https://cloud.google.com/bigtable/docs/reference/admin/rest/v2/projects.instances.tables/create
const maxTableNameLength = 50

var tableNameSanitizer = regexp.MustCompile(`[^a-zA-Z0-9_\-\.]`)

func normalizeTableName(t string) string {
	cleaned := tableNameSanitizer.ReplaceAllString(t, "_")
	cleaned = strings.TrimLeft(cleaned, "-.")
	if len(cleaned) > maxTableNameLength {
		cleaned = cleaned[:maxTableNameLength]
	}
	return cleaned
}

type driver struct{}

var _ boilerplate.Connector = &driver{}

func (d driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	endpointSchema, err := schemagen.GenerateSchema("Materialize Bigtable Spec", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("Bigtable Table", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return boilerplate.RunSpec(ctx, req, "https://go.estuary.dev/materialize-bigtable", endpointSchema, resourceSchema)
}

func (d driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	return boilerplate.RunValidate(ctx, req, newMaterialization)
}

func (d driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return boilerplate.RunApply(ctx, req, newMaterialization)
}

func (d driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, newMaterialization)
}

type materialization struct {
	cfg         config
	adminClient *bigtable.AdminClient
	dataClient  *bigtable.Client
}

var _ boilerplate.Materializer[config, fieldConfig, resource, mappedType] = &materialization{}

func newMaterialization(ctx context.Context, materializationName string, cfg config, featureFlags map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, mappedType], error) {
	admin, err := cfg.adminClient(ctx)
	if err != nil {
		return nil, err
	}

	data, err := cfg.dataClient(ctx)
	if err != nil {
		admin.Close()
		return nil, err
	}

	return &materialization{
		cfg:         cfg,
		adminClient: admin,
		dataClient:  data,
	}, nil
}

func (d *materialization) Config() boilerplate.MaterializeCfg {
	return boilerplate.MaterializeCfg{
		TranslateField:      func(f string) string { return f },
		ConcurrentApply:     true,
		NoCreateNamespaces:  true,
		NoTruncateResources: true,
	}
}

func (d *materialization) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	for _, p := range resourcePaths {
		tableName := p[0]
		if _, err := d.adminClient.TableInfo(ctx, tableName); err != nil {
			if status.Code(err) == codes.NotFound {
				continue
			}
			return fmt.Errorf("describing table %q: %w", tableName, err)
		}
		is.PushResource(tableName)
	}

	return nil
}

func (d *materialization) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	prereq := &cerrors.PrereqErr{}

	// Tables() proves three things: the instance/project IDs resolve, the
	// credentials authenticate, and the role has at least bigtable.tables.list
	// on the instance. Most user-facing misconfigurations surface here.
	if _, err := d.adminClient.Tables(ctx); err != nil {
		switch status.Code(err) {
		case codes.NotFound:
			prereq.Err(fmt.Errorf("Bigtable instance %q not found in project %q: double-check the project_id and instance_id", d.cfg.InstanceID, d.cfg.ProjectID))
		case codes.PermissionDenied, codes.Unauthenticated:
			prereq.Err(fmt.Errorf("permission denied accessing instance %q: the configured credentials need at minimum the Bigtable User role on this instance", d.cfg.InstanceID))
		default:
			prereq.Err(fmt.Errorf("listing tables in instance %q: %w", d.cfg.InstanceID, err))
		}
	}

	return prereq
}

func (d *materialization) NewConstraint(p pf.Projection, _ bool, fc fieldConfig) pm.Response_Validated_Constraint {
	var c pm.Response_Validated_Constraint
	switch {
	case p.IsPrimaryKey:
		c.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		c.Reason = "Primary key locations are required"
	case p.IsRootDocumentProjection():
		c.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		c.Reason = "The root document is required for a standard updates materialization"
	case p.Field == "_meta/op":
		c.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		c.Reason = "The operation type should usually be materialized"
	case strings.HasPrefix(p.Field, "_meta/"):
		c.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		c.Reason = "Metadata fields are able to be materialized"
	case p.Inference.IsSingleType() && slices.Contains(p.Inference.Types, "object"):
		c.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		c.Reason = "Object fields may be materialized"
	default:
		c.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		c.Reason = "This field is able to be materialized"
	}
	return c
}

func (d *materialization) MapType(p boilerplate.Projection, fc fieldConfig) (mappedType, boilerplate.ElementConverter) {
	return mapType(p), nil
}

func (d *materialization) Setup(ctx context.Context, is *boilerplate.InfoSchema) (string, error) {
	return "", nil
}

func (d *materialization) CreateNamespace(ctx context.Context, ns string) (string, error) {
	return "", nil
}

func (d *materialization) CreateResource(ctx context.Context, res boilerplate.MappedBinding[config, resource, mappedType]) (string, boilerplate.ActionApplyFn, error) {
	tableName := res.ResourcePath[0]
	return fmt.Sprintf("create table %q", tableName), func(ctx context.Context) error {
		// MaxVersions(2) keeps the last committed cell plus an in-flight cell
		// from a crashed attempt, so the dirty-read detection in Load can fall
		// back to the safe version.
		conf := &bigtable.TableConf{
			TableID: tableName,
			ColumnFamilies: map[string]bigtable.Family{
				columnFamily: {GCPolicy: bigtable.MaxVersionsGCPolicy(2)},
			},
		}
		if err := d.adminClient.CreateTableFromConf(ctx, conf); err != nil {
			return fmt.Errorf("create table %s: %w", tableName, err)
		}

		return nil
	}, nil
}

func (d *materialization) DeleteResource(ctx context.Context, resourcePath []string) (string, boilerplate.ActionApplyFn, error) {
	tableName := resourcePath[0]
	return fmt.Sprintf("delete table %q", tableName), func(ctx context.Context) error {
		if err := d.adminClient.DeleteTable(ctx, tableName); err != nil {
			return fmt.Errorf("delete table %s: %w", tableName, err)
		}

		return nil
	}, nil
}

func (d *materialization) TruncateResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	panic("not supported — NoTruncateResources is set")
}

func (d *materialization) MustRecreateResource(req *pm.Request_Apply, lastBinding, newBinding *pf.MaterializationSpec_Binding) (bool, error) {
	return false, nil
}

func (d *materialization) UpdateResource(
	ctx context.Context,
	resourcePath []string,
	existing boilerplate.ExistingResource,
	update boilerplate.BindingUpdate[config, resource, mappedType],
) (string, boilerplate.ActionApplyFn, error) {
	return "", nil, nil
}

func (d *materialization) NewTransactor(
	ctx context.Context,
	req pm.Request_Open,
	is boilerplate.InfoSchema,
	mappedBindings []boilerplate.MappedBinding[config, resource, mappedType],
	be *m.BindingEvents,
) (m.Transactor, error) {
	bindings := make([]binding, 0, len(mappedBindings))
	for _, mb := range mappedBindings {
		b := binding{
			tableName: mb.ResourcePath[0],
			table:     d.dataClient.Open(mb.ResourcePath[0]),
			docField:  mb.FieldSelection.Document, // Usually "flow_document", but can be projected to a different field name
		}
		for _, p := range mb.SelectedProjections() {
			if !p.IsRootDocumentProjection() {
				b.fields = append(b.fields, p.Mapped)
			}
		}
		bindings = append(bindings, b)
	}

	return &transactor{bindings: bindings, hardDelete: d.cfg.HardDelete}, nil
}

func (d *materialization) ListTestTasks(ctx context.Context) ([]string, error)        { return nil, nil }
func (d *materialization) CleanupTestTask(ctx context.Context, taskName string) error { return nil }

func (d *materialization) SnapshotTestResource(ctx context.Context, path []string) ([]string, [][]any, error) {
	tableName := path[0]
	tbl := d.dataClient.Open(tableName)

	// Read latest 1 version of every cell — snapshots reflect committed state.
	var rows []bigtable.Row
	if err := tbl.ReadRows(ctx, bigtable.InfiniteRange(""), func(r bigtable.Row) bool {
		rows = append(rows, r)
		return true
	}, bigtable.RowFilter(bigtable.LatestNFilter(1))); err != nil {
		return nil, nil, fmt.Errorf("scanning table %q: %w", tableName, err)
	}

	// columnQualifier strips the column-family prefix from a "family:qualifier"
	// string returned by Bigtable.
	columnQualifier := func(c string) string {
		if i := strings.IndexByte(c, ':'); i >= 0 {
			return c[i+1:]
		}

		return c
	}

	// renderBytes converts a raw cell value into a snapshot-friendly form to
	// keep test snapshots legible — without it, Go's JSON encoder would render
	// every []byte as base64.
	//
	//   - empty cells become null (a 0-byte cell is how we encode null).
	//   - JSON objects/arrays surface as json.RawMessage so they inline. This
	//     covers the document column (which may be projected under any name) and
	//     JSON-typed fields.
	//   - cells that are valid printable UTF-8 render as a string.
	//   - everything else (8-byte BE ints, packed FDB tuples, etc.) renders as
	//     `0x...` hex — deterministic and easier to eyeball than base64.
	renderBytes := func(value []byte) any {
		if len(value) == 0 {
			return nil
		}
		if (value[0] == '{' || value[0] == '[') && json.Valid(value) {
			return json.RawMessage(value)
		}
		if utf8.Valid(value) {
			printable := true
			for _, r := range string(value) {
				if !unicode.IsPrint(r) && !unicode.IsSpace(r) {
					printable = false
					break
				}
			}
			if printable {
				return string(value)
			}
		}

		return "0x" + hex.EncodeToString(value)
	}

	colSet := map[string]struct{}{"_row_key": {}}
	for _, r := range rows {
		for _, items := range r {
			for _, it := range items {
				colSet[columnQualifier(it.Column)] = struct{}{}
			}
		}
	}
	columns := make([]string, 0, len(colSet))
	for c := range colSet {
		columns = append(columns, c)
	}
	sort.Strings(columns)

	out := make([][]any, 0, len(rows))
	for _, r := range rows {
		row := make([]any, len(columns))
		for i, c := range columns {
			if c == "_row_key" {
				row[i] = renderBytes([]byte(r.Key()))
				continue
			}
			for _, items := range r {
				for _, it := range items {
					if columnQualifier(it.Column) == c {
						row[i] = renderBytes(it.Value)
					}
				}
			}
		}
		out = append(out, row)
	}

	sort.Slice(out, func(i, j int) bool {
		for c := range columns {
			vi := fmt.Sprint(out[i][c])
			vj := fmt.Sprint(out[j][c])
			if vi != vj {
				return vi < vj
			}
		}
		return false
	})

	return columns, out, nil
}

func (d *materialization) Close(ctx context.Context) {
	d.dataClient.Close()
	d.adminClient.Close()
}

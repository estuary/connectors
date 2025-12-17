package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/estuary/connectors/go/auth/iam"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/invopop/jsonschema"
)

var featureFlagDefaults = map[string]bool{
	"retain_existing_data_on_backfill": false,
}

type AuthType string

const (
	AWSAccessKey AuthType = "AWSAccessKey"
	AWSIAM       AuthType = "AWSIAM"
)

type CredentialsConfig struct {
	AuthType AuthType `json:"auth_type"`

	AccessKeyCredentials
	iam.IAMConfig
}

func (CredentialsConfig) JSONSchema() *jsonschema.Schema {
	subSchemas := []schemagen.OneOfSubSchemaT{
		schemagen.OneOfSubSchema("Access Key", AccessKeyCredentials{}, string(AWSAccessKey)),
	}
	subSchemas = append(subSchemas,
		schemagen.OneOfSubSchema("AWS IAM", iam.AWSConfig{}, string(AWSIAM)))

	return schemagen.OneOfSchema("Authentication", "", "auth_type", string(AWSAccessKey), subSchemas...)
}

func (c *CredentialsConfig) Validate() error {
	switch c.AuthType {
	case AWSAccessKey:
		if c.AWSAccessKeyID == "" {
			return errors.New("missing 'aws_access_key_id'")
		}
		if c.AWSSecretAccessKey == "" {
			return errors.New("missing 'aws_secret_access_key'")
		}
		return nil
	case AWSIAM:
		if err := c.ValidateIAM(); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("unknown 'auth_type'")
}

// Since the AccessKeyCredentials and IAMConfig have conflicting JSON field names, only parse
// the embedded struct of interest.
func (c *CredentialsConfig) UnmarshalJSON(data []byte) error {
	var discriminator struct {
		AuthType AuthType `json:"auth_type"`
	}
	if err := json.Unmarshal(data, &discriminator); err != nil {
		return err
	}
	c.AuthType = discriminator.AuthType

	switch c.AuthType {
	case AWSAccessKey:
		return json.Unmarshal(data, &c.AccessKeyCredentials)
	case AWSIAM:
		return json.Unmarshal(data, &c.IAMConfig)
	}
	return fmt.Errorf("unexpected auth_type: %s", c.AuthType)
}

type AccessKeyCredentials struct {
	AWSAccessKeyID     string `json:"aws_access_key_id" jsonschema:"title=AWS Access Key ID,description=AWS Access Key ID for capturing from the DynamoDB table." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"aws_secret_access_key" jsonschema:"title=AWS Secret Access key,description=AWS Access Key ID for capturing from the DynamoDB table." jsonschema_extras:"secret=true,order=2"`
}

type config struct {
	AWSAccessKeyID     string             `json:"awsAccessKeyId" jsonschema:"-"`
	AWSSecretAccessKey string             `json:"awsSecretAccessKey" jsonschema:"-" jsonschema_extras:"secret=true"`
	Region             string             `json:"region" jsonschema:"title=Region,description=Region of the materialized tables." jsonschema_extras:"order=1"`
	Credentials        *CredentialsConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"x-iam-auth=true,order=2"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	Endpoint     string `json:"endpoint,omitempty" jsonschema:"title=AWS Endpoint,description=The AWS endpoint URI to connect to. Use if you're materializing to a compatible API that isn't provided by AWS."`
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

func (c config) Validate() error {
	var requiredProperties = [][]string{
		{"region", c.Region},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.Credentials == nil {
		if c.AWSAccessKeyID == "" {
			return errors.New("missing 'awsAccessKeyId'")
		}
		if c.AWSSecretAccessKey == "" {
			return errors.New("missing 'awsSecretAccessKey'")
		}
	} else if err := c.Credentials.Validate(); err != nil {
		return err
	}

	return nil
}

func (c config) DefaultNamespace() string {
	return ""
}

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

func (c *config) CredentialsProvider(ctx context.Context) (aws.CredentialsProvider, error) {
	if c.Credentials == nil {
		return credentials.NewStaticCredentialsProvider(c.AWSAccessKeyID, c.AWSSecretAccessKey, ""), nil
	}

	switch c.Credentials.AuthType {
	case AWSAccessKey:
		return credentials.NewStaticCredentialsProvider(
			c.Credentials.AWSAccessKeyID, c.Credentials.AWSSecretAccessKey, ""), nil
	case AWSIAM:
		return c.Credentials.IAMTokens.AWSCredentialsProvider()
	}
	return nil, errors.New("unknown 'auth_type'")
}

type resource struct {
	Table string `json:"table" jsonschema:"title=Table Name,description=The name of the table to be materialized to." jsonschema_extras:"x-collection-name=true"`
}

func (r resource) Validate() error {
	var requiredProperties = [][]string{
		{"table", r.Table},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if _, err := normalizeTableName(r.Table); err != nil {
		return err
	}

	return nil
}

func (r resource) WithDefaults(cfg config) resource {
	return r
}

func (r resource) Parameters() ([]string, bool, error) {
	tableName, err := normalizeTableName(r.Table)
	if err != nil {
		return nil, false, err
	}

	return []string{tableName}, false, nil
}

var (
	tableNameSanitizer = regexp.MustCompile(`[^\.\-_0-9a-zA-Z]`)
	maxTableNameLength = 255
	minTableNameLength = 3
)

// For table naming requirements, see
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.NamingRules
func normalizeTableName(t string) (string, error) {
	cleaned := tableNameSanitizer.ReplaceAllString(t, "_")

	// The cleaned string is now guaranteed to be only ASCII characters.
	if len(cleaned) < minTableNameLength {
		return "", fmt.Errorf(
			"table name '%s' is invalid: must contain at least %d alphanumeric, dash ('-'), dot ('.'), or underscore ('_') characters",
			t,
			minTableNameLength,
		)
	}

	if len(cleaned) > maxTableNameLength {
		cleaned = cleaned[:maxTableNameLength]
	}

	return cleaned, nil
}

func (c *config) client(ctx context.Context) (*client, error) {
	credProvider, err := c.CredentialsProvider(ctx)
	if err != nil {
		return nil, err
	}

	opts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithCredentialsProvider(credProvider),
		awsConfig.WithRegion(c.Region),
		awsConfig.WithRetryer(func() aws.Retryer {
			// Bump up the number of retry maximum attempts from the default of 3. The maximum retry
			// duration is 20 seconds, so this gives us around 5 minutes of retrying retryable
			// errors before giving up and crashing the connector.
			//
			// Ref: https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/retries-timeouts/
			return retry.AddWithMaxAttempts(retry.NewStandard(), 20)
		}),
	}

	if c.Advanced.Endpoint != "" {
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: c.Advanced.Endpoint}, nil
		})

		opts = append(opts, awsConfig.WithEndpointResolverWithOptions(customResolver))
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating aws config: %w", err)
	}

	return &client{db: dynamodb.NewFromConfig(awsCfg)}, nil
}

type client struct {
	db *dynamodb.Client
}

func Driver() driver {
	return driver{}
}

type driver struct{}

var _ boilerplate.Connector = &driver{}

func (d driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	endpointSchema, err := schemagen.GenerateSchema("Materialize DynamoDB Spec", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("DynamoDB Table", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return boilerplate.RunSpec(ctx, req, "https://go.estuary.dev/materialize-dynamodb", endpointSchema, resourceSchema)
}

func (d driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	for _, b := range req.Bindings {
		// The primary key for a DynamoDB table is the partition key, and an optional sort key. For
		// now we only support materializing collections with at most 2 collection keys, to map to
		// this table structure.
		if len(b.Collection.Key) > 2 {
			return nil, fmt.Errorf(
				"cannot materialize collection '%s' because it has more than 2 keys (has %d keys)",
				b.Collection.Name.String(),
				len(b.Collection.Key),
			)
		}
	}

	return boilerplate.RunValidate(ctx, req, newMaterialization)
}

func (d driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return boilerplate.RunApply(ctx, req, newMaterialization)
}

func (d driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, newMaterialization)
}

type materialization struct {
	cfg    config
	client *client
}

var _ boilerplate.Materializer[config, fieldConfig, resource, mappedType] = &materialization{}

func newMaterialization(ctx context.Context, materializationName string, cfg config, featureFlags map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, mappedType], error) {
	client, err := cfg.client(ctx)
	if err != nil {
		return nil, err
	}

	return &materialization{
		cfg:    cfg,
		client: client,
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

func (d *materialization) PopulateInfoSchema(ctx context.Context, resourcePaths [][]string, is *boilerplate.InfoSchema) error {
	for _, p := range resourcePaths {
		tableName := p[0]

		d, err := d.client.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		if err != nil {
			var errNotFound *types.ResourceNotFoundException
			if errors.As(err, &errNotFound) {
				// Table hasn't been created yet.
				continue
			}
			return fmt.Errorf("describing table %q: %w", tableName, err)
		}

		res := is.PushResource(tableName)
		for _, def := range d.Table.AttributeDefinitions {
			res.PushField(boilerplate.ExistingField{
				Name:               *def.AttributeName,
				Nullable:           false,                     // Table keys can never be nullable.
				Type:               string(def.AttributeType), // "B", "S", or "N".
				CharacterMaxLength: 0,
			})
		}
	}

	return nil
}

func (d *materialization) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	return nil
}

func (d *materialization) NewConstraint(p pf.Projection, deltaUpdates bool, fc fieldConfig) pm.Response_Validated_Constraint {
	// By default only the collection key and root document fields are materialized, due to
	// DynamoDB's 400kb single item size limit. Additional fields are optional and may be selected
	// to materialize as top-level properties with the applicable conversion applied, if desired.
	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "Primary key locations are required"
	case p.IsRootDocumentProjection() && !deltaUpdates:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document is required for a standard updates materialization"
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	default:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "This field is able to be materialized"
	}

	return constraint
}

func (d *materialization) MapType(p boilerplate.Projection, fc fieldConfig) (mappedType, boilerplate.ElementConverter) {
	return mapType(p.Projection), nil
}

func (d *materialization) Setup(ctx context.Context, is *boilerplate.InfoSchema) (string, error) {
	return "", nil
}

func (d *materialization) CreateNamespace(ctx context.Context, ns string) (string, error) {
	return "", nil
}

func (d *materialization) CreateResource(ctx context.Context, res boilerplate.MappedBinding[config, resource, mappedType]) (string, boilerplate.ActionApplyFn, error) {
	tableName := res.ResourcePath[0]
	attrs, schema := tableConfigFromProjections(res.Keys)

	return fmt.Sprintf("create table %q", tableName), func(ctx context.Context) error {
		return createTable(ctx, d.client, tableName, attrs, schema)
	}, nil
}

func (d *materialization) DeleteResource(ctx context.Context, resourcePath []string) (string, boilerplate.ActionApplyFn, error) {
	return fmt.Sprintf("delete table %q", resourcePath[0]), func(ctx context.Context) error {
		return deleteTable(ctx, d.client, resourcePath[0])
	}, nil
}

func (d *materialization) TruncateResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	panic("not supported")
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
	// No-op since DynamoDB only applies a schema to the key columns, and Flow doesn't allow you to
	// change the key of an established collection, and the Validation constraints don't allow
	// changing the type of a key field in a way that would change its materialized type.
	return "", nil, nil
}

func (d *materialization) NewMaterializerTransactor(
	ctx context.Context,
	req pm.Request_Open,
	is boilerplate.InfoSchema,
	mappedBindings []boilerplate.MappedBinding[config, resource, mappedType],
	be *m.BindingEvents,
) (boilerplate.MaterializerTransactor, error) {
	var bindings []binding
	tablesToBindings := make(map[string]int)
	for idx, b := range mappedBindings {
		mappedFields := make([]mappedType, 0, len(b.FieldSelection.AllFields()))
		for _, p := range b.SelectedProjections() {
			mappedFields = append(mappedFields, p.Mapped)
		}

		tablesToBindings[b.ResourcePath[0]] = idx
		bindings = append(bindings, binding{
			tableName: b.ResourcePath[0],
			fields:    mappedFields,
			docField:  b.Document.Field,
		})
	}

	return &transactor{
		client:           d.client,
		bindings:         bindings,
		tablesToBindings: tablesToBindings,
	}, nil
}

func (d *materialization) Close(ctx context.Context) {}

func tableConfigFromProjections(keys []boilerplate.MappedProjection[mappedType]) ([]types.AttributeDefinition, []types.KeySchemaElement) {
	// The collection keys will be used as the partition key and sort key, respectively.
	keyTypes := [2]types.KeyType{types.KeyTypeHash, types.KeyTypeRange}

	attrs := []types.AttributeDefinition{}
	schema := []types.KeySchemaElement{}

	for idx, k := range keys {
		attrs = append(attrs, types.AttributeDefinition{
			AttributeName: aws.String(k.Field),
			AttributeType: k.Mapped.ddbScalarType,
		})
		schema = append(schema, types.KeySchemaElement{
			AttributeName: aws.String(k.Field),
			KeyType:       keyTypes[idx],
		})
	}

	return attrs, schema
}

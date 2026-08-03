package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	awsiam "github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/estuary/connectors/go/auth/iam"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
)

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
	return schemagen.OneOfSchema("Authentication", "", "auth_type", string(AWSAccessKey),
		schemagen.OneOfSubSchema("Access Key", AccessKeyCredentials{}, string(AWSAccessKey)),
		schemagen.OneOfSubSchema("AWS IAM", iam.AWSConfig{}, string(AWSIAM)),
	)
}

func (c *CredentialsConfig) Validate() error {
	switch c.AuthType {
	case AWSAccessKey:
		if c.AccessKeyCredentials.AWSAccessKeyID == "" {
			return fmt.Errorf("missing %q", "aws_access_key_id")
		}
		if c.AccessKeyCredentials.AWSSecretAccessKey == "" {
			return fmt.Errorf("missing %q", "aws_secret_access_key")
		}
		return nil
	case AWSIAM:
		return c.ValidateIAM()
	}
	return fmt.Errorf("unknown %q: %s", "auth_type", c.AuthType)
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
	return fmt.Errorf("unknown %q: %s", "auth_type", c.AuthType)
}

type AccessKeyCredentials struct {
	AWSAccessKeyID     string `json:"aws_access_key_id" jsonschema:"title=AWS Access Key ID,description=AWS Access Key ID for publishing to EventBridge." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"aws_secret_access_key" jsonschema:"title=AWS Secret Access Key,description=AWS Secret Access Key for publishing to EventBridge." jsonschema_extras:"secret=true,order=2"`
}

type config struct {
	Region       string             `json:"region" jsonschema:"title=AWS Region,description=Region of the EventBridge event bus." jsonschema_extras:"order=1"`
	EventBusName string             `json:"event_bus_name" jsonschema:"title=Event Bus Name,description=Name or ARN of the EventBridge event bus to publish to; verified via DescribeEventBus on Apply. Use \"default\" for the account's default bus.,default=default" jsonschema_extras:"order=2"`
	Credentials  *CredentialsConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"x-iam-auth=true,order=3"`
	Advanced     advancedConfig     `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	Endpoint     string `json:"endpoint,omitempty" jsonschema:"title=AWS Endpoint,description=Override the AWS endpoint URL. Used to direct requests at a compatible API such as LocalStack."`
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

func (c config) Validate() error {
	if c.Region == "" {
		return fmt.Errorf("missing %q", "region")
	}
	if c.EventBusName == "" {
		return fmt.Errorf("missing %q", "event_bus_name")
	}
	if c.Credentials == nil {
		return fmt.Errorf("missing %q", "credentials")
	}
	return c.Credentials.Validate()
}

// EventBridge has no namespace concept; the bus name is endpoint-level.
func (c config) DefaultNamespace() string { return "" }

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, nil
}

func (c config) credentialsProvider() (aws.CredentialsProvider, error) {
	if c.Credentials == nil {
		return nil, errors.New("missing credentials")
	}
	switch c.Credentials.AuthType {
	case AWSAccessKey:
		return credentials.NewStaticCredentialsProvider(
			c.Credentials.AccessKeyCredentials.AWSAccessKeyID,
			c.Credentials.AccessKeyCredentials.AWSSecretAccessKey,
			"",
		), nil
	case AWSIAM:
		return c.Credentials.IAMTokens.AWSCredentialsProvider()
	}
	return nil, fmt.Errorf("unknown %q: %s", "auth_type", c.Credentials.AuthType)
}

type resource struct {
	Source     string `json:"source" jsonschema:"title=Event Source,description=Source field set on every event published from this binding,default=estuary.flow" jsonschema_extras:"x-collection-name=true,order=1"`
	DetailType string `json:"detail_type" jsonschema:"title=Detail Type,description=DetailType field set on every event published from this binding,default=Document Published" jsonschema_extras:"order=2"`

	// busName is populated from the endpoint config via WithDefaults so that
	// Parameters() can produce the full [bus, source, detail_type] resource
	// path. It is not serialized.
	busName string `json:"-"`
}

func (r resource) Validate() error {
	if r.Source == "" {
		return fmt.Errorf("missing %q", "source")
	}
	if r.DetailType == "" {
		return fmt.Errorf("missing %q", "detail_type")
	}
	return nil
}

func (r resource) WithDefaults(cfg config) resource {
	r.busName = cfg.EventBusName
	return r
}

func (r resource) Parameters() ([]string, bool, error) {
	return []string{r.busName, r.Source, r.DetailType}, true, nil
}

// fieldConfig and mappedType are required by the Materializer generic
// signature but carry no destination-specific information for EventBridge —
// every document is published as an opaque JSON payload, so there is no
// per-field schema to track.

type fieldConfig struct{}

func (fieldConfig) Validate() error    { return nil }
func (fieldConfig) CastToString() bool { return false }

type mappedType struct{}

func (mappedType) String() string                            { return "json" }
func (mappedType) Compatible(boilerplate.ExistingField) bool { return true }
func (mappedType) CanMigrate(boilerplate.ExistingField) bool { return false }

type driver struct{}

var _ boilerplate.Connector = &driver{}

func (driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	endpointSchema, err := schemagen.GenerateSchema("Materialize EventBridge Spec", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("EventBridge Binding", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return boilerplate.RunSpec(ctx, req, "https://go.estuary.dev/materialize-eventbridge", endpointSchema, resourceSchema)
}

func (driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	return boilerplate.RunValidate(ctx, req, newMaterialization)
}

func (driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return boilerplate.RunApply(ctx, req, newMaterialization)
}

func (driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, newMaterialization)
}

// awsConfigFor builds an aws.Config from the connector's authentication
// config (static keys or assumed-role STS tokens) and region, applying the
// optional endpoint override (used by tests against LocalStack).
func awsConfigFor(ctx context.Context, cfg config) (aws.Config, error) {
	credProvider, err := cfg.credentialsProvider()
	if err != nil {
		return aws.Config{}, err
	}

	opts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithCredentialsProvider(credProvider),
		awsConfig.WithRegion(cfg.Region),
	}

	if cfg.Advanced.Endpoint != "" {
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: cfg.Advanced.Endpoint, SigningRegion: region}, nil
		})
		opts = append(opts, awsConfig.WithEndpointResolverWithOptions(resolver))
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("creating aws config: %w", err)
	}
	return awsCfg, nil
}

type materialization struct {
	cfg    config
	client *eventbridge.Client
}

var _ boilerplate.Materializer[config, fieldConfig, resource, mappedType] = &materialization{}

func newMaterialization(ctx context.Context, materializationName string, cfg config, featureFlags map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, mappedType], error) {
	awsCfg, err := awsConfigFor(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &materialization{
		cfg:    cfg,
		client: eventbridge.NewFromConfig(awsCfg),
	}, nil
}

func (d *materialization) Config() boilerplate.MaterializeCfg {
	return boilerplate.MaterializeCfg{
		ConcurrentApply:     true,
		NoCreateNamespaces:  true,
		NoTruncateResources: true,
	}
}

func (d *materialization) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	for _, p := range resourcePaths {
		is.PushResource(p...)
	}
	return nil
}

func (d *materialization) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	prereqs := &cerrors.PrereqErr{}
	busARN, err := describeBus(ctx, d.client, d.cfg.EventBusName)
	if err != nil {
		prereqs.Err(err)
		return prereqs
	}
	checkPutEventsPermission(ctx, d.cfg, busARN, prereqs)
	return prereqs
}

func (d *materialization) NewConstraint(p pf.Projection, deltaUpdates bool, fc fieldConfig) pm.Response_Validated_Constraint {
	if !deltaUpdates {
		// EventBridge is delta-only and Parameters() coerces deltaUpdates=true
		// upstream, so the false branch should be unreachable. Panic if it ever
		// fires so a regression in Parameters() (or in the boilerplate plumbing)
		// surfaces immediately rather than producing silently-wrong constraints.
		panic("NewConstraint called with deltaUpdates=false; Parameters() should have coerced to true")
	}

	var constraint pm.Response_Validated_Constraint
	switch {
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document must be materialized"
	case p.IsPrimaryKey:
		// EventBridge has no partition/order/dedup concept and the
		// connector never reads it.PackedKey or per-key projections, so
		// keys carry no meaning on the destination side. FIELD_FORBIDDEN
		// would be the most accurate signal, but the Flow runtime rejects
		// FORBIDDEN on collection-key fields (they back the materialization
		// group-by). OPTIONAL is the closest we can go.
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "EventBridge ignores keys; including them in the payload is optional"
	default:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "EventBridge only materializes the full document"
	}
	return constraint
}

func (d *materialization) MapType(p boilerplate.Projection, fc fieldConfig) (mappedType, boilerplate.ElementConverter) {
	return mappedType{}, nil
}

func (d *materialization) Setup(ctx context.Context, is *boilerplate.InfoSchema) (string, error) {
	return "", nil
}

func (d *materialization) CreateNamespace(ctx context.Context, ns string) (string, error) {
	return "", nil
}

func (d *materialization) CreateResource(ctx context.Context, b boilerplate.MappedBinding[config, resource, mappedType]) (string, boilerplate.ActionApplyFn, error) {
	return "", nil, nil
}

func (d *materialization) DeleteResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	// Test cleanup paths invoke the returned fn directly without nil-checking,
	// so we return a no-op fn rather than nil.
	return "", func(context.Context) error { return nil }, nil
}

func (d *materialization) UpdateResource(
	ctx context.Context,
	path []string,
	existing boilerplate.ExistingResource,
	update boilerplate.BindingUpdate[config, resource, mappedType],
) (string, boilerplate.ActionApplyFn, error) {
	return "", nil, nil
}

func (d *materialization) TruncateResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	// Unreachable: Config() sets NoTruncateResources=true, so boilerplate
	// never invokes this. Kept to satisfy the Materializer interface.
	panic("TruncateResource called despite NoTruncateResources=true")
}

func (d *materialization) MustRecreateResource(req *pm.Request_Apply, lastBinding, newBinding *pf.MaterializationSpec_Binding) (bool, error) {
	return false, nil
}

func (d *materialization) NewTransactor(
	ctx context.Context,
	req pm.Request_Open,
	is boilerplate.InfoSchema,
	mappedBindings []boilerplate.MappedBinding[config, resource, mappedType],
	be *m.BindingEvents,
) (m.Transactor, error) {
	bindings := make([]bindingState, 0, len(mappedBindings))
	for _, b := range mappedBindings {
		bindings = append(bindings, bindingState{
			source:     b.Config.Source,
			detailType: b.Config.DetailType,
		})
	}

	return &transactor{
		client:       d.client,
		eventBusName: d.cfg.EventBusName,
		bindings:     bindings,
	}, nil
}

func (d *materialization) ListTestTasks(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (d *materialization) CleanupTestTask(ctx context.Context, name string) error {
	return nil
}

// SnapshotTestResource is required by boilerplate.Materializer but is only
// invoked by the test rig (bptest.RunMaterializationTestParallel). The
// production runtime never calls it. The panic is a safety net: if a test ever
// forgets to wrap the materialization in materializationUnderTest, we want a
// loud failure rather than a silent empty snapshot that would mask the bug.
func (d *materialization) SnapshotTestResource(ctx context.Context, path []string) ([]string, [][]any, error) {
	panic("SnapshotTestResource is unreachable in production; tests must wrap the materialization in materializationUnderTest")
}

func (d *materialization) Close(ctx context.Context) {}

func describeBus(ctx context.Context, client *eventbridge.Client, name string) (string, error) {
	out, err := client.DescribeEventBus(ctx, &eventbridge.DescribeEventBusInput{Name: &name})
	if err != nil {
		return "", fmt.Errorf("describing event bus %q: %w", name, err)
	}
	return aws.ToString(out.Arn), nil
}

func checkPutEventsPermission(ctx context.Context, cfg config, busARN string, prereqs *cerrors.PrereqErr) {
	awsCfg, err := awsConfigFor(ctx, cfg)
	if err != nil {
		log.WithField("error", err).Warn("could not build AWS config for permission check; skipping")
		return
	}

	callerOut, err := sts.NewFromConfig(awsCfg).GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		log.WithField("error", err).Warn("GetCallerIdentity failed; skipping write-permission check")
		return
	}

	principalARN := iamPrincipalARN(aws.ToString(callerOut.Arn))
	simOut, err := awsiam.NewFromConfig(awsCfg).SimulatePrincipalPolicy(ctx, &awsiam.SimulatePrincipalPolicyInput{
		PolicySourceArn: &principalARN,
		ActionNames:     []string{"events:PutEvents"},
		ResourceArns:    []string{busARN},
	})
	if err != nil {
		log.WithField("error", err).Warn("SimulatePrincipalPolicy failed; skipping write-permission check")
		return
	}

	for _, result := range simOut.EvaluationResults {
		if result.EvalDecision != iamtypes.PolicyEvaluationDecisionTypeAllowed {
			prereqs.Err(fmt.Errorf("principal %s does not have events:PutEvents permission on bus %s", principalARN, busARN))
			return
		}
	}
}

// iamPrincipalARN converts an STS assumed-role ARN to the IAM role ARN that
// SimulatePrincipalPolicy requires. Other ARN forms are returned unchanged.
//
// Example:
//
//	arn:aws:sts::123456789012:assumed-role/MyRole/session
//	→ arn:aws:iam::123456789012:role/MyRole
func iamPrincipalARN(arnStr string) string {
	// ARN structure: arn:partition:service:region:account:resource
	parts := strings.SplitN(arnStr, ":", 6)
	if len(parts) != 6 {
		return arnStr
	}
	resource := parts[5]
	if !strings.HasPrefix(resource, "assumed-role/") {
		return arnStr
	}
	// resource is "assumed-role/RoleName/SessionName"
	roleParts := strings.SplitN(resource, "/", 3)
	if len(roleParts) < 2 {
		return arnStr
	}
	return fmt.Sprintf("arn:%s:iam::%s:role/%s", parts[1], parts[4], roleParts[1])
}

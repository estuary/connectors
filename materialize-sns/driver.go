package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/estuary/connectors/go/auth/iam"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/invopop/jsonschema"
)

type AuthType string

const (
	AWSAccessKey AuthType = "AWSAccessKey"
	AWSIAM       AuthType = "AWSIAM"
)

type AccessKeyCredentials struct {
	AWSAccessKeyID     string `json:"aws_access_key_id" jsonschema:"title=AWS Access Key ID,description=AWS Access Key ID for publishing to the SNS topics." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"aws_secret_access_key" jsonschema:"title=AWS Secret Access Key,description=AWS Secret Access Key for publishing to the SNS topics." jsonschema_extras:"secret=true,order=2"`
}

type CredentialsConfig struct {
	AuthType AuthType `json:"auth_type"`

	AccessKeyCredentials
	iam.IAMConfig
}

func (CredentialsConfig) JSONSchema() *jsonschema.Schema {
	subSchemas := []schemagen.OneOfSubSchemaT{
		schemagen.OneOfSubSchema("Access Key", AccessKeyCredentials{}, string(AWSAccessKey)),
		schemagen.OneOfSubSchema("AWS IAM", iam.AWSConfig{}, string(AWSIAM)),
	}
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
		return c.ValidateIAM()
	}
	return fmt.Errorf("unknown 'auth_type'")
}

// AccessKeyCredentials and IAMConfig share field names in their embedded form, so we discriminate
// at decode time and parse only the variant matching auth_type.
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

type advancedConfig struct {
	Endpoint string `json:"endpoint,omitempty" jsonschema:"title=AWS Endpoint,description=Override the SNS endpoint URL."`
}

type config struct {
	Region      string             `json:"region" jsonschema:"title=AWS Region,description=Region of the SNS topics this materialization publishes to." jsonschema_extras:"order=1"`
	Credentials *CredentialsConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"x-iam-auth=true,order=2"`
	Advanced    advancedConfig     `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

func (c config) Validate() error {
	if c.Region == "" {
		return fmt.Errorf("missing 'region'")
	}
	if c.Credentials == nil {
		return fmt.Errorf("missing 'credentials'")
	}
	if err := c.Credentials.Validate(); err != nil {
		return err
	}
	// When IAM auth is configured, the IAM 'aws_region' (used to scope AssumeRole) must match the
	// top-level SNS 'region' (used by the SNS client). Mismatched values produce confusing
	// downstream API errors, so reject them up front.
	if c.Credentials.AuthType == AWSIAM && c.Credentials.AWSRegion != c.Region {
		return fmt.Errorf(
			"'credentials.aws_region' (%q) does not match top-level 'region' (%q); both must refer to the same AWS region",
			c.Credentials.AWSRegion, c.Region,
		)
	}
	return nil
}

func (c *config) credentialsProvider() (aws.CredentialsProvider, error) {
	switch c.Credentials.AuthType {
	case AWSAccessKey:
		return credentials.NewStaticCredentialsProvider(
			c.Credentials.AWSAccessKeyID, c.Credentials.AWSSecretAccessKey, ""), nil
	case AWSIAM:
		return c.Credentials.IAMTokens.AWSCredentialsProvider()
	}
	return nil, fmt.Errorf("unknown 'auth_type'")
}

func (c *config) awsConfig(ctx context.Context) (aws.Config, error) {
	credProvider, err := c.credentialsProvider()
	if err != nil {
		return aws.Config{}, err
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithCredentialsProvider(credProvider),
		awsConfig.WithRegion(c.Region),
		awsConfig.WithRetryer(func() aws.Retryer {
			return retry.AddWithMaxAttempts(retry.NewStandard(), 20)
		}),
	)
	if err != nil {
		return aws.Config{}, fmt.Errorf("creating AWS config: %w", err)
	}
	return awsCfg, nil
}

func (c *config) snsClient(awsCfg aws.Config) *sns.Client {
	var opts []func(*sns.Options)
	if c.Advanced.Endpoint != "" {
		ep := c.Advanced.Endpoint
		opts = append(opts, func(o *sns.Options) { o.BaseEndpoint = &ep })
	}
	return sns.NewFromConfig(awsCfg, opts...)
}

func (c *config) stsClient(awsCfg aws.Config) *sts.Client {
	var opts []func(*sts.Options)
	if c.Advanced.Endpoint != "" {
		ep := c.Advanced.Endpoint
		opts = append(opts, func(o *sts.Options) { o.BaseEndpoint = &ep })
	}
	return sts.NewFromConfig(awsCfg, opts...)
}

// callerAccountID returns the AWS account ID of the configured credentials. We use this to
// pre-validate that user-supplied topic ARNs are in our own account: otherwise SNS happily
// creates a same-named topic in our account and the ARN-mismatch only surfaces afterwards,
// leaving an orphan topic behind.
func callerAccountID(ctx context.Context, c *sts.Client) (string, error) {
	out, err := c.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", fmt.Errorf("identifying AWS caller via STS: %w", err)
	}
	if out.Account == nil {
		return "", fmt.Errorf("STS GetCallerIdentity returned no account ID")
	}
	return *out.Account, nil
}

type resource struct {
	TopicName string `json:"topic_name" jsonschema:"title=Topic Name,description=Name of the SNS topic to publish to (without the ARN prefix). FIFO topics must end in '.fifo'." jsonschema_extras:"x-collection-name=true"`
}

// topicNamePattern matches AWS's published rules for SNS topic names: up to 256 characters of
// alphanumeric / hyphen / underscore. FIFO topics additionally require a `.fifo` suffix, in which
// case the name portion before the suffix follows the same rules and the whole name still fits in
// 256 characters.
var (
	topicNamePattern     = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,256}$`)
	fifoTopicNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,251}\.fifo$`)
)

func (r resource) Validate() error {
	if r.TopicName == "" {
		return fmt.Errorf("missing 'topic_name'")
	}
	if strings.HasSuffix(r.TopicName, ".fifo") {
		if !fifoTopicNamePattern.MatchString(r.TopicName) {
			return fmt.Errorf(
				"invalid topic_name %q: must be 1-256 alphanumeric / '-' / '_' characters",
				r.TopicName,
			)
		}
		return nil
	}
	if !topicNamePattern.MatchString(r.TopicName) {
		return fmt.Errorf(
			"invalid topic_name %q: must be 1-256 alphanumeric / '-' / '_' characters",
			r.TopicName,
		)
	}
	return nil
}

func (r resource) IsFifo() bool {
	return strings.HasSuffix(r.TopicName, ".fifo")
}

// arn returns the full ARN for this topic in the given region and AWS account.
func (r resource) arn(region, account string) string {
	return fmt.Sprintf("arn:aws:sns:%s:%s:%s", region, account, r.TopicName)
}

func Driver() driver {
	return driver{}
}

type driver struct{}

var _ boilerplate.Connector = &driver{}

func (d driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	endpointSchema, err := schemagen.GenerateSchema("Materialize AWS SNS Spec", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("AWS SNS Topic", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-sns",
	}, nil
}

func (d driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	cfg, err := resolveEndpointConfig(req.ConfigJson)
	if err != nil {
		return nil, err
	}

	awsCfg, err := cfg.awsConfig(ctx)
	if err != nil {
		return nil, err
	}
	client := cfg.snsClient(awsCfg)
	account, err := callerAccountID(ctx, cfg.stsClient(awsCfg))
	if err != nil {
		return nil, err
	}

	var out []*pm.Response_Validated_Binding
	for _, b := range req.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, err
		}
		topicARN := res.arn(cfg.Region, account)

		var constraints []*pm.Response_Validated_ProjectionConstraint
		for _, projection := range b.Collection.Projections {
			var constraint = new(pm.Response_Validated_Constraint)
			switch {
			case projection.IsRootDocumentProjection():
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
				constraint.Reason = "The root document must be materialized"
			case projection.IsPrimaryKey:
				constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
			default:
				constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
				constraint.Reason = "SNS only materializes the full document"
			}
			constraints = append(constraints, &pm.Response_Validated_ProjectionConstraint{
				Field:      projection.Field,
				Constraint: constraint,
			})
		}

		// Surface authentication / region errors up front, but ignore NotFound — Apply() will
		// create missing topics.
		if _, err := client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
			TopicArn: aws.String(topicARN),
		}); err != nil {
			var notFound *snstypes.NotFoundException
			if !errors.As(err, &notFound) {
				return nil, fmt.Errorf("checking SNS topic %q: %w", topicARN, err)
			}
		}

		out = append(out, &pm.Response_Validated_Binding{
			CaseInsensitiveFields: false,
			ProjectionConstraints: constraints,
			DeltaUpdates:          true,
			ResourcePath:          []string{res.TopicName},
		})
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

func (d driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	cfg, err := resolveEndpointConfig(req.Materialization.ConfigJson)
	if err != nil {
		return nil, err
	}

	awsCfg, err := cfg.awsConfig(ctx)
	if err != nil {
		return nil, err
	}
	client := cfg.snsClient(awsCfg)
	account, err := callerAccountID(ctx, cfg.stsClient(awsCfg))
	if err != nil {
		return nil, err
	}

	var actions []string
	checked := make(map[string]struct{})
	for _, b := range req.Materialization.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, err
		}
		topicARN := res.arn(cfg.Region, account)
		if _, ok := checked[topicARN]; ok {
			continue
		}
		checked[topicARN] = struct{}{}

		_, err = client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
			TopicArn: aws.String(topicARN),
		})
		if err == nil {
			continue
		}
		var notFound *snstypes.NotFoundException
		if !errors.As(err, &notFound) {
			return nil, fmt.Errorf("checking SNS topic %q: %w", topicARN, err)
		}

		input := &sns.CreateTopicInput{Name: aws.String(res.TopicName)}
		if res.IsFifo() {
			// ContentBasedDeduplication is explicitly disabled: the transactor always sets an
			// explicit MessageDeduplicationId derived from (packedKey, document), so the
			// content-based fallback is unwanted. Leaving it on would silently mask a bug where
			// we forget to set MessageDeduplicationId.
			input.Attributes = map[string]string{
				"FifoTopic":                 "true",
				"ContentBasedDeduplication": "false",
			}
		}
		if _, err := client.CreateTopic(ctx, input); err != nil {
			return nil, fmt.Errorf("creating SNS topic %q: %w", topicARN, err)
		}
		actions = append(actions, fmt.Sprintf("created topic %s", topicARN))
	}

	return &pm.Response_Applied{ActionDescription: strings.Join(actions, "\n")}, nil
}

func (d driver) NewTransactor(ctx context.Context, open pm.Request_Open, _ *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	cfg, err := resolveEndpointConfig(open.Materialization.ConfigJson)
	if err != nil {
		return nil, nil, nil, err
	}

	awsCfg, err := cfg.awsConfig(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	client := cfg.snsClient(awsCfg)
	account, err := callerAccountID(ctx, cfg.stsClient(awsCfg))
	if err != nil {
		return nil, nil, nil, err
	}

	var bindings []*topicBinding
	for _, b := range open.Materialization.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, nil, nil, err
		}
		bindings = append(bindings, &topicBinding{
			topicARN: res.arn(cfg.Region, account),
			isFifo:   res.IsFifo(),
		})
	}

	return &transactor{
		client:   client,
		bindings: bindings,
	}, &pm.Response_Opened{}, nil, nil
}

func resolveEndpointConfig(specJson json.RawMessage) (config, error) {
	var cfg = config{}
	if err := pf.UnmarshalStrict(specJson, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing SNS endpoint config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return cfg, fmt.Errorf("invalid SNS endpoint config: %w", err)
	}
	return cfg, nil
}

func resolveResourceConfig(specJson json.RawMessage) (resource, error) {
	var res = resource{}
	if err := pf.UnmarshalStrict(specJson, &res); err != nil {
		return res, fmt.Errorf("parsing SNS resource config: %w", err)
	}
	if err := res.Validate(); err != nil {
		return res, fmt.Errorf("invalid SNS resource config: %w", err)
	}
	return res, nil
}

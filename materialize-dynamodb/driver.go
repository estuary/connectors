package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-boilerplate/validate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

type config struct {
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=Access Key ID,description=AWS Access Key ID for materializing to DynamoDB." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=Secret Access Key,description=AWS Secret Access Key for materializing to DynamoDB." jsonschema_extras:"secret=true,order=2"`
	Region             string `json:"region" jsonschema:"title=Region,description=Region of the materialized tables." jsonschema_extras:"order=3"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type advancedConfig struct {
	Endpoint string `json:"endpoint,omitempty" jsonschema:"title=AWS Endpoint,description=The AWS endpoint URI to connect to. Use if you're materializing to a compatible API that isn't provided by AWS."`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"awsAccessKeyId", c.AWSAccessKeyID},
		{"awsSecretAccessKey", c.AWSSecretAccessKey},
		{"region", c.Region},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	return nil
}

type resource struct {
	Table        string `json:"table" jsonschema:"title=Table Name,description=The name of the table to be materialized to." jsonschema_extras:"x-collection-name=true"`
	DeltaUpdates bool   `json:"delta_updates,omitempty" jsonschema:"title=Delta updates,default=false"`
}

func (r *resource) Validate() error {
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
	opts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AWSAccessKeyID, c.AWSSecretAccessKey, ""),
		),
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
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	es := schemagen.GenerateSchema("Materialize DynamoDB Spec", &config{})
	endpointSchema, err := es.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("DynamoDB Table", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-dynamodb",
	}, nil
}

func (d driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	cfg, err := resolveEndpointConfig(req.ConfigJson)
	if err != nil {
		return nil, err
	}

	client, err := cfg.client(ctx)
	if err != nil {
		return nil, err
	}

	storedSpec, err := getSpec(ctx, client, req.Name.String())
	if err != nil && err != errMetaTableNotFound {
		return nil, err
	}

	var bindings = []*pm.Response_Validated_Binding{}
	for i, binding := range req.Bindings {
		res, err := resolveResourceConfig(binding.ResourceConfigJson)
		if err != nil {
			return nil, fmt.Errorf("building resource for binding %v: %w", i, err)
		}

		// The primary key for a DynamoDB table is the partition key, and an optional sort key. For
		// now we only support materializing collections with at most 2 collection keys, to map to
		// this table structure.
		if len(binding.Collection.Key) > 2 {
			return nil, fmt.Errorf(
				"cannot materialize collection '%s' because it has more than 2 keys (has %d keys)'",
				binding.Collection.Name.String(),
				len(binding.Collection.Key),
			)
		}

		tableName, err := normalizeTableName(res.Table)
		if err != nil {
			return nil, err
		}

		constraints, err := ddbValidator.ValidateBinding(
			[]string{tableName},
			res.DeltaUpdates,
			binding.Collection,
			storedSpec,
		)
		if err != nil {
			return nil, err
		}

		bindings = append(bindings, &pm.Response_Validated_Binding{
			Constraints:  constraints,
			ResourcePath: []string{tableName},
			DeltaUpdates: res.DeltaUpdates,
		})
	}
	var response = &pm.Response_Validated{Bindings: bindings}

	return response, nil
}

func (d driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	cfg, err := resolveEndpointConfig(req.Materialization.ConfigJson)
	if err != nil {
		return nil, err
	}

	client, err := cfg.client(ctx)
	if err != nil {
		return nil, err
	}

	actions := []string{}
	doAction := func(desc string, fn func() error) error {
		if !req.DryRun {
			actions = append(actions, "- "+desc)
			return fn()
		}
		actions = append(actions, "- "+desc+" (skipping due to dry-run)")
		return nil
	}

	storedSpec, err := getSpec(ctx, client, req.Materialization.Name.String())
	if err != nil {
		if err == errMetaTableNotFound {
			if err := doAction(fmt.Sprintf("create table '%s", metaTableName), func() error {
				return createTable(ctx, req.DryRun, client, metaTableName, metaTableAttrs, metaTableSchema)
			}); err != nil {
				return nil, fmt.Errorf("creating metadata table: %w", err)
			}
		} else {
			return nil, fmt.Errorf("getting stored spec: %w", err)
		}
	}

	for _, b := range req.Materialization.Bindings {
		if len(b.Collection.Key) > 2 {
			return nil, fmt.Errorf(
				"cannot materialize collection '%s' because it has more than 2 keys (has %d keys)'",
				b.Collection.Name.String(),
				len(b.Collection.Key),
			)
		}

		if err := ddbValidator.ValidateSelectedFields(b, storedSpec); err != nil {
			return nil, err
		}

		found, err := validate.FindExistingBinding(b.ResourcePath, b.Collection.Name, storedSpec)
		if err != nil {
			return nil, err
		}

		tableName := b.ResourcePath[0]
		if found == nil {
			// This table may not have been created yet, so create it now.
			attrs, schema := tableConfigFromBinding(b.Collection.Projections)
			if err := doAction(fmt.Sprintf("create table '%s'", tableName), func() error {
				return createTable(ctx, req.DryRun, client, tableName, attrs, schema)
			}); err != nil {
				return nil, fmt.Errorf("creating table '%s': %w", tableName, err)
			}
		}
	}

	// Update the stored materialization spec now that all of the new tables have been created.
	if err := doAction(fmt.Sprintf("update stored materialization spec and set version = %s", req.Version), func() error {
		return putSpec(ctx, client, req.Materialization, req.Version)
	}); err != nil {
		return nil, fmt.Errorf("updating stored materialization spec: %w", err)
	}

	return &pm.Response_Applied{
		ActionDescription: strings.Join(actions, "\n"),
	}, nil
}

func (d driver) NewTransactor(ctx context.Context, open pm.Request_Open) (pm.Transactor, *pm.Response_Opened, error) {
	var cfg, err = resolveEndpointConfig(open.Materialization.ConfigJson)
	if err != nil {
		return nil, nil, err
	}

	client, err := cfg.client(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("creating client: %w", err)
	}

	var bindings []binding
	tablesToBindings := make(map[string]int)
	for idx, b := range open.Materialization.Bindings {
		tablesToBindings[b.ResourcePath[0]] = idx
		bindings = append(bindings, binding{
			tableName: b.ResourcePath[0],
			fields:    mapFields(b),
			docField:  b.FieldSelection.Document,
		})
	}

	return &transactor{
		client:           client,
		bindings:         bindings,
		tablesToBindings: tablesToBindings,
	}, &pm.Response_Opened{}, nil
}

func resolveEndpointConfig(specJson json.RawMessage) (config, error) {
	var cfg = config{}
	if err := pf.UnmarshalStrict(specJson, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing endpoint config: %w", err)
	}

	return cfg, nil
}

func resolveResourceConfig(specJson json.RawMessage) (resource, error) {
	var res = resource{}
	if err := pf.UnmarshalStrict(specJson, &res); err != nil {
		return res, fmt.Errorf("parsing resource config: %w", err)
	}

	return res, nil
}

func createTable(
	ctx context.Context,
	dryRun bool,
	client *client,
	name string,
	attrs []types.AttributeDefinition,
	keySchema []types.KeySchemaElement,
) error {
	if dryRun {
		return nil
	}

	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: attrs,
		KeySchema:            keySchema,
		TableName:            aws.String(name),
		BillingMode:          types.BillingModePayPerRequest,
	}

	_, err := client.db.CreateTable(ctx, input)
	if err != nil {
		var errInUse *types.ResourceInUseException
		// Any error other than an "already exists" error is a more serious problem. Usually we
		// should not be trying to create tables that already exist, so emit a warning log if that
		// ever occurs.
		if !errors.As(err, &errInUse) {
			return fmt.Errorf("create table %s: %w", name, err)
		}
		log.WithField("table", name).Warn("table already exists")
	}

	// Wait for the table to be in an "active" state.
	maxAttempts := 30
	for attempt := 0; attempt < maxAttempts; attempt++ {
		d, err := client.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(name),
		})
		if err != nil {
			return err
		}

		if d.Table.TableStatus == types.TableStatusActive {
			return nil
		}

		log.WithFields(log.Fields{
			"table":      name,
			"lastStatus": d.Table.TableStatus,
		}).Debug("waiting for table to become ready")
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("table %s was created but did not become ready in time", name)
}

func tableConfigFromBinding(projections []pf.Projection) ([]types.AttributeDefinition, []types.KeySchemaElement) {
	mappedKeys := []mappedType{}
	for _, p := range projections {
		if p.IsPrimaryKey {
			mappedKeys = append(mappedKeys, mapType(&p))
		}
	}

	// The collection keys will be used as the partition key and sort key, respectively.
	keyTypes := [2]types.KeyType{types.KeyTypeHash, types.KeyTypeRange}

	attrs := []types.AttributeDefinition{}
	schema := []types.KeySchemaElement{}

	for idx, k := range mappedKeys {
		attrs = append(attrs, types.AttributeDefinition{
			AttributeName: aws.String(k.field),
			AttributeType: k.ddbScalarType,
		})
		schema = append(schema, types.KeySchemaElement{
			AttributeName: aws.String(k.field),
			KeyType:       keyTypes[idx],
		})
	}

	return attrs, schema
}

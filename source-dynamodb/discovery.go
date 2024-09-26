package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type discoveredTable struct {
	name      string
	streamArn string
	// keyFields and keyTypes are ordered as partition key followed by sort key, if there is a sort
	// key.
	keyFields []string
	keyTypes  []types.ScalarAttributeType
	rcus      int
}

type columnSchema struct {
	jsonType        string
	contentEncoding string
	format          string
}

func (s columnSchema) toType() *jsonschema.Schema {
	out := &jsonschema.Schema{
		Type:   s.jsonType,
		Extras: make(map[string]interface{}),
	}

	if s.contentEncoding != "" {
		out.Extras["contentEncoding"] = s.contentEncoding
	}
	if s.format != "" {
		out.Extras["format"] = s.format
	}
	return out
}

const noDiscoveredTablesMsg = `Could not discover any tables to capture; no data will be captured. Possible causes:
  - No tables with DynamoDB streams enabled. DynamoDB streams must be enabled with "View type" set to "New and old images" for each table to capture.
  - Insufficient access to list tables. The IAM user configured for the capture must have ListTables permissions.
  - There are no tables to capture in the AWS region configured for the capture.`

var dynamodbTypeToJSON = map[types.ScalarAttributeType]columnSchema{
	types.ScalarAttributeTypeS: {jsonType: "string"},
	// DynamoDB makes no distinction between integer and decimal numbers. A decimal number can be
	// used as a DynamoDB partition key or sort key, but not as a Flow collection key. Numeric types
	// that are part of the partition key or sort key are converted to strings with { format: number }.
	types.ScalarAttributeTypeN: {jsonType: "string", format: "number"},
	types.ScalarAttributeTypeB: {jsonType: "string", contentEncoding: "base64"},
}

func (driver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	client, err := cfg.toClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	tables, err := discoverTables(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("discovering tables: %w", err)
	}
	if len(tables) == 0 {
		return nil, cerrors.NewUserError(nil, noDiscoveredTablesMsg)
	}

	bindings := make([]*pc.Response_Discovered_Binding, 0, len(tables))

	for _, table := range tables {
		log.WithFields(log.Fields{
			"table":     table.name,
			"key":       table.keyFields,
			"keyTypes":  table.keyTypes,
			"streamArn": table.streamArn,
		}).Debug("discovered table")

		anchor := table.name

		properties := make(map[string]*jsonschema.Schema)
		required := []string{}
		keyPtrs := []string{}

		for idx, k := range table.keyFields {
			properties[k] = dynamodbTypeToJSON[table.keyTypes[idx]].toType()
			required = append(required, k)
			keyPtrs = append(keyPtrs, "/"+k)
		}

		schema := jsonschema.Schema{
			Definitions: jsonschema.Definitions{
				anchor: &jsonschema.Schema{
					Type: "object",
					Extras: map[string]interface{}{
						"$anchor":    anchor,
						"properties": properties,
					},
					Required: required,
				},
			},
			AllOf: []*jsonschema.Schema{
				{
					Extras: map[string]interface{}{
						"properties": map[string]*jsonschema.Schema{
							"_meta": {
								Type: "object",
								Extras: map[string]interface{}{
									"properties": map[string]*jsonschema.Schema{
										"op": {
											Enum:        []interface{}{"c", "d", "u"},
											Description: "Change operation type: 'c' Create/Insert, 'u' Update, 'd' Delete.",
										},
										"snapshot": {
											Type:        "boolean",
											Description: "Snapshot is true if the record was produced from an initial table backfill and unset if produced from reading a stream record.",
										},
										"eventId": {
											Type:        "string",
											Description: "A globally unique identifier for the event that was recorded in this stream record.",
										},
										"userIdentity": {
											Type:        "object",
											Description: "Contains details about the type of identity that made the request.",
											Extras: map[string]interface{}{
												"properties": map[string]jsonschema.Schema{
													"principalId": {
														Type:        "string",
														Description: "A unique identifier for the entity that made the call. For Time To Live, the principalId is 'dynamodb.amazonaws.com'.",
													},
													"type": {
														Type:        "string",
														Description: "The type of the identity. For Time To Live, the type is 'Service'.",
													},
												},
											},
										},
										"approximateCreationDateTime": {
											Type:        "string",
											Format:      "date-time",
											Description: "The approximate date and time when the stream record was created, in UNIX epoch time format and rounded down to the closest second.",
										},
										"before": {
											Ref:         "#" + anchor,
											Description: "The item in the DynamoDB table as it appeared before it was modified.",
											Extras: map[string]interface{}{
												"reduce": map[string]interface{}{
													"strategy": "firstWriteWins",
												},
											},
										},
									},
									"reduce": map[string]interface{}{
										"strategy": "merge",
									},
								},
								Required: []string{"op"},
							},
						},
					},
					Required: []string{"_meta"},
					If: &jsonschema.Schema{
						Extras: map[string]interface{}{
							"properties": map[string]*jsonschema.Schema{
								"_meta": {
									Extras: map[string]interface{}{
										"properties": map[string]*jsonschema.Schema{
											"op": {
												Extras: map[string]interface{}{
													"const": "d",
												},
											},
										},
									},
								},
							},
						},
					},
					Then: &jsonschema.Schema{
						Extras: map[string]interface{}{
							"reduce": map[string]interface{}{
								"strategy": "merge",
								"delete":   true,
							},
						},
					},
					Else: &jsonschema.Schema{
						Extras: map[string]interface{}{
							"reduce": map[string]interface{}{
								"strategy": "merge",
							},
						},
					},
				},
				{Ref: "#" + anchor},
			},
			Extras: map[string]interface{}{"x-infer-schema": true},
		}

		rawSchema, err := schema.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("marshalling schema json: %w", err)
		}

		resourceJSON, err := json.Marshal(resource{
			Table: table.name,
		})
		if err != nil {
			return nil, fmt.Errorf("marshalling resource json: %w", err)
		}

		bindings = append(bindings, &pc.Response_Discovered_Binding{
			RecommendedName:    table.name,
			ResourceConfigJson: resourceJSON,
			DocumentSchemaJson: rawSchema,
			Key:                keyPtrs,
		})

	}

	return &pc.Response_Discovered{Bindings: bindings}, nil
}

func discoverTables(ctx context.Context, c *client) ([]discoveredTable, error) {
	allTableNames := []string{}

	var exclusiveStartTableName *string
	for {
		listing, err := c.db.ListTables(ctx, &dynamodb.ListTablesInput{
			ExclusiveStartTableName: exclusiveStartTableName,
		})
		if err != nil {
			return nil, fmt.Errorf("listing tables: %w", err)
		}

		allTableNames = append(allTableNames, listing.TableNames...)
		exclusiveStartTableName = listing.LastEvaluatedTableName

		if exclusiveStartTableName == nil { // Pagination
			break
		}
	}

	// Discover tables with a moderate degree of parallelism, to accommodate large numbers of tables
	// from the ListTables response. Per
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ServiceQuotas.html#limits-api,
	// you can send up to 2,500 DescribeTable requests per second, so we are very unlikely to exceed
	// any API rate limits.
	out := []discoveredTable{}
	var mu sync.Mutex
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(10)

	for _, t := range allTableNames {
		t := t
		group.Go(func() error {
			discovered, include, err := discoverTable(groupCtx, c, t)
			if err != nil {
				return fmt.Errorf("discovering table: %w", err)
			}

			if include {
				mu.Lock()
				out = append(out, discovered)
				mu.Unlock()
			}

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	return out, nil
}

func discoverTable(ctx context.Context, c *client, table string) (discoveredTable, bool, error) {
	tableDescribe, err := c.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	})
	if err != nil {
		return discoveredTable{}, false, fmt.Errorf("describe table: %w", err)
	}

	if tableDescribe.Table.StreamSpecification == nil ||
		tableDescribe.Table.StreamSpecification.StreamEnabled == nil ||
		!*tableDescribe.Table.StreamSpecification.StreamEnabled {
		log.WithField("table", tableDescribe.Table.TableName).Info("table will not be captured since it does not have streaming enabled")
		return discoveredTable{}, false, nil
	}

	if tableDescribe.Table.LatestStreamArn == nil {
		// This condition may not actually be possible, but with everything in the AWS SDK being a
		// pointer it's hard to tell.
		log.WithField("table", tableDescribe.Table.TableName).Warn("streaming enabled for table but no stream ARN found; table will not be captured")
		return discoveredTable{}, false, nil
	}

	if tableDescribe.Table.StreamSpecification.StreamViewType != types.StreamViewTypeNewAndOldImages {
		log.WithFields(log.Fields{
			"table":          tableDescribe.Table.TableName,
			"streamArn":      tableDescribe.Table.LatestStreamArn,
			"streamViewType": tableDescribe.Table.StreamSpecification.StreamViewType,
		}).Warn("streamViewType must be NEW_AND_OLD_IMAGES; table will not be captured")
		return discoveredTable{}, false, nil
	}

	var rcus int
	if tableDescribe.Table.ProvisionedThroughput != nil && tableDescribe.Table.ProvisionedThroughput.ReadCapacityUnits != nil {
		rcus = int(*tableDescribe.Table.ProvisionedThroughput.ReadCapacityUnits)
	} else {
		log.WithField("table", tableDescribe.Table.TableName).Warn("could not determine provisioned RCUs for table; backfill reads will not be limited unless explicitly configured")
	}

	out := discoveredTable{
		name:      table,
		streamArn: *tableDescribe.Table.LatestStreamArn,
		rcus:      rcus,
	}

	// Streams are enabled on the table and the stream is active. Determine the hash key and
	// optional sort key schema.
	foundTypes := make(map[string]types.ScalarAttributeType)
	for _, def := range tableDescribe.Table.AttributeDefinitions {
		foundTypes[*def.AttributeName] = def.AttributeType
	}

	type tableKey struct {
		name          string
		attributeType types.ScalarAttributeType
	}

	var partitionKey, sortKey *tableKey

	for _, key := range tableDescribe.Table.KeySchema {
		if key.KeyType == types.KeyTypeHash {
			partitionKey = &tableKey{
				name:          *key.AttributeName,
				attributeType: foundTypes[*key.AttributeName],
			}
		} else if key.KeyType == types.KeyTypeRange {
			sortKey = &tableKey{
				name:          *key.AttributeName,
				attributeType: foundTypes[*key.AttributeName],
			}
		}
	}

	out.keyFields = append(out.keyFields, partitionKey.name)
	out.keyTypes = append(out.keyTypes, partitionKey.attributeType)

	if sortKey != nil {
		out.keyFields = append(out.keyFields, sortKey.name)
		out.keyTypes = append(out.keyTypes, sortKey.attributeType)
	}

	return out, true, nil
}

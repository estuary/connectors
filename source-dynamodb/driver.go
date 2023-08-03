package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

type driver struct{}

var _ boilerplate.Connector = &driver{}

func (driver) Spec(ctx context.Context, req *pc.Request_Spec) (*pc.Response_Spec, error) {
	var endpointSchema, err = schemagen.GenerateSchema("Source DynamoDB Spec", &config{}).MarshalJSON()
	if err != nil {
		fmt.Println(fmt.Errorf("generating endpoint schema: %w", err))
	}
	resourceSchema, err := schemagen.GenerateSchema("Resource", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/source-dynamodb",
	}, nil
}

func (driver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	client, err := cfg.toClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	var out []*pc.Response_Validated_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		logEntry := log.WithField("table", res.Table)

		// Sanity-checks: Can we run a scan on the table?
		if _, err := client.db.Scan(ctx, &dynamodb.ScanInput{
			TableName: aws.String(res.Table),
			Limit:     aws.Int32(1),
		}); err != nil {
			return nil, fmt.Errorf("cannot read from table %s: %w", res.Table, err)
		}
		logEntry.Debug("validated table scan")

		// Can we read from shards of the table?
		tableDescribe, err := client.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(res.Table),
		})
		if err != nil {
			return nil, fmt.Errorf("describing table %s: %w", res.Table, err)
		}

		streamDescribe, err := client.stream.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
			StreamArn: tableDescribe.Table.LatestStreamArn,
		})
		if err != nil {
			return nil, fmt.Errorf("describing stream for table %s: %w", res.Table, err)
		}

		if len(streamDescribe.StreamDescription.Shards) > 0 {
			iter, err := client.stream.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
				ShardId:           streamDescribe.StreamDescription.Shards[0].ShardId,
				ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
				StreamArn:         tableDescribe.Table.LatestStreamArn,
			})
			if err != nil {
				return nil, fmt.Errorf("getting shard iterator: %w", err)
			}

			if _, err := client.stream.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
				ShardIterator: iter.ShardIterator,
				Limit:         aws.Int32(1),
			}); err != nil {
				return nil, fmt.Errorf("cannot read from table %s stream: %w", res.Table, err)
			}

			logEntry.Debug("validated table stream shard read")
		} else {
			// If a stream is enabled, there should always be shards for the table stream.
			log.WithField("table", res.Table).Warn("no shards found for table")
		}

		out = append(out, &pc.Response_Validated_Binding{
			ResourcePath: []string{res.Table},
		})
	}

	return &pc.Response_Validated{Bindings: out}, nil
}

func (d driver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{
		ActionDescription: "",
	}, nil
}

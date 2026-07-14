package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

type driver struct{}

func (driver) Spec(context.Context, *pc.Request_Spec) (*pc.Response_Spec, error) {
	endpointSchema, err := schemagen.GenerateSchema("Source SQS Spec", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}
	resourceSchema, err := schemagen.GenerateSchema("Resource", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/source-sqs",
		ResourcePathPointers:     []string{"/queueUrl"},
	}, nil
}

func (driver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	client, err := buildClient(ctx, &cfg, 0)
	if err != nil {
		return nil, err
	}

	var out []*pc.Response_Validated_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		if err := validateQueue(ctx, client, &cfg, res.QueueURL); err != nil {
			return nil, err
		}

		out = append(out, &pc.Response_Validated_Binding{
			ResourcePath: []string{res.QueueURL},
		})
	}

	return &pc.Response_Validated{Bindings: out}, nil
}

func validateQueue(ctx context.Context, client *sqs.Client, cfg *config, queueURL string) error {
	name, urlRegion, err := parseQueueURL(queueURL)
	if err != nil {
		return err
	}

	// Queue URLs are region-qualified, so a mismatch with the configured
	// region is detectable up front. Custom endpoints (LocalStack, VPC
	// endpoints) yield an empty urlRegion and skip this check.
	if cfg.Advanced.Endpoint == "" && urlRegion != "" && urlRegion != cfg.Region {
		return fmt.Errorf("queue %q is in region %q but the endpoint config specifies region %q", queueURL, urlRegion, cfg.Region)
	}

	// Request All rather than naming attributes. Real AWS rejects an explicit
	// request for FifoQueue on a standard queue with InvalidAttributeName.
	// With All, the attribute is simply absent on standard queues.
	attrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		return fmt.Errorf("queue %q is not accessible: %w", queueURL, err)
	}

	// A mismatch here shouldn't be possible against real AWS (the `.fifo`
	// suffix is enforced at queue creation), so it points to a misbehaving
	// emulator or a malformed URL.
	attrFifo := attrs.Attributes[string(types.QueueAttributeNameFifoQueue)] == "true"
	if attrFifo != isFifoQueueName(name) {
		return fmt.Errorf("queue %q name indicates fifo=%v but its FifoQueue attribute indicates fifo=%v", queueURL, isFifoQueueName(name), attrFifo)
	}

	if visibility, err := strconv.Atoi(attrs.Attributes[string(types.QueueAttributeNameVisibilityTimeout)]); err == nil && visibility < 30 {
		log.WithFields(log.Fields{
			"queue":             queueURL,
			"visibilityTimeout": visibility,
		}).Warn("queue visibility timeout is under 30 seconds; 180s or more is recommended to reduce the number of duplicate redeliveries")
	}

	return probeDeletePermission(ctx, client, queueURL)
}

// probeDeletePermission checks sqs:DeleteMessage up front with a fabricated
// receipt handle. A capture that can receive but not delete looks healthy
// while duplicates pile up without bound, so it's better to catch missing
// delete permission here than after the task is running. Access denial
// surfaces as an error, while an allowed call fails with a harmless
// invalid-handle error (or is ignored by emulators) with no side effects.
func probeDeletePermission(ctx context.Context, client *sqs.Client, queueURL string) error {
	_, err := client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: aws.String("flow-validate-permissions-probe"),
	})
	if err != nil && isAccessDenied(err) {
		return fmt.Errorf("credentials lack sqs:DeleteMessage on queue %q: the connector requires sqs:ReceiveMessage, sqs:DeleteMessage, and sqs:GetQueueAttributes on each bound queue and sqs:ListQueues for discovery: %w", queueURL, err)
	}
	return nil
}

func isAccessDenied(err error) bool {
	var ae smithy.APIError
	return errors.As(err, &ae) && strings.Contains(ae.ErrorCode(), "AccessDenied")
}

// Apply is a no-op since the connector owns no server-side resources.
func (driver) Apply(context.Context, *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{}, nil
}

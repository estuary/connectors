package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"golang.org/x/sync/errgroup"
)

type driver struct{}

func (driver) Spec(context.Context, *pc.Request_Spec) (*pc.Response_Spec, error) {
	var endpointSchema, err = schemagen.GenerateSchema("Source Google PubSub Spec", &config{}).MarshalJSON()
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
		DocumentationUrl:         "https://go.estuary.dev/source-google-pubsub",
		ResourcePathPointers:     []string{"/topic"},
	}, nil
}

func (driver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var out []*pc.Response_Validated_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		out = append(out, &pc.Response_Validated_Binding{
			ResourcePath: []string{res.Topic},
		})
	}

	return &pc.Response_Validated{Bindings: out}, nil
}

// subscriptionName creates a deterministic subscription name based on the name
// of the topic and the name of the capture. The capture needs to create
// subscriptions for each included topic, and they all necessarily must have
// different names, so incorporating the name of the topic ensures uniqueness
// within the same capture. Including the name of the capture likewise ensures
// uniqueness if there are multiple captures that are capturing from the same
// topics, assuming that the names of the captures are different.
//
// The optional value for `prefix` can be configured by the user to make it
// easier to see which subscriptions are for which captures in the event of
// multiple captures.
func subscriptionName(prefix, topic, captureName string) string {
	// Use a digest of the topic and capture name, since there are restrictions
	// on the length of the subscription name and the characters it may contain.
	hsh := md5.New()
	hsh.Write([]byte(fmt.Sprintf("%s%s", topic, captureName)))
	digest := hex.EncodeToString(hsh.Sum(nil))

	out := fmt.Sprintf("EstuaryFlow_%s", digest)

	if prefix != "" {
		out = fmt.Sprintf("%s_%s", prefix, out)
	}

	return out
}

func (driver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.Capture.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	client, err := cfg.client(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting client from config: %w", err)
	}

	var desc string
	for _, b := range req.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		sub := subscriptionName(cfg.SubscriptionPrefix, res.Topic, req.Capture.Name.String())
		exists, err := client.Subscription(sub).Exists(ctx)
		if err != nil {
			return nil, fmt.Errorf("checking if subscription exists: %w", err)
		}
		if !exists {
			if _, err := client.CreateSubscription(ctx, sub, pubsub.SubscriptionConfig{
				Topic: client.Topic(res.Topic),
				// If messages are publish to the topic with an ordering key,
				// the connector will read receive them in order because of this
				// setting.
				EnableMessageOrdering: true,
				// "Exactly once" here isn't really exactly once end-to-end, but
				// setting this to `true` ensures messages are not sent again
				// after they are acknowledged to PubSub. It remains to be seen
				// if the presumptive reduction in duplicate messages is worth
				// whatever performance hit this incurs, but reducing duplicates
				// generally seems like a good idea.
				EnableExactlyOnceDelivery: true,
			}); err != nil {
				return nil, fmt.Errorf("creating subscription: %w", err)
			}

			desc += fmt.Sprintf("Created subscription %q for topic %q\n", sub, res.Topic)
		}
	}

	return &pc.Response_Applied{ActionDescription: desc}, nil
}

func (driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	ctx := stream.Context()

	var cfg config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	client, err := cfg.client(ctx)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	if err := stream.Ready(true); err != nil {
		return err
	}

	group, groupCtx := errgroup.WithContext(ctx)

	// Arrange for reading of runtime acknowledgements and serialized output of
	// documents corresponding to received PubSub messages.
	e := newEmitter(stream)
	group.Go(func() error {
		return e.runtimeAckWorker(groupCtx)
	})

	for idx, binding := range open.Capture.Bindings {
		idx := idx

		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}

		sub := subscriptionName(cfg.SubscriptionPrefix, res.Topic, open.Capture.Name.String())
		group.Go(func() error {
			return captureResource(groupCtx, e, client, res.Topic, sub, idx)
		})
	}

	return group.Wait()
}

func captureResource(
	ctx context.Context,
	emitter *emitter,
	client *pubsub.Client,
	topic string,
	subscription string,
	binding int,
) error {
	sub := client.Subscription(subscription)

	// This setting prevents a large amount of messages to be buffered by the
	// PubSub library for this topic before they can be output by the connector.
	sub.ReceiveSettings.MaxOutstandingBytes = 1 * 1024 * 1024

	// The `Receive` callback function doesn't let you return an error, so this
	// context is cancelled with the causing error if one is encountered when
	// handling a message.
	ctx, cancel := context.WithCancelCause(ctx)

	return sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		rtAck, err := emitter.emit(ctx, emitMessage{
			m:           m,
			binding:     binding,
			subcription: subscription,
			topic:       topic,
		})
		if err != nil {
			cancel(fmt.Errorf("subscription %q failed to emit message: %w", sub.ID(), err))
			m.Nack()
			return
		}

		select {
		case <-rtAck:
			// When the runtime has acknowledged this message and its
			// checkpoint, acknowledge the receipt to PubSub.
			m.Ack()
		case <-ctx.Done():
			cancel(ctx.Err())
			m.Nack()
		}
	})
}

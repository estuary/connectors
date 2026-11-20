package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	m "github.com/estuary/connectors/go/materialize"
	testutil "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

var integrationConfig = json.RawMessage(`{"host":"localhost","port":5674,"username":"test","password":"factorytest"}`)

// TestIntegration runs against a RabbitMQ 4.1 broker that it starts and stops
// itself from docker-compose.yaml on the flow-test network. Docker Compose is
// required and ports 5674 and 15674 must be free. It runs as part of
// `go test -v ./materialize-rabbitmq/...` and is skipped with -short. Run it alone with:
//
//	go test -v ./materialize-rabbitmq/... -run '^TestIntegration' -count=1
//
// Snapshots in .snapshots/ cover exchange settings, Apply actions and every
// published document. Refresh them with UPDATE_SNAPSHOTS=true and review the diff.
func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
	testutil.SkipRuntimeV1(t)

	// Ensure shared docker network exists; ignore failure if it already does.
	_ = exec.Command("docker", "network", "create", "flow-test").Run()
	t.Cleanup(func() { runCompose(t, "down", "-v", "--remove-orphans") })
	runCompose(t, "up", "-d", "--wait", "--wait-timeout", "180")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cfg, err := resolveEndpointConfig(integrationConfig)
	require.NoError(t, err)
	client, err := cfg.client(ctx)
	require.NoError(t, err)
	defer client.Close()
	ch, err := client.Channel()
	require.NoError(t, err)
	defer ch.Close()
	d := Driver()

	t.Run("validate", func(t *testing.T) {
		spec := integrationSpec(t, "validate")
		req := &pm.Request_Validate{ConfigJson: spec.ConfigJson}
		for _, b := range spec.Bindings {
			req.Bindings = append(req.Bindings, &pm.Request_Validate_Binding{
				ResourceConfigJson: b.ResourceConfigJson,
				Collection:         b.Collection,
			})
		}
		resp, err := d.Validate(ctx, req)
		require.NoError(t, err)
		snapshotJSON(t, resp)

		badConfig := cfg
		badConfig.Password = "incorrect-test-password"
		req.ConfigJson, err = json.Marshal(badConfig)
		require.NoError(t, err)
		_, err = d.Validate(ctx, req)
		require.ErrorContains(t, err, "connecting to RabbitMQ")
	})

	t.Run("apply", func(t *testing.T) {
		spec := integrationSpec(t, "apply")
		require.NoError(t, ch.ExchangeDeclare("apply.orders", "topic", true, false, false, false, nil))
		require.NoError(t, ch.ExchangeDeclare("apply.events", "direct", true, false, false, false, nil))

		// Passive checks must preserve even exchanges with different settings.
		require.NoError(t, ch.ExchangeDeclare("apply.existing", "fanout", false, true, true, false,
			amqp.Table{"alternate-exchange": "apply.audit"}))
		spec.Bindings = append(spec.Bindings, integrationBinding(t, resource{Exchange: "apply.existing"}, "/id"))
		before := readExchange(t, ctx, "apply.existing")

		first := applyIntegrationSpec(t, ctx, spec)
		require.Equal(t, "created exchange apply.audit", first.ActionDescription)
		second := applyIntegrationSpec(t, ctx, spec)
		require.Empty(t, second.ActionDescription)
		require.Equal(t, before, readExchange(t, ctx, "apply.existing"))

		var exchanges []exchangeView
		for _, b := range spec.Bindings {
			exchanges = append(exchanges, readExchange(t, ctx, b.ResourcePath[0]))
		}
		snapshotJSON(t, map[string]any{
			"first_apply":  first.ActionDescription,
			"second_apply": second.ActionDescription,
			"exchanges":    exchanges,
		})
	})

	t.Run("publish", func(t *testing.T) {
		spec := integrationSpec(t, "publish")
		applyIntegrationSpec(t, ctx, spec)
		queues := bindReadbackQueues(t, ch, spec, nil)
		transactions := readTransactions(t)
		tx := openIntegrationTransactor(t, ctx, spec)
		storeDocuments(t, ctx, tx, transactions[0])
		storeDocuments(t, ctx, tx, transactions[1])
		tx.Destroy()

		// A fresh transactor still appends updates and deletions to the queues.
		tx = openIntegrationTransactor(t, ctx, spec)
		storeDocuments(t, ctx, tx, transactions[2])
		var messages []messageView
		for i, count := range []int{6, 1, 1} {
			messages = append(messages, drainQueue(t, ch, queues[i], count)...)
		}
		snapshotJSON(t, messages)
	})

	t.Run("publish_window", func(t *testing.T) {
		spec := integrationSpec(t, "window")
		// The existing StoreIterator fixture alternates integer-keyed bindings.
		spec.Bindings = []*pf.MaterializationSpec_Binding{spec.Bindings[0], spec.Bindings[2]}
		applyIntegrationSpec(t, ctx, spec)
		queues := bindReadbackQueues(t, ch, spec, nil)
		tx := openIntegrationTransactor(t, ctx, spec)
		count := publishWindow*2 + 3
		_, err := tx.Store(storeIterator(ctx, count))
		require.NoError(t, err)
		seen := make(map[int]bool)
		for i, queue := range queues {
			for _, msg := range drainQueue(t, ch, queue, (count+1-i)/2) {
				var doc struct {
					ID int `json:"id"`
				}
				require.NoError(t, json.Unmarshal(msg.Body, &doc))
				require.False(t, seen[doc.ID], "duplicate document %d", doc.ID)
				require.Equal(t, i, doc.ID%2)
				require.JSONEq(t, fmt.Sprintf(`{"id":%d,"_meta":{"uuid":%q,"op":"d"}}`, doc.ID, testUUID), string(msg.Body))
				seen[doc.ID] = true
			}
		}
		for i := 0; i < count; i++ {
			require.True(t, seen[i], "missing document %d", i)
		}
	})

	t.Run("publish_nack", func(t *testing.T) {
		spec := integrationSpec(t, "nack")
		spec.Bindings = spec.Bindings[:1]
		applyIntegrationSpec(t, ctx, spec)
		bindReadbackQueues(t, ch, spec, amqp.Table{"x-max-length": int32(1), "x-overflow": "reject-publish"})
		tx := openIntegrationTransactor(t, ctx, spec)
		_, err := tx.Store(documentIterator(ctx, readTransactions(t)[0][:2]))
		require.ErrorContains(t, err, "nacked message")
	})

	t.Run("publish_closed_channel", func(t *testing.T) {
		spec := integrationSpec(t, "closed")
		applyIntegrationSpec(t, ctx, spec)
		tx := openIntegrationTransactor(t, ctx, spec)
		require.NoError(t, ch.ExchangeDelete(spec.Bindings[0].ResourcePath[0], false, false))
		_, err := tx.Store(documentIterator(ctx, readTransactions(t)[0][:1]))
		require.Error(t, err)
		require.True(t, tx.channel.IsClosed())
	})
}

func runCompose(t *testing.T, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	args = append([]string{"compose", "-p", "materialize-rabbitmq", "-f", "docker-compose.yaml"}, args...)
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	require.NoErrorf(t, err, "docker %s: %s", strings.Join(args, " "), out)
}

func integrationSpec(t *testing.T, prefix string) *pf.MaterializationSpec {
	t.Helper()
	spec := &pf.MaterializationSpec{
		Name:          "tests/rabbitmq",
		ConnectorType: pf.MaterializationSpec_IMAGE,
		ConfigJson:    integrationConfig,
		Bindings: []*pf.MaterializationSpec_Binding{
			integrationBinding(t, resource{Exchange: prefix + ".orders", RoutingKey: "orders", RoutingKeyField: "/status"}, "/id"),
			integrationBinding(t, resource{Exchange: prefix + ".events", ExchangeType: "direct", RoutingKey: "events"}, "/region", "/seq"),
			integrationBinding(t, resource{Exchange: prefix + ".audit", ExchangeType: "fanout"}, "/id"),
		},
		ShardTemplate:       &pc.ShardSpec{Id: "tests/rabbitmq/shard", MaxTxnDuration: time.Second},
		RecoveryLogTemplate: integrationJournal("recovery/tests/rabbitmq/log"),
	}
	require.NoError(t, spec.Validate())
	return spec
}

func integrationBinding(t *testing.T, res resource, keys ...string) *pf.MaterializationSpec_Binding {
	t.Helper()
	raw, err := json.Marshal(res)
	require.NoError(t, err)
	collection := pf.CollectionSpec{
		Name:              pf.Collection("tests/" + res.Exchange),
		Key:               keys,
		UuidPtr:           "/_meta/uuid",
		PartitionTemplate: integrationJournal("tests/" + res.Exchange + "/pivot=00"),
		Projections: []pf.Projection{
			{Field: "flow_document", Ptr: ""},
			{Field: "status", Ptr: "/status"},
		},
	}
	for _, key := range keys {
		collection.Projections = append(collection.Projections, pf.Projection{Field: strings.TrimPrefix(key, "/"), Ptr: key, IsPrimaryKey: true})
	}
	sort.Slice(collection.Projections, func(i, j int) bool { return collection.Projections[i].Field < collection.Projections[j].Field })
	validated, err := res.validateBinding(collection)
	require.NoError(t, err)
	return &pf.MaterializationSpec_Binding{
		ResourceConfigJson: raw,
		ResourcePath:       validated.ResourcePath,
		Collection:         collection,
		DeltaUpdates:       true,
		FieldSelection:     pf.FieldSelection{Document: "flow_document"},
	}
}

func integrationJournal(name string) *pb.JournalSpec {
	return &pb.JournalSpec{
		Name:        pb.Journal(name),
		Replication: 3,
		Fragment: pb.JournalSpec_Fragment{
			Length:           512 * 1024 * 1024,
			CompressionCodec: pb.CompressionCodec_GZIP,
			RefreshInterval:  30 * time.Second,
		},
	}
}

func applyIntegrationSpec(t *testing.T, ctx context.Context, spec *pf.MaterializationSpec) *pm.Response_Applied {
	t.Helper()
	resp, err := Driver().Apply(ctx, &pm.Request_Apply{Materialization: spec, Version: "v1"})
	require.NoError(t, err)
	return resp
}

func openIntegrationTransactor(t *testing.T, ctx context.Context, spec *pf.MaterializationSpec) *transactor {
	t.Helper()
	tx, _, _, err := Driver().NewTransactor(ctx, pm.Request_Open{Materialization: spec}, nil)
	require.NoError(t, err)
	t.Cleanup(tx.Destroy)
	return tx.(*transactor)
}

type storedDocument struct {
	Binding uint32          `json:"binding"`
	Key     tuple.Tuple     `json:"key"`
	Doc     json.RawMessage `json:"doc"`
	Delete  bool            `json:"delete"`
	Exists  bool            `json:"exists"`
}

func readTransactions(t *testing.T) [][]storedDocument {
	t.Helper()
	raw, err := os.ReadFile("testdata/publish.json")
	require.NoError(t, err)
	var transactions [][]storedDocument
	require.NoError(t, json.Unmarshal(raw, &transactions))
	return transactions
}

func documentIterator(ctx context.Context, docs []storedDocument) *m.StoreIterator {
	stream := &testStream{}
	for _, doc := range docs {
		stream.requests = append(stream.requests, pm.Request{Store: &pm.Request_Store{
			Binding:      doc.Binding,
			KeyPacked:    doc.Key.Pack(),
			ValuesPacked: tuple.Tuple{nil}.Pack(),
			DocJson:      doc.Doc,
			Delete:       doc.Delete,
			Exists:       doc.Exists,
		}})
	}
	stream.requests = append(stream.requests, pm.Request{StartCommit: &pm.Request_StartCommit{}})
	return m.NewStoreIterator(ctx, stream, &pm.Request{Flush: &pm.Request_Flush{}})
}

func storeDocuments(t *testing.T, ctx context.Context, tx *transactor, docs []storedDocument) {
	t.Helper()
	commit, err := tx.Store(documentIterator(ctx, docs))
	require.NoError(t, err)
	require.Nil(t, commit)
}

func bindReadbackQueues(t *testing.T, ch *amqp.Channel, spec *pf.MaterializationSpec, args amqp.Table) []string {
	t.Helper()
	var queues []string
	for _, b := range spec.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		require.NoError(t, err)
		q, err := ch.QueueDeclare(res.Exchange+".readback", true, false, false, false, args)
		require.NoError(t, err)
		key := res.RoutingKey
		if res.ExchangeType == "topic" {
			key = "#"
		}
		require.NoError(t, ch.QueueBind(q.Name, key, res.Exchange, false, nil))
		queues = append(queues, q.Name)
	}
	return queues
}

type exchangeView struct {
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	Durable    bool           `json:"durable"`
	AutoDelete bool           `json:"auto_delete"`
	Internal   bool           `json:"internal"`
	Arguments  map[string]any `json:"arguments"`
}

func readExchange(t *testing.T, ctx context.Context, name string) exchangeView {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost:15674/api/exchanges/%2F/"+url.PathEscape(name), nil)
	require.NoError(t, err)
	req.SetBasicAuth("test", "factorytest")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var exchange exchangeView
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&exchange))
	return exchange
}

type messageView struct {
	Queue      string            `json:"queue"`
	Exchange   string            `json:"exchange"`
	RoutingKey string            `json:"routing_key"`
	Body       json.RawMessage   `json:"body"`
	Properties messageProperties `json:"properties"`
}

type messageProperties struct {
	ContentType  string `json:"content_type"`
	DeliveryMode uint8  `json:"delivery_mode"`
	MessageID    string `json:"message_id"`
}

func drainQueue(t *testing.T, ch *amqp.Channel, queue string, expected int) []messageView {
	t.Helper()
	var messages []messageView
	for i := 0; i < expected; i++ {
		msg, ok, err := ch.Get(queue, true)
		require.NoError(t, err)
		require.True(t, ok, "missing message %d from %s", i, queue)
		var compact bytes.Buffer
		require.NoError(t, json.Compact(&compact, msg.Body))
		require.Equal(t, compact.Bytes(), msg.Body)
		require.Equal(t, "application/json", msg.ContentType)
		require.Equal(t, uint8(2), msg.DeliveryMode)
		require.Regexp(t, "^[0-9a-f]{64}$", msg.MessageId)
		require.Empty(t, msg.Headers)
		require.Empty(t, msg.ContentEncoding)
		require.Zero(t, msg.Priority)
		require.Empty(t, msg.CorrelationId)
		require.Empty(t, msg.ReplyTo)
		require.Empty(t, msg.Expiration)
		require.Zero(t, msg.Timestamp)
		require.Empty(t, msg.Type)
		require.Empty(t, msg.UserId)
		require.Empty(t, msg.AppId)
		messages = append(messages, messageView{
			Queue: queue, Exchange: msg.Exchange, RoutingKey: msg.RoutingKey, Body: msg.Body,
			Properties: messageProperties{ContentType: msg.ContentType, DeliveryMode: msg.DeliveryMode, MessageID: msg.MessageId},
		})
	}
	_, ok, err := ch.Get(queue, true)
	require.NoError(t, err)
	require.False(t, ok, "unexpected extra message in %s", queue)
	return messages
}

func snapshotJSON(t *testing.T, value any) {
	t.Helper()
	formatted, err := json.MarshalIndent(value, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

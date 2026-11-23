package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	amqp "github.com/rabbitmq/amqp091-go"
)

func (config) DefaultNamespace() string                              { return "" }
func (config) FeatureFlags() (string, map[string]common.FlagDefault) { return "", nil }
func (r resource) WithDefaults(config) resource                      { return r }
func (r resource) Parameters() ([]string, bool, error)               { return []string{r.Exchange}, true, nil }

type fieldConfig struct{}

func (fieldConfig) Validate() error    { return nil }
func (fieldConfig) CastToString() bool { return false }

type mappedType struct{}

func (mappedType) String() string                            { return "json" }
func (mappedType) Compatible(boilerplate.ExistingField) bool { return true }
func (mappedType) CanMigrate(boilerplate.ExistingField) bool { return false }

// The typed test adapter reads and cleans up the bound queues. Transactions
// run the production driver in a flowctl subprocess.
type runtimeMaterializer struct {
	client *amqp.Connection
	rows   map[string][][]any
}

var _ boilerplate.Materializer[config, fieldConfig, resource, mappedType] = &runtimeMaterializer{}

func newRuntimeMaterializer(ctx context.Context, name string, cfg config, flags map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, mappedType], error) {
	client, err := cfg.client(ctx)
	if err != nil {
		return nil, err
	}
	return &runtimeMaterializer{client: client, rows: make(map[string][][]any)}, nil
}

func (*runtimeMaterializer) Config() boilerplate.MaterializeCfg {
	return boilerplate.MaterializeCfg{NoCreateNamespaces: true, NoTruncateResources: true}
}

func (m *runtimeMaterializer) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, paths [][]string) error {
	for _, path := range paths {
		if err := checkExchange(m.client, resource{Exchange: path[0], ExchangeType: "topic"}); err != nil {
			return err
		}
		is.PushResource(path...)
	}
	return nil
}

func (*runtimeMaterializer) CheckPrerequisites(context.Context) *cerrors.PrereqErr {
	return &cerrors.PrereqErr{}
}

func (*runtimeMaterializer) NewConstraint(p pf.Projection, delta bool, fc fieldConfig) pm.Response_Validated_Constraint {
	binding, _ := (resource{}).validateBinding(pf.CollectionSpec{Projections: []pf.Projection{p}})
	return *binding.ProjectionConstraints[0].Constraint
}

func (*runtimeMaterializer) MapType(boilerplate.Projection, fieldConfig) (mappedType, boilerplate.ElementConverter) {
	return mappedType{}, nil
}

func (*runtimeMaterializer) Setup(context.Context, *boilerplate.InfoSchema) (string, error) {
	return "", nil
}

func (*runtimeMaterializer) CreateNamespace(context.Context, string) (string, error) {
	return "", nil
}

func (m *runtimeMaterializer) CreateResource(ctx context.Context, b boilerplate.MappedBinding[config, resource, mappedType]) (string, boilerplate.ActionApplyFn, error) {
	return "", nil, checkExchange(m.client, b.Config)
}

func (m *runtimeMaterializer) DeleteResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	return "", func(context.Context) error {
		ch, err := m.client.Channel()
		if err != nil {
			return err
		}
		defer ch.Close()
		if _, err := ch.QueueDelete(path[0]+".readback", false, false, false); err != nil {
			return err
		}
		return ch.ExchangeDelete(path[0], false, false)
	}, nil
}

func (*runtimeMaterializer) UpdateResource(context.Context, []string, boilerplate.ExistingResource, boilerplate.BindingUpdate[config, resource, mappedType]) (string, boilerplate.ActionApplyFn, error) {
	return "", nil, nil
}

func (*runtimeMaterializer) TruncateResource(context.Context, []string) (string, boilerplate.ActionApplyFn, error) {
	return "", nil, nil
}

func (*runtimeMaterializer) MustRecreateResource(*pm.Request_Apply, *pf.MaterializationSpec_Binding, *pf.MaterializationSpec_Binding) (bool, error) {
	return false, nil
}

func (*runtimeMaterializer) NewTransactor(ctx context.Context, req pm.Request_Open, is boilerplate.InfoSchema, bindings []boilerplate.MappedBinding[config, resource, mappedType], be *m.BindingEvents) (m.Transactor, error) {
	tx, _, _, err := Driver().NewTransactor(ctx, req, be)
	return tx, err
}

func (*runtimeMaterializer) ListTestTasks(context.Context) ([]string, error) { return nil, nil }
func (*runtimeMaterializer) CleanupTestTask(context.Context, string) error   { return nil }

func (m *runtimeMaterializer) SnapshotTestResource(ctx context.Context, path []string) ([]string, [][]any, error) {
	ch, err := m.client.Channel()
	if err != nil {
		return nil, nil, err
	}
	defer ch.Close()
	for {
		msg, ok, err := ch.Get(path[0]+".readback", true)
		if err != nil {
			return nil, nil, err
		} else if !ok {
			break
		}
		if !json.Valid(msg.Body) || msg.ContentType != "application/json" || msg.DeliveryMode != amqp.Persistent {
			return nil, nil, fmt.Errorf("invalid JSON message or properties from exchange %q", path[0])
		}
		m.rows[path[0]] = append(m.rows[path[0]], []any{string(msg.Body), msg.RoutingKey, msg.MessageId})
	}
	rows := m.rows[path[0]]
	sort.Slice(rows, func(i, j int) bool { return rows[i][0].(string) < rows[j][0].(string) })
	return []string{"payload", "routing_key", "message_id"}, rows, nil
}

func (m *runtimeMaterializer) Close(context.Context) { m.client.Close() }

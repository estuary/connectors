package connector

import (
	"context"
	"fmt"
	"sort"

	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

// streamV2Retirement takes a binding's snowpipe streaming v2 channels out of
// service on behalf of the snowpipe streaming path that replaced them. It records
// each channel as retiring in the checkpoint before it drops the channel, so that a
// checkpoint which outlives the drop explains the empty channel it names.
type streamV2Retirement struct {
	manager  *streamV2Manager // supplies the sidecar
	fenced   bool             // this session opened every registered channel
	bindings map[string]*retiringBinding
}

type retiringBinding struct {
	database, schema, table string
	// items holds this shard's channels of the binding, keyed as the checkpoint
	// keys them.
	items map[string]*streamV2Item
}

func newStreamV2Retirement(m *streamV2Manager) *streamV2Retirement {
	return &streamV2Retirement{manager: m, bindings: make(map[string]*retiringBinding)}
}

// register records the channels of one binding to retire. Only items whose
// KeyBegin lies in the shard's range are taken; the rest belong to sibling shards.
func (r *streamV2Retirement) register(stateKey, database, schema, table string, prior map[string]*streamV2Item, shard *pf.RangeSpec) {
	var items = make(map[string]*streamV2Item)
	for key, item := range prior {
		if item == nil {
			continue
		}
		if item.KeyBegin < shard.KeyBegin || item.KeyBegin > shard.KeyEnd {
			continue
		}
		items[key] = item
	}
	if len(items) == 0 {
		return
	}
	r.bindings[stateKey] = &retiringBinding{database: database, schema: schema, table: table, items: items}
}

// fence opens every registered channel once per session. Opening a channel takes
// it from whichever process held it before, so nothing else appends to it while
// the snowpipe streaming path materializes the same documents.
func (r *streamV2Retirement) fence(ctx context.Context) error {
	if r.fenced || len(r.bindings) == 0 {
		r.fenced = true
		return nil
	}

	client, err := r.manager.ensureStarted(ctx)
	if err != nil {
		return err
	}

	for stateKey, b := range r.bindings {
		for _, item := range b.items {
			status, err := client.OpenChannel(ctx, b.database, b.schema, b.table, item.Channel)
			if err != nil {
				return fmt.Errorf("opening channel %q: %w", item.Channel, err)
			}
			log.WithFields(log.Fields{
				"stateKey":        stateKey,
				"channel":         item.Channel,
				"checkpointIndex": item.Counter,
				"committedToken":  status.committedToken(),
			}).Info("fenced a snowpipe streaming v2 channel ahead of retiring it")
		}
	}
	r.fenced = true
	return nil
}

// markers returns the binding's items with Retiring set, or nil when the binding
// has nothing left to retire.
func (r *streamV2Retirement) markers(stateKey string) map[string]*streamV2Item {
	var b, ok = r.bindings[stateKey]
	if !ok {
		return nil
	}
	var out = make(map[string]*streamV2Item, len(b.items))
	for key, item := range b.items {
		var marker = *item
		marker.Retiring = true
		out[key] = &marker
	}
	return out
}

// pending reports whether the binding still has channels to drop.
func (r *streamV2Retirement) pending(stateKey string) bool {
	_, ok := r.bindings[stateKey]
	return ok
}

// drop retires every channel of the binding and forgets them, returning the
// checkpoint keys of the items it retired. It requires fence to have run this
// session; the drop uses the handle the open produced.
func (r *streamV2Retirement) drop(ctx context.Context, stateKey string) ([]string, error) {
	if !r.fenced {
		return nil, fmt.Errorf("retiring channels of %q before this session fenced them", stateKey)
	}
	var b, ok = r.bindings[stateKey]
	if !ok {
		return nil, nil
	}

	client, err := r.manager.ensureStarted(ctx)
	if err != nil {
		return nil, err
	}

	var channels []string
	var keys []string
	for key, item := range b.items {
		if err := retireChannel(ctx, client, item.Channel); err != nil {
			return nil, err
		}
		channels = append(channels, item.Channel)
		keys = append(keys, key)
	}
	sort.Strings(channels)
	log.WithFields(log.Fields{
		"table":    b.table,
		"channels": channels,
	}).Info("retired the snowpipe streaming v2 channels of a binding that moved to the snowpipe streaming path")

	delete(r.bindings, stateKey)
	return keys, nil
}

// done reports that every registered binding has been dropped.
func (r *streamV2Retirement) done() bool {
	return len(r.bindings) == 0
}

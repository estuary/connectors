package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

const maxBatchLines = 5000

// Large documents can make 5,000 lines exceed a server's request size limit.
// A single line larger than this is still sent alone.
const maxBatchBytes = 8 << 20

type transactor struct {
	client   *client
	bindings []*measurementBinding
	be       *m.BindingEvents
	// Points InfluxDB dropped for retention since the connector started.
	droppedPoints int
}

type measurementBinding struct {
	path           []string
	resource       resource
	fields         pf.FieldSelection
	numericFields  map[string]string
	timestampInKey bool
	keyTags        map[string]bool
	operationField string
}

var _ m.Transactor = &transactor{}

func (t *transactor) UnmarshalState(state json.RawMessage) error { return nil }

func (t *transactor) Flush(context.Context, map[int]time.Time) error { return nil }

func (t *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	return nil, nil
}

// InfluxDB is delta-update only.
func (t *transactor) Load(it *m.LoadIterator, _ func(int, json.RawMessage) error) error {
	for it.Next() {
		return fmt.Errorf("driver only supports delta updates")
	}
	return it.Err()
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	var batch strings.Builder
	var lines int
	written := make([]int64, len(t.bindings))
	round := it.Round

	flush := func() error {
		if lines == 0 {
			return nil
		}
		dropped, err := t.client.writePoints(it.Context(), batch.String())
		if err != nil {
			return err
		}
		if dropped > 0 {
			// The status keeps only the first warning for a key, so log each count.
			t.droppedPoints += dropped
			t.be.Warn("influxdb-retention-dropped", fmt.Sprintf("InfluxDB is dropping points older than the retention period of bucket %q; see the logs for counts", t.client.config.Bucket))
			log.WithFields(log.Fields{"dropped": dropped, "total": t.droppedPoints}).Warn(fmt.Sprintf("InfluxDB dropped %d points older than the retention period of bucket %q (%d since the connector started)", dropped, t.client.config.Bucket, t.droppedPoints))
		}
		for i, n := range written {
			if n > 0 {
				t.be.ReportRowStats(round, t.bindings[i].path, m.TotalRowStats(n))
			}
		}
		batch.Reset()
		lines = 0
		clear(written)
		return nil
	}

	// Delta updates never load existing points, so deletes must also run for unseen keys.
	for it.Next(false) {
		binding := t.bindings[it.Binding]
		if t.client.config.HardDelete && binding.isDelete(it) {
			if err := flush(); err != nil {
				return nil, err
			}
			request, err := binding.deleteRequest(it.Key, it.Values)
			if errors.Is(err, errEmptyKeyTag) {
				t.be.Warn(fmt.Sprintf("influxdb-empty-key-tag-%d", it.Binding), fmt.Sprintf("Skipping a delete for measurement %q whose points cannot be matched: %v", binding.resource.Measurement, err))
				continue
			} else if err != nil {
				return nil, fmt.Errorf("encoding delete for measurement %q: %w", binding.resource.Measurement, err)
			}
			if err := t.client.deletePoints(it.Context(), request); err != nil {
				return nil, err
			}
			t.be.ReportRowStats(round, binding.path, m.TotalRowStats(1))
			continue
		}
		line, err := binding.point(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("encoding point for measurement %q: %w", binding.resource.Measurement, err)
		}
		if line == "" {
			t.be.Warn(fmt.Sprintf("influxdb-empty-fields-%d", it.Binding), fmt.Sprintf("Skipping points for measurement %q with no non-null fields", binding.resource.Measurement))
			continue
		}
		if lines > 0 && batch.Len()+len(line)+1 > maxBatchBytes {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		batch.WriteString(line)
		batch.WriteByte('\n')
		lines++
		written[it.Binding]++
		if lines == maxBatchLines || batch.Len() >= maxBatchBytes {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return nil, nil
}

func (t *transactor) Destroy() {
	t.client.httpClient.CloseIdleConnections()
}

func (t *transactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, rangeSpec pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return nil, nil
}

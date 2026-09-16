package materialize

import (
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/gogo/protobuf/types"
	log "github.com/sirupsen/logrus"
)

func logBackfillSignals(spec *pf.MaterializationSpec, flush *pm.Request_Flush) error {
	var fields = func(binding uint32, ts *types.Timestamp) (log.Fields, error) {
		if int(binding) >= len(spec.Bindings) {
			return nil, fmt.Errorf("protocol error (backfill signal names binding %d of %d)", binding, len(spec.Bindings))
		}
		var boundary, err = types.TimestampFromProto(ts)
		if err != nil {
			return nil, fmt.Errorf("protocol error (backfill signal of binding %d has invalid timestamp): %w", binding, err)
		}
		return log.Fields{
			"binding":      binding,
			"resourcePath": spec.Bindings[binding].ResourcePath,
			"boundary":     boundary,
		}, nil
	}

	for _, b := range flush.BackfillBegins {
		if f, err := fields(b.Binding, b.Timestamp); err != nil {
			return err
		} else {
			log.WithFields(f).Info("backfill begins")
		}
	}
	for _, c := range flush.BackfillCompletes {
		if f, err := fields(c.Binding, c.Timestamp); err != nil {
			return err
		} else {
			log.WithFields(f).Info("backfill completes")
		}
	}
	return nil
}

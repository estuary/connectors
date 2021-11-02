package main

import (
	"encoding/json"
	"fmt"

	"github.com/estuary/protocols/airbyte"
	"github.com/sirupsen/logrus"
)

// validator consumes a sequence of Airbyte records from a capture connector,
// interprets them as a stream of CDC changes to the `(ID, SequenceNumber)`
// dataset produced by `trafficGenerator`, and detects any anomalies in the
// change stream (which would indicate a correctness bug in the connector).
type validator struct {
	history []changeEvent
	entries map[int]int
	errs    []error
}

func (v *validator) Validate(rec *airbyte.Record) error {
	var fields map[string]interface{}
	if err := json.Unmarshal(rec.Data, &fields); err != nil {
		return fmt.Errorf("error parsing record: %w", err)
	}
	var evt changeEvent
	if val, ok := fields["_change_type"]; ok {
		evt.op = val.(string)
	}
	if val, ok := fields["id"]; ok {
		evt.id = numericToInt(val)
	}
	if val, ok := fields["seq"]; ok {
		evt.seq = numericToInt(val)
	}

	return v.validateEvent(&evt)
}

func (v *validator) validateEvent(evt *changeEvent) error {
	var err = v.validateChange(evt)
	v.history = append(v.history, *evt)
	if err != nil {
		logrus.WithField("index", len(v.history)).WithField("evt", evt).WithField("reason", err).Warn("invalid mutation")
		v.errs = append(v.errs, err)
	} else {
		logrus.WithField("index", len(v.history)).WithField("evt", evt).Debug("valid mutation")
	}
	return err
}

func (v *validator) validateChange(evt *changeEvent) error {
	if v.entries == nil {
		v.entries = make(map[int]int)
	}
	switch evt.op {
	case "Insert":
		var _, ok = v.entries[evt.id]
		v.entries[evt.id] = evt.seq
		if ok {
			return fmt.Errorf("insert of already present id=%d", evt.id)
		}
	case "Update":
		var prevSeq, ok = v.entries[evt.id]
		v.entries[evt.id] = evt.seq
		if !ok {
			return fmt.Errorf("update of nonexistent id=%d", evt.id)
		}
		if prevSeq != evt.seq-1 {
			return fmt.Errorf("updated id=%d from seq %d to %d", evt.id, prevSeq, evt.seq)
		}
	case "Delete":
		var prevSeq, ok = v.entries[evt.id]
		if !ok {
			return fmt.Errorf("delete of nonexistent id=%d", evt.id)
		}
		delete(v.entries, evt.id)
		if prevSeq != 2 && prevSeq != 6 {
			return fmt.Errorf("deleted id=%d at incorrect seq=%d", evt.id, prevSeq)
		}
	default:
		return fmt.Errorf("invalid change type %q for id=%d", evt.op, evt.id)
	}
	return nil
}

func (v *validator) Close() error {
	var maxID = 0
	for id := range v.entries {
		if id > maxID {
			maxID = id
		}
	}
	for id := 0; id <= maxID; id++ {
		var seq = v.entries[id]
		if seq != 9 {
			v.errs = append(v.errs, fmt.Errorf("row with id=%d has incomplete seq=%d at close", id, seq))
		}
	}
	if len(v.errs) > 0 {
		return v.errs[0]
	}
	return nil
}

func (v *validator) Errors() []error {
	return v.errs
}

// numericToInt converts an interface{} value whose underlying type is `int` or
// `float64` into an integer, and panics on any other inputs. This is required
// because JSON round-tripping will tend to turn integer primary keys into
// floats.
func numericToInt(x interface{}) int {
	switch x := x.(type) {
	case float64:
		return int(x)
	case int64:
		return int(x)
	case nil:
		return 0
	}
	return x.(int)
}

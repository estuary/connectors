package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// dsdgen rows per work item. For the sales tables a dsdgen row is a ticket or
// order, which expands to several line items. Tests lower this to exercise
// chunking at small scale.
var chunkTargetRows int64 = 1_000_000

// Stdout lines a stream buffers before writing them out with their checkpoint.
// If documents went out on their own, another stream's checkpoint could commit
// them and a restart would emit them again.
const checkpointEvery = 10_000

type state struct {
	Bindings map[boilerplate.StateKey]*bindingState `json:"bindingStateV1,omitempty"`
}

// A returns binding reads the same dsdgen stream as its sales parent, so the
// two record the same progress.
type bindingState struct {
	Scale     string           `json:"scale,omitempty"`     // scale factor the rows were generated at
	Chunks    int              `json:"chunks,omitempty"`    // work items the stream is split into
	Completed int              `json:"completed,omitempty"` // chunks 1..Completed are fully emitted
	InFlight  map[string]int64 `json:"inFlight,omitempty"`  // chunk -> stdout lines already emitted
	Done      bool             `json:"done,omitempty"`
}

type binding struct {
	index    int
	table    *tableDef
	stateKey boilerplate.StateKey
	state    *bindingState
}

type capture struct {
	out      *boilerplate.PullOutput
	gen      *generator
	bindings []*binding
}

func planChunks(rows int64) int {
	return int((rows + chunkTargetRows - 1) / chunkTargetRows)
}

func (c *capture) run() error {
	for _, b := range c.bindings {
		if b.state.Scale != "" && b.state.Scale != c.gen.scale {
			return fmt.Errorf("binding %s holds data generated at scale factor %s but the endpoint is now configured for %s; backfill the bindings to regenerate the dataset at the new scale", b.table.Name, b.state.Scale, c.gen.scale)
		}
	}
	if err := c.out.Ready(false); err != nil {
		return err
	}
	var ctx = c.out.Context()

	// One dsdgen stream per parent table. A returns binding joins its parent's
	// stream even when the parent itself is disabled.
	var streams []*streamRun
	var byParent = map[string]*streamRun{}
	for _, b := range c.bindings {
		var parent = streamOf(b.table)
		s, ok := byParent[parent.Name]
		if !ok {
			s = &streamRun{capture: c, parent: parent}
			byParent[parent.Name] = s
			streams = append(streams, s)
		}
		s.members = append(s.members, b)
	}

	group, gctx := errgroup.WithContext(ctx)
	for _, s := range streams {
		group.Go(func() error { return s.run(gctx) })
	}
	if err := group.Wait(); err != nil {
		if ctx.Err() != nil {
			log.Info("shutting down due to context cancellation")
			return nil
		}
		return err
	}
	log.WithFields(log.Fields{
		"eventType": "connectorStatus",
		"bindings":  len(c.bindings),
	}).Info("Every binding has emitted its full dataset; idling")
	<-ctx.Done()
	log.Info("shutting down due to context cancellation")
	return nil
}

type streamRun struct {
	capture *capture
	parent  *tableDef
	members []*binding
}

func (s *streamRun) run(ctx context.Context) error {
	var chunks int
	for _, m := range s.members {
		if m.state.Chunks > 0 {
			chunks = m.state.Chunks
			break
		}
	}
	if chunks == 0 {
		rows, err := s.capture.gen.rowCount(ctx, s.parent.Name)
		if err != nil {
			return err
		}
		chunks = planChunks(rows)
		log.WithFields(log.Fields{"table": s.parent.Name, "rows": rows, "chunks": chunks}).Info("planned generation")
	}

	// Resume from the lowest chunk any member still needs. Members further
	// along drop the rows they already emitted, so a binding added later
	// catches up without its siblings repeating anything.
	var start = chunks + 1
	for _, m := range s.members {
		m.state.Scale = s.capture.gen.scale
		m.state.Chunks = chunks
		if !m.state.Done && m.state.Completed+1 < start {
			start = m.state.Completed + 1
		}
	}
	if start > chunks {
		return nil
	}
	if err := s.checkpoint(func(m *binding) map[string]any {
		return map[string]any{"scale": m.state.Scale, "chunks": m.state.Chunks}
	}); err != nil {
		return err
	}
	for k := start; k <= chunks; k++ {
		if err := s.runChunk(ctx, k, chunks); err != nil {
			return err
		}
	}
	log.WithField("table", s.parent.Name).Info("stream complete")
	return nil
}

func (s *streamRun) runChunk(ctx context.Context, chunk, chunks int) error {
	var key = strconv.Itoa(chunk)
	type target struct {
		b    *binding
		skip int64 // stdout lines of this chunk already emitted before a restart
		docs []json.RawMessage
	}
	var targets []*target
	for _, m := range s.members {
		if chunk > m.state.Completed {
			targets = append(targets, &target{b: m, skip: m.state.InFlight[key]})
		}
	}
	if len(targets) == 0 {
		return nil
	}
	log.WithFields(log.Fields{"table": s.parent.Name, "chunk": chunk, "chunks": chunks}).Info("generating chunk")

	var lines int64
	var emit = func(fn func(m *binding) map[string]any) error {
		var batches = map[int][]json.RawMessage{}
		for _, t := range targets {
			if len(t.docs) > 0 {
				batches[t.b.index] = t.docs
				t.docs = nil
			}
		}
		checkpoint, err := s.buildCheckpoint(fn)
		if err != nil {
			return err
		}
		return emitWithCheckpoint(s.capture.out, batches, checkpoint)
	}
	var progress = func(m *binding) map[string]any {
		if chunk <= m.state.Completed {
			return nil
		}
		if m.state.InFlight == nil {
			m.state.InFlight = map[string]int64{}
		}
		// Never lower a member's mark. The earlier run committed the rows below
		// it, and this run skips them rather than emitting them again.
		m.state.InFlight[key] = max(m.state.InFlight[key], lines)
		return map[string]any{"inFlight": map[string]int64{key: m.state.InFlight[key]}}
	}

	err := s.capture.gen.stream(ctx, s.parent.Name, chunks, chunk, func(line []byte) error {
		table, err := routeLine(s.parent, line)
		if err != nil {
			return err
		}
		var idx = lines
		lines++
		for _, t := range targets {
			if t.b.table != table || idx < t.skip {
				continue
			}
			doc, err := decodeLine(table, line)
			if err != nil {
				return err
			}
			t.docs = append(t.docs, doc)
		}
		if lines%checkpointEvery == 0 {
			return emit(progress)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return emit(func(m *binding) map[string]any {
		if chunk <= m.state.Completed {
			return nil
		}
		m.state.Completed = chunk
		m.state.InFlight = nil
		m.state.Done = chunk == chunks
		return map[string]any{"completed": chunk, "inFlight": nil, "done": m.state.Done}
	})
}

// Holds the lock across the documents and their checkpoint so another stream's
// checkpoint cannot commit these documents first.
func emitWithCheckpoint(out *boilerplate.PullOutput, batches map[int][]json.RawMessage, checkpoint json.RawMessage) error {
	out.Lock()
	defer out.Unlock()
	for binding, docs := range batches {
		for _, doc := range docs {
			if err := out.Send(&pc.Response{Captured: &pc.Response_Captured{Binding: uint32(binding), DocJson: doc}}); err != nil {
				return fmt.Errorf("writing captured documents: %w", err)
			}
		}
	}
	if err := out.Send(&pc.Response{Checkpoint: &pc.Response_Checkpoint{
		State: &pf.ConnectorState{UpdatedJson: checkpoint, MergePatch: true},
	}}); err != nil {
		return fmt.Errorf("writing checkpoint: %w", err)
	}
	return nil
}

func (s *streamRun) buildCheckpoint(fn func(m *binding) map[string]any) (json.RawMessage, error) {
	var states = map[boilerplate.StateKey]any{}
	for _, m := range s.members {
		if state := fn(m); state != nil {
			states[m.stateKey] = state
		}
	}
	return json.Marshal(map[string]any{"bindingStateV1": states})
}

func (s *streamRun) checkpoint(fn func(m *binding) map[string]any) error {
	checkpoint, err := s.buildCheckpoint(fn)
	if err != nil {
		return err
	}
	return s.capture.out.Checkpoint(checkpoint, true)
}

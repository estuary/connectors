package filesource

import (
	"encoding/json"
	"fmt"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

// bindingStateV1 is the conventional location to persist individual binding states in a top-level
// JSON object containing the connector state.
const bindingStateV1 = "bindingStateV1"

// unmarshalState for CaptureState provides compatibility with legacy state checkpoints which were a
// map[string]State keying the stream name to the State. It should be removed when all Filesource
// captures have migrated to the new state, after which point older captures that haven't migrated
// will be non-functional.
func unmarshalState(stateJson json.RawMessage, bindings []*pf.CaptureSpec_Binding) (CaptureState, bool, error) {
	s := CaptureState{
		States: make(map[boilerplate.StateKey]State),
	}

	// Legacy states are an object with "streams" from the resource pointing to a State. The new
	// state is an object with a single key of "bindingStateV1", with that pointing to an object
	// containing State's keyed by binding stateKeys. Unmarshal into this general structure first to
	// figure out which one we are dealing with.
	v := make(map[string]json.RawMessage)
	if err := json.Unmarshal(stateJson, &v); err != nil {
		return CaptureState{}, false, err
	}
	migrated := false

	if len(v) == 0 {
		// State was an empty object.
		return s, migrated, nil
	}

	if _, ok := v[bindingStateV1]; ok && len(v) != 1 {
		return CaptureState{}, false, fmt.Errorf("existing state has key 'bindingStateV1' but %d top level keys", len(v))
	} else if _, ok := v[bindingStateV1]; ok {
		log.Info("skipping state migration since it's already done")
		if err := json.Unmarshal(v[bindingStateV1], &s.States); err != nil {
			return CaptureState{}, false, err
		}
	} else {
		// There is no key for "bindingStateV1" yet, so we must not have migrated the state and
		// should do that now.
		migrated = true

		for _, b := range bindings {
			if b.StateKey == "" {
				return CaptureState{}, false, fmt.Errorf("state key was empty for binding %s", b.ResourcePath)
			}

			var res resource
			if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
				return CaptureState{}, false, fmt.Errorf("error parsing resource config: %w", err)
			}

			stateRaw, ok := v[res.Stream]
			if !ok {
				// We've already established that v has keys, and no existing filesource captures
				// have any number of bindings other than 1, so bail out if the state can't be
				// found.
				return CaptureState{}, false, fmt.Errorf("existing state not found for stream %q", res.Stream)
			}

			var streamState State
			if err := json.Unmarshal(stateRaw, &streamState); err != nil {
				return CaptureState{}, false, err
			}
			s.States[boilerplate.StateKey(b.StateKey)] = streamState

			log.WithFields(log.Fields{
				"stateKey":    b.StateKey,
				"stream":      res.Stream,
				"streamState": streamState,
			}).Info("migrated binding state")
		}

		log.WithFields(log.Fields{
			"oldState": string(stateJson),
			"newState": s,
		}).Info("migrated capture state")
	}

	if err := s.Validate(); err != nil {
		return CaptureState{}, false, err
	}

	return s, migrated, nil
}

// CaptureState is the State of each captured binding.
type CaptureState struct {
	States map[boilerplate.StateKey]State `json:"bindingStateV1,omitempty"`
}

func (s CaptureState) Validate() error {
	for stateKey, state := range s.States {
		if err := state.Validate(); err != nil {
			return fmt.Errorf("stateKey %s: %w", stateKey, err)
		}
	}
	return nil
}

// State of a captured file prefix.
type State struct {
	// Exclusive lower bound modification time of files processed in this sweep.
	MinBound time.Time `json:"minBound"`
	// MaxBound is the exclusive upper-bound of files being processed in this sweep.
	// If MaxBound is nil, then this state checkpoint was generated after the completion
	// of a prior sweep and before the commencement of the next one.
	MaxBound *time.Time `json:"maxBound"`
	// Maximum modification time of any object processed during this sweep,
	// or nil if no files have been processed so far.
	// The MaxMod of a current sweep becomes the MinBound of the next sweep.
	MaxMod *time.Time `json:"maxMod"`

	// Base path which is currently being processed, or was last processed (if Complete).
	Path string `json:"path"`
	// Number of records from the file at |Path| which have been emitted.
	Records int `json:"records"`
	// Whether the file at |Path| is complete.
	Complete bool `json:"complete"`

	// skip is used for crash recovery. It's the number of records of the current
	// Path which we'll skip, to seek to the point of prior maximum progres.
	skip int `json:"-"`
}

func (p *State) Validate() error {

	// Consistency checks over timestamps.
	if p.MaxBound != nil && !p.MaxBound.After(p.MinBound) {
		return fmt.Errorf("expected maxBound > minBound")
	} else if p.MaxMod != nil && !p.MaxMod.After(p.MinBound) {
		return fmt.Errorf("expected maxMod > minBound")
	} else if p.MaxMod != nil && p.MaxBound != nil && !p.MaxBound.After(*p.MaxMod) {
		return fmt.Errorf("expected maxBound > maxMod")
	}

	// Consistency checks of Path state.
	if p.Path == "" {
		if p.Records != 0 {
			return fmt.Errorf("expected records == 0 if path is empty")
		} else if p.Complete {
			return fmt.Errorf("expected !complete if path is empty")
		}
	} else {
		if p.Records < 0 {
			return fmt.Errorf("expected records >= 0")
		}
	}

	return nil
}

func (p *State) startSweep(horizon time.Time) {
	if p.MaxBound != nil {
		// We've recovered a previous, partially completed sweep.
		// Leave MaxBound as it is, because the prior invocation has already
		// partially walked the keyspace using MaxBound to determine what to
		// process, and we want the remainder of the sweep to be consistent
		// to the decisions it's already made.
	} else {
		// Mark the commencement of a new sweep.
		p.MaxBound = cloneTime(horizon)
	}
}

func (p *State) finishSweep(monotonic bool) {
	if !monotonic {
		var minBound = p.MinBound
		if p.MaxMod != nil {
			// We processed at least one file in this sweep.
			// Move MinBound forward by the smallest delta which excludes all known
			// and processed files. We use MaxMod rather than MaxBound to be as
			// robust as possible to modification-time slippage or delays
			// in observations of files from the store.
			//
			// This isn't foolproof, however:
			// If a prefix requires many underlying listing RPCs to enumerate,
			// and a file A is created with a mod time < MaxBound
			// which _should_ have appeared in an early list batch but didn't,
			// while another file B with a mod time > A but < MaxBound is
			// observed and processed, then the next MinBound would reflect B
			// and would cause A to be missed.
			//
			// It therefore is also important to select a MaxBound = Now - Delta
			// such that no file PUT to the store and assigned a modification time
			// before MaxBound can fail to appear in a listing at timepoint Now.
			// In other words, Delta must account for the maximum clock drift
			// between the Store and this connector.
			minBound = *p.MaxMod
		}

		*p = State{
			MinBound: minBound,
			MaxBound: nil,
			MaxMod:   nil,
			Path:     "",
			Records:  0,
			Complete: false,
		}
	} else {
		*p = State{
			MinBound: p.MinBound, // Never increment MinBound.
			MaxBound: nil,
			MaxMod:   p.MaxMod, // Continue accumulating the MaxMod.
			Path:     p.Path,   // Track path which the next sweep begins from.
			Records:  0,
			Complete: p.Path != "",
		}
	}
}

func (p *State) shouldSkip(path string, modTime time.Time) (_ bool, reason string) {
	// Have we already processed through this filename in this sweep?
	if p.Path > path {
		return true, "state.Path > obj.Path"
	}
	if p.Path == path && p.Complete {
		return true, "state.Path == obj.Path && Complete"
	}
	// Is the path modified before our window (exclusive) ?
	if !modTime.After(p.MinBound) {
		return true, "!modTime.After(MinBound)"
	}
	// Is the path modified after our window (exclusive) ?
	if !p.MaxBound.After(modTime) {
		return true, "!MaxBound.After(modTime)"
	}
	return false, ""
}

func (p *State) startPath(path string, modTime time.Time) bool {
	if p.Path > path || (p.Path == path && p.Complete) {
		panic("should have been filtered already")
	} else if !p.MaxBound.After(modTime) {
		// Path modTime can be re-stated after opening it for reading,
		// such that it's now outside of our window.
		return false
	}

	if p.Path == path {
		p.skip = p.Records
	} else {
		p.skip = 0
	}

	if p.MaxMod == nil || modTime.After(*p.MaxMod) {
		p.MaxMod = cloneTime(modTime)
	}

	p.Path = path
	p.Records = 0
	p.Complete = false

	return true
}

func (p *State) nextLines(lines []json.RawMessage) []json.RawMessage {
	p.Records += len(lines)

	if ll := len(lines); ll < p.skip {
		p.skip -= ll
		return nil
	}
	if p.skip != 0 {
		lines = lines[p.skip:]
		p.skip = 0
	}
	return lines
}

func (p *State) finishPath() {
	if p.skip != 0 {
		// This would require that a file was modified after a prior invocation
		// partly read it and then crashed, but somehow *before* the horizon time
		// of that prior invocation. Technically this is possible if the object
		// store is very delayed and is PUTing objects at wall-clock future
		// timepoints, with effective modification times _behind_ the selected
		// horizon timestamp.
		log.WithFields(log.Fields{
			"horizon": p.MaxBound,
			"path":    p.Path,
			"records": p.Records,
			"skip":    p.skip,
		}).Error("finished object with skip != 0")
		p.skip = 0
	}

	p.Complete = true
	p.Records = 0
}

func cloneTime(t time.Time) *time.Time {
	var out = new(time.Time)
	*out = t
	return out
}

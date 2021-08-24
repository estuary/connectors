package filesource

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
)

// States is the State of each captured file prefix.
type States map[string]State

func (p States) Validate() error {
	for prefix, state := range p {
		if err := state.Validate(); err != nil {
			return fmt.Errorf("prefix %s: %w", prefix, err)
		}
	}
	return nil
}

// State of a captured file prefix.
type State struct {
	// Exclusive lower bound modification time of files processed in this sweep.
	MinBound time.Time `json:"minBound,omitempty"`
	// MaxBound is the exclusive upper-bound of files being processed in this sweep.
	// If MaxBound is nil, then this state checkpoint was generated after the completion
	// of a prior sweep and before the commencement of the next one.
	MaxBound *time.Time `json:"maxBound,omitempty"`
	// Maximum modification time of any object processed during this sweep,
	// or nil if no files have been processed so far.
	// The MaxMod of a current sweep becomes the MinBound of the next sweep.
	MaxMod *time.Time `json:"maxMod,omitempty"`

	// Base path which is currently being processed, or was last processed (if Complete).
	Path string `json:"path,omitempty"`
	// Number of records from the file at |Path| which have been emitted.
	Records int `json:"records,omitempty"`
	// Whether the file at |Path| is complete.
	Complete bool `json:"complete,omitempty"`

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
		logrus.WithFields(logrus.Fields{
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

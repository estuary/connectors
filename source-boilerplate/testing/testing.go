package testing

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	pc "github.com/estuary/flow/go/protocols/capture"
	"github.com/estuary/flow/go/protocols/flow"
	jsonpatch "github.com/evanphx/json-patch/v5"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

// CaptureSpec represents a configured capture task which can be run in an automated test.
//
// It consists of a pc.DriverServer, endpoint configuration, state checkpoint, and a list
// of bindings. This is roughly equivalent to the configuration and runtime state of a real
// capture running within Flow.
//
// In addition there is a set of "sanitizers" which replace specified regexes with constant
// strings (this allows variable data such as timestamps to pass snapshot validation) and a
// generic CaptureValidator interface which summarizes capture output (this allows validation
// even of captures which are too large to fit entirely in memory, or other properties of
// interest).
type CaptureSpec struct {
	Driver       pc.DriverServer
	EndpointSpec interface{}
	Bindings     []*flow.CaptureSpec_Binding
	Checkpoint   json.RawMessage

	Validator  CaptureValidator
	Sanitizers map[string]*regexp.Regexp
	Errors     []error
}

// Validate performs validation against the target database.
func (cs *CaptureSpec) Validate(ctx context.Context, t testing.TB) ([]*pc.ValidateResponse_Binding, error) {
	t.Helper()

	endpointSpecJSON, err := json.Marshal(cs.EndpointSpec)
	require.NoError(t, err)

	validation, err := cs.Driver.Validate(ctx, &pc.ValidateRequest{EndpointSpecJson: endpointSpecJSON})
	if err != nil {
		return nil, err
	}
	return validation.Bindings, nil
}

// VerifyDiscover runs Discover and then performs snapshot verification on the result.
func (cs *CaptureSpec) VerifyDiscover(ctx context.Context, t testing.TB, matchers ...*regexp.Regexp) {
	t.Helper()

	var bindings = cs.Discover(ctx, t, matchers...)

	var summary = new(strings.Builder)
	for idx, binding := range bindings {
		fmt.Fprintf(summary, "Binding %d:\n", idx)
		bs, err := json.MarshalIndent(binding, "  ", "  ")
		require.NoError(t, err)
		io.Copy(summary, bytes.NewReader(bs))
		fmt.Fprintf(summary, "\n")
	}

	cupaloy.SnapshotT(t, summary.String())
}

// Discover performs catalog discovery against the target database and returns
// a sorted (by recommended name) list of all discovered bindings whose resource
// spec matches one of the provided regexes.
func (cs *CaptureSpec) Discover(ctx context.Context, t testing.TB, matchers ...*regexp.Regexp) []*pc.DiscoverResponse_Binding {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	endpointSpecJSON, err := json.Marshal(cs.EndpointSpec)
	require.NoError(t, err)

	discovery, err := cs.Driver.Discover(ctx, &pc.DiscoverRequest{
		EndpointSpecJson: endpointSpecJSON,
	})
	require.NoError(t, err)

	var matchedBindings = make(map[string]*pc.DiscoverResponse_Binding)
	var matchedNames []string
	for _, binding := range discovery.Bindings {
		if matchers == nil || matchesAny(matchers, string(binding.ResourceSpecJson)) {
			var name = binding.RecommendedName.String()
			matchedNames = append(matchedNames, name)
			matchedBindings[name] = binding
		}
	}
	sort.Strings(matchedNames)

	var resultBindings []*pc.DiscoverResponse_Binding
	for _, name := range matchedNames {
		resultBindings = append(resultBindings, matchedBindings[name])
	}
	return resultBindings
}

func matchesAny(matchers []*regexp.Regexp, text string) bool {
	for _, matcher := range matchers {
		if matcher.MatchString(text) {
			return true
		}
	}
	return false
}

// Capture performs data capture from the target database into the associated CaptureValidator,
// updating the state checkpoint and accumulating errors as appropriate.
func (cs *CaptureSpec) Capture(ctx context.Context, t testing.TB, callback func(data json.RawMessage)) *CaptureSpec {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}
	log.WithFields(log.Fields{
		"checkpoint": cs.Checkpoint,
	}).Debug("running test capture")

	endpointSpecJSON, err := json.Marshal(cs.EndpointSpec)
	require.NoError(t, err)

	var open = &pc.PullRequest{
		Open: &pc.PullRequest_Open{
			DriverCheckpointJson: cs.Checkpoint,
			Capture: &flow.CaptureSpec{
				Capture:          flow.Capture("acmeCo/" + strings.Replace(t.Name(), "/", "-", -1) + "capture"),
				EndpointSpecJson: endpointSpecJSON,
				Bindings:         cs.Bindings,
			},
		},
	}

	var adapter = &pullAdapter{
		ctx:        ctx,
		openReq:    open,
		checkpoint: cs.Checkpoint,
		bindings:   cs.Bindings,
		validator:  cs.Validator,
		sanitizers: cs.Sanitizers,
		callback:   callback,
	}
	if err := cs.Driver.Pull(adapter); err != nil {
		log.WithField("err", err).Error("capture terminated with error")
		if !errors.Is(err, context.Canceled) {
			cs.Errors = append(cs.Errors, err)
		}
	}
	cs.Checkpoint = adapter.checkpoint
	return cs
}

// Reset clears validator and error state, but leaves the state checkpoint intact.
func (cs *CaptureSpec) Reset() {
	cs.Validator.Reset()
	cs.Errors = nil
}

// Summary returns a human-readable summary of the capture output, current state checkpoint,
// and any errors encountered along the way.
func (cs *CaptureSpec) Summary() string {
	var w = new(strings.Builder)

	// Ask the output validator to summarize all observed output
	if err := cs.Validator.Summarize(w); err != nil {
		fmt.Fprintf(w, "# ================================\n")
		fmt.Fprintf(w, "# Error Summarizing Capture Output\n")
		fmt.Fprintf(w, "# ================================\n")
		fmt.Fprintf(w, "%v\n", err)
	}

	// Append the final state checkpoint
	fmt.Fprintf(w, "# ================================\n")
	fmt.Fprintf(w, "# Final State Checkpoint\n")
	fmt.Fprintf(w, "# ================================\n")
	fmt.Fprintf(w, "%s\n", sanitize(cs.Sanitizers, cs.Checkpoint))

	// If the error result is non-nil, add that to the summary as well
	if len(cs.Errors) != 0 {
		fmt.Fprintf(w, "# ================================\n")
		fmt.Fprintf(w, "# Captures Terminated With Errors\n")
		fmt.Fprintf(w, "# ================================\n")
		for _, err := range cs.Errors {
			fmt.Fprintf(w, "%v\n", err)
		}
	}
	return w.String()
}

type pullAdapter struct {
	ctx         context.Context
	openReq     *pc.PullRequest
	checkpoint  []byte
	bindings    []*flow.CaptureSpec_Binding
	validator   CaptureValidator
	callback    func(data json.RawMessage)
	sanitizers  map[string]*regexp.Regexp
	transaction []*pc.Documents
}

func (a *pullAdapter) Recv() (*pc.PullRequest, error) {
	if msg := a.openReq; msg != nil {
		a.openReq = nil
		return msg, nil
	}
	time.Sleep(10 * time.Millisecond)
	return &pc.PullRequest{Acknowledge: &pc.Acknowledge{}}, nil
}

func normalizeJSON(bs json.RawMessage) (json.RawMessage, error) {
	var x interface{}
	if err := json.Unmarshal(bs, &x); err != nil {
		return nil, err
	}
	return json.Marshal(x)
}

func (a *pullAdapter) Send(m *pc.PullResponse) error {
	if m.Documents != nil {
		a.transaction = append(a.transaction, m.Documents)
	}
	if m.Checkpoint != nil {
		if !m.Checkpoint.Rfc7396MergePatch || a.checkpoint == nil {
			a.checkpoint = m.Checkpoint.DriverCheckpointJson
		} else if checkpoint, err := jsonpatch.MergePatch(a.checkpoint, m.Checkpoint.DriverCheckpointJson); err != nil {
			return fmt.Errorf("test error merging checkpoint: %w", err)
		} else if normalized, err := normalizeJSON(checkpoint); err != nil {
			return fmt.Errorf("test error normalizing checkpoint: %w", err)
		} else {
			a.checkpoint = normalized
		}
		if logLevel := log.TraceLevel; log.IsLevelEnabled(logLevel) {
			if !bytes.Equal(m.Checkpoint.DriverCheckpointJson, []byte("{}")) {
				log.WithFields(log.Fields{
					"checkpoint": m.Checkpoint.DriverCheckpointJson,
					"patch":      m.Checkpoint.Rfc7396MergePatch,
					"result":     json.RawMessage(a.checkpoint),
				}).Log(logLevel, "checkpoint")
			}
		}

		for _, documents := range a.transaction {
			var binding string
			if idx := documents.Binding; 0 <= idx && int(idx) < len(a.bindings) {
				binding = strings.Join(a.bindings[idx].ResourcePath, "|")
			} else {
				binding = fmt.Sprintf("Invalid Binding %d", idx)
			}
			for _, slice := range documents.DocsJson {
				var doc = json.RawMessage(documents.Arena[slice.Begin:slice.End])
				a.validator.Output(binding, sanitize(a.sanitizers, doc))
				if a.callback != nil {
					a.callback(doc)
				}
			}
		}
		a.transaction = nil

		if a.callback != nil {
			a.callback(a.checkpoint)
		}
	}
	return nil
}

func (a *pullAdapter) Context() context.Context     { return a.ctx }
func (a *pullAdapter) SendMsg(m interface{}) error  { return nil }
func (a *pullAdapter) RecvMsg(m interface{}) error  { panic("RecvMsg is not supported") }
func (a *pullAdapter) SendHeader(metadata.MD) error { panic("SendHeader is not supported") }
func (a *pullAdapter) SetHeader(metadata.MD) error  { panic("SetHeader is not supported") }
func (a *pullAdapter) SetTrailer(metadata.MD)       { panic("SetTrailer is not supported") }

// A CaptureValidator represents a stateful component which can receive capture output
// documents and summarize the results upon request. The type of summarization performed
// can vary depending on the needs of a particular test.
type CaptureValidator interface {
	Output(collection string, data json.RawMessage)
	Summarize(w io.Writer) error
	Reset()
}

// SortedCaptureValidator maintains a list of every document emitted to each output
// collection, and returns the list in sorted order upon request.
type SortedCaptureValidator struct {
	documents map[string][]json.RawMessage // Map from collection name to list of documents
}

// Output feeds a new document into the CaptureValidator.
func (v *SortedCaptureValidator) Output(collection string, data json.RawMessage) {
	if v.documents == nil {
		v.documents = make(map[string][]json.RawMessage)
	}
	v.documents[collection] = append(v.documents[collection], data)
}

// Summarize writes a human-readable / snapshottable summary of the documents observed by the CaptureValidator.
func (v *SortedCaptureValidator) Summarize(w io.Writer) error {
	var collections []string
	for collection := range v.documents {
		collections = append(collections, collection)
	}
	sort.Strings(collections)

	for _, collection := range collections {
		// Take the original sequence of output documents, use a map to deduplicate them,
		// and then put them back into a list in and sort it.
		var inputDocs = v.documents[collection]
		var uniqueDocs = make(map[string]struct{}, len(inputDocs))
		for _, doc := range inputDocs {
			uniqueDocs[string(doc)] = struct{}{}
		}
		var sortedDocs = make([]string, 0, len(uniqueDocs))
		for doc := range uniqueDocs {
			sortedDocs = append(sortedDocs, doc)
		}
		sort.Strings(sortedDocs)

		fmt.Fprintf(w, "# ================================\n")
		fmt.Fprintf(w, "# Collection %q: %d Documents\n", collection, len(sortedDocs))
		fmt.Fprintf(w, "# ================================\n")
		for _, doc := range sortedDocs {
			fmt.Fprintf(w, "%s\n", doc)
		}
	}
	return nil
}

// Reset clears the internal state of the CaptureValidator.
func (v *SortedCaptureValidator) Reset() {
	v.documents = nil
}

// ChecksumValidator receives documents and reduces them into a final checksum. Useful for
// snapshotting large collections where a human-readable output is not feasible.
type ChecksumValidator struct {
	collections map[string]*checksumValidatorState
}

type checksumValidatorState struct {
	count    int
	checksum [32]byte
}

func (v *ChecksumValidator) Output(collection string, data json.RawMessage) {
	if v.collections == nil {
		v.collections = make(map[string]*checksumValidatorState)
	}
	state, ok := v.collections[collection]
	if !ok {
		state = &checksumValidatorState{}
		v.collections[collection] = state
	}
	state.reduce(data)
}

func (s *checksumValidatorState) reduce(data json.RawMessage) {
	s.count++
	var docSum = sha256.Sum256([]byte(data))
	for idx := range s.checksum {
		s.checksum[idx] ^= docSum[idx]
	}
}

func (v *ChecksumValidator) Summarize(w io.Writer) error {
	var names []string
	for name := range v.collections {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		fmt.Fprintf(w, "# ================================\n")
		fmt.Fprintf(w, "# Collection %q: %d Documents\n", name, v.collections[name].count)
		fmt.Fprintf(w, "# ================================\n")
		fmt.Fprintf(w, "Checksum: %x\n", v.collections[name].checksum)
	}
	return nil
}

func (v *ChecksumValidator) Reset() {
	v.collections = nil
}

// WatchdogValidator wraps a CaptureValidator and resets a timer after any output is received. This
// timer can be used as a condition for terminating a test due to lack of output when the capture
// context needs to be restarted for other reasons.
type WatchdogValidator struct {
	Inner CaptureValidator

	WatchdogTimer *time.Timer
	ResetPeriod   time.Duration
}

func (v *WatchdogValidator) Output(collection string, data json.RawMessage) {
	v.WatchdogTimer.Reset(v.ResetPeriod)
	v.Inner.Output(collection, data)
}

func (v *WatchdogValidator) Summarize(w io.Writer) error { return v.Inner.Summarize(w) }
func (v *WatchdogValidator) Reset()                      { v.Inner.Reset() }

func sanitize(sanitizers map[string]*regexp.Regexp, data json.RawMessage) json.RawMessage {
	var bs = []byte(data)
	for replacement, matcher := range sanitizers {
		bs = matcher.ReplaceAll(bs, []byte(replacement))
	}
	return json.RawMessage(bs)
}

// DefaultSanitizers is a collection of generic capture output sanitizers which should
// be applied to any type of database's captures.
var DefaultSanitizers = map[string]*regexp.Regexp{
	`"<TIMESTAMP>"`: regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|-[0-9]+:[0-9]+)"`),
}

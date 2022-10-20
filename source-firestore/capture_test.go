package main

import (
	"bytes"
	"context"
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

type testCaptureSpec struct {
	Driver       pc.DriverServer
	EndpointSpec interface{}
	Bindings     []*flow.CaptureSpec_Binding
	Checkpoint   json.RawMessage

	Validator  captureValidator
	Sanitizers map[string]*regexp.Regexp
	Errors     []error
}

func (cs *testCaptureSpec) Discover(ctx context.Context, t testing.TB, matchers ...*regexp.Regexp) {
	t.Helper()
	if os.Getenv("RUN_CAPTURES") != "yes" {
		t.Skipf("skipping %q capture: ${RUN_CAPTURES} != \"yes\"", t.Name())
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

	var summary = new(strings.Builder)
	for idx, name := range matchedNames {
		fmt.Fprintf(summary, "Binding %d:\n", idx)
		bs, err := json.MarshalIndent(matchedBindings[name], "  ", "  ")
		require.NoError(t, err)
		io.Copy(summary, bytes.NewReader(bs))
		fmt.Fprintf(summary, "\n")

	}
	cupaloy.SnapshotT(t, summary.String())
}

func matchesAny(matchers []*regexp.Regexp, text string) bool {
	for _, matcher := range matchers {
		if matcher.MatchString(text) {
			return true
		}
	}
	return false
}

func (cs *testCaptureSpec) Capture(ctx context.Context, t testing.TB, sentinel string) *testCaptureSpec {
	t.Helper()
	if os.Getenv("RUN_CAPTURES") != "yes" {
		t.Skipf("skipping %q capture: ${RUN_CAPTURES} != \"yes\"", t.Name())
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
			Tail: true,
		},
	}

	ctx, shutdown := context.WithCancel(ctx)
	var adapter = &testPullAdapter{
		ctx:           ctx,
		openReq:       open,
		checkpoint:    cs.Checkpoint,
		bindings:      cs.Bindings,
		validator:     cs.Validator,
		sanitizers:    cs.Sanitizers,
		sentinelValue: sentinel,
		shutdown:      shutdown,
	}
	if err := cs.Driver.Pull(adapter); err != nil && !errors.Is(err, context.Canceled) {
		cs.Errors = append(cs.Errors, err)
	}
	cs.Checkpoint = adapter.checkpoint
	return cs
}

func (cs *testCaptureSpec) Verify(t testing.TB) {
	t.Helper()
	cupaloy.SnapshotT(t, cs.Summary())
	cs.Validator.Reset()
	cs.Errors = nil
}

func (cs *testCaptureSpec) Summary() string {
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

type testPullAdapter struct {
	ctx         context.Context
	openReq     *pc.PullRequest
	checkpoint  []byte
	bindings    []*flow.CaptureSpec_Binding
	validator   captureValidator
	sanitizers  map[string]*regexp.Regexp
	transaction []*pc.Documents

	// After the sentinel value is observed in an output document, the next state
	// checkpoint will cause the shutdown thunk to be invoked.
	sentinelReached bool
	sentinelValue   string
	shutdown        func()
}

func (a *testPullAdapter) Recv() (*pc.PullRequest, error) {
	if msg := a.openReq; msg != nil {
		a.openReq = nil
		return msg, nil
	}
	time.Sleep(10 * time.Millisecond)
	return &pc.PullRequest{Acknowledge: &pc.Acknowledge{}}, nil
}

func (a *testPullAdapter) Send(m *pc.PullResponse) error {
	if m.Documents != nil {
		a.transaction = append(a.transaction, m.Documents)
	}
	if m.Checkpoint != nil {
		if !m.Checkpoint.Rfc7396MergePatch || a.checkpoint == nil {
			a.checkpoint = m.Checkpoint.DriverCheckpointJson
		} else if checkpoint, err := jsonpatch.MergePatch(a.checkpoint, m.Checkpoint.DriverCheckpointJson); err == nil {
			a.checkpoint = checkpoint
		} else {
			return fmt.Errorf("test error merging checkpoint: %w", err)
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
			for _, slice := range documents.DocsJson {
				var doc = json.RawMessage(documents.Arena[slice.Begin:slice.End])
				var binding string
				if idx := documents.Binding; 0 <= idx && int(idx) < len(a.bindings) {
					binding = strings.Join(a.bindings[idx].ResourcePath, "|")
				} else {
					binding = fmt.Sprintf("Invalid Binding %d", idx)
				}
				if !a.sentinelReached && bytes.Contains(doc, []byte(a.sentinelValue)) {
					a.sentinelReached = true
				}
				a.validator.Output(binding, sanitize(a.sanitizers, doc))
			}
		}
		a.transaction = nil

		if a.sentinelReached && !bytes.Equal(m.Checkpoint.DriverCheckpointJson, []byte("{}")) {
			a.shutdown()
		}
	}
	return nil
}

func (a *testPullAdapter) Context() context.Context     { return a.ctx }
func (a *testPullAdapter) SendMsg(m interface{}) error  { return nil }
func (a *testPullAdapter) RecvMsg(m interface{}) error  { panic("RecvMsg is not supported") }
func (a *testPullAdapter) SendHeader(metadata.MD) error { panic("SendHeader is not supported") }
func (a *testPullAdapter) SetHeader(metadata.MD) error  { panic("SetHeader is not supported") }
func (a *testPullAdapter) SetTrailer(metadata.MD)       { panic("SetTrailer is not supported") }

type captureValidator interface {
	Output(collection string, data json.RawMessage)
	Summarize(w io.Writer) error
	Reset()
}

type sortedCaptureValidator struct {
	documents map[string][]json.RawMessage // Map from collection name to list of documents
}

func (v *sortedCaptureValidator) Output(collection string, data json.RawMessage) {
	if v.documents == nil {
		v.documents = make(map[string][]json.RawMessage)
	}
	v.documents[collection] = append(v.documents[collection], data)
}

func (v *sortedCaptureValidator) Summarize(w io.Writer) error {
	var collections []string
	for collection := range v.documents {
		collections = append(collections, collection)
	}
	sort.Strings(collections)

	for _, collection := range collections {
		var docs = make([]json.RawMessage, len(v.documents[collection]))
		copy(docs, v.documents[collection])
		sort.Slice(docs, func(i, j int) bool { return bytes.Compare(docs[i], docs[j]) < 0 })

		fmt.Fprintf(w, "# ================================\n")
		fmt.Fprintf(w, "# Collection %q: %d Documents\n", collection, len(docs))
		fmt.Fprintf(w, "# ================================\n")
		for _, doc := range docs {
			fmt.Fprintf(w, "%s\n", string(doc))
		}
	}
	return nil
}

func (v *sortedCaptureValidator) Reset() {
	v.documents = nil
}

func sanitize(sanitizers map[string]*regexp.Regexp, data json.RawMessage) json.RawMessage {
	var bs = []byte(data)
	for replacement, matcher := range sanitizers {
		bs = matcher.ReplaceAll(bs, []byte(replacement))
	}
	return json.RawMessage(bs)
}

var DefaultSanitizers = map[string]*regexp.Regexp{
	`"<TIMESTAMP>"`: regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]*Z"`),
}

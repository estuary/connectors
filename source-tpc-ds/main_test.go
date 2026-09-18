package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func requireDsdgen(t *testing.T) {
	if _, err := newGenerator("1"); err != nil {
		t.Skipf("skipping: %v", err)
	}
}

func allBindings(t *testing.T) []*flow.CaptureSpec_Binding {
	var out []*flow.CaptureSpec_Binding
	for _, tbl := range tables {
		res, err := json.Marshal(resource{Table: tbl.Name})
		require.NoError(t, err)
		out = append(out, &flow.CaptureSpec_Binding{
			ResourceConfigJson: res,
			ResourcePath:       []string{tbl.Name},
			Collection:         flow.CollectionSpec{Name: flow.Collection("acmeCo/tpcds/" + tbl.Name)},
			StateKey:           tbl.Name,
		})
	}
	return out
}

func TestSpec(t *testing.T) {
	resp, err := (&st.CaptureSpec{Driver: new(driver)}).Spec(context.Background())
	require.NoError(t, err)
	bs, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(bs))
}

func TestDiscover(t *testing.T) {
	t.Setenv("TEST_DATABASE", "yes")
	var cs = &st.CaptureSpec{Driver: new(driver), EndpointSpec: &config{Scale: 1}}
	cs.VerifyDiscover(context.Background(), t)
}

func TestValidate(t *testing.T) {
	var cs = &st.CaptureSpec{Driver: new(driver), EndpointSpec: &config{Scale: 1}, Bindings: allBindings(t)}
	validated, err := cs.Validate(context.Background(), t)
	require.NoError(t, err)
	require.Len(t, validated, 24)

	bad, err := json.Marshal(resource{Table: "nope"})
	require.NoError(t, err)
	cs.Bindings = []*flow.CaptureSpec_Binding{{ResourceConfigJson: bad}}
	_, err = cs.Validate(context.Background(), t)
	require.ErrorContains(t, err, `unknown TPC-DS table "nope"`)

	cs.Bindings = nil
	cs.EndpointSpec = &config{Scale: 0}
	_, err = cs.Validate(context.Background(), t)
	require.ErrorContains(t, err, "scale factor must be a positive number")
	cs.EndpointSpec = &config{Scale: 0.001}
	_, err = cs.Validate(context.Background(), t)
	require.ErrorContains(t, err, "scale factor must be at least 0.01")
}

func TestCapture(t *testing.T) {
	t.Setenv("TEST_DATABASE", "yes")
	requireDsdgen(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var validator = &digestValidator{onAllDone: cancel, bindings: 24}
	var cs = &st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: &config{Scale: 0.01},
		Bindings:     allBindings(t),
		Validator:    validator,
	}
	cs.Capture(ctx, t, nil)
	cupaloy.SnapshotT(t, cs.Summary())
}

func TestRestartAfterCompletion(t *testing.T) {
	t.Setenv("TEST_DATABASE", "yes")
	requireDsdgen(t)

	var done = state{Bindings: map[boilerplate.StateKey]*bindingState{}}
	for _, tbl := range tables {
		done.Bindings[boilerplate.StateKey(tbl.Name)] = &bindingState{Scale: "0.01", Chunks: 1, Completed: 1, Done: true}
	}
	checkpoint, err := json.Marshal(done)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var validator = &digestValidator{bindings: 24}
	var cs = &st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: &config{Scale: 0.01},
		Bindings:     allBindings(t),
		Validator:    validator,
		Checkpoint:   checkpoint,
	}
	cs.Capture(ctx, t, nil)
	require.Empty(t, cs.Errors)
	require.Empty(t, validator.collections)
}

func TestScaleChangeFails(t *testing.T) {
	t.Setenv("TEST_DATABASE", "yes")
	requireDsdgen(t)

	checkpoint, err := json.Marshal(state{Bindings: map[boilerplate.StateKey]*bindingState{
		"store_sales": {Scale: "1", Chunks: 1, Completed: 1, Done: true},
	}})
	require.NoError(t, err)
	var cs = &st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: &config{Scale: 0.5},
		Bindings:     allBindings(t),
		Validator:    &digestValidator{bindings: 24},
		Checkpoint:   checkpoint,
	}
	cs.Capture(context.Background(), t, nil)
	require.Len(t, cs.Errors, 1)
	require.ErrorContains(t, cs.Errors[0], "generated at scale factor 1 but the endpoint is now configured for 0.5; backfill")
}

type digestValidator struct {
	collections map[string]*digestState
	bindings    int
	onAllDone   func()
	allDone     bool
}

type digestState struct {
	docs  int
	hash  hash.Hash
	first []json.RawMessage
}

func (v *digestValidator) Output(collection string, data json.RawMessage) {
	if v.collections == nil {
		v.collections = map[string]*digestState{}
	}
	s, ok := v.collections[collection]
	if !ok {
		s = &digestState{hash: sha256.New()}
		v.collections[collection] = s
	}
	s.docs++
	s.hash.Write(data)
	s.hash.Write([]byte{'\n'})
	if len(s.first) < 3 {
		s.first = append(s.first, append(json.RawMessage(nil), data...))
	}
}

func (v *digestValidator) Checkpoint(data json.RawMessage) {
	if v.allDone || v.onAllDone == nil {
		return
	}
	var s state
	if err := json.Unmarshal(data, &s); err != nil {
		return
	}
	var done int
	for _, b := range s.Bindings {
		if b.Done {
			done++
		}
	}
	if done == v.bindings {
		v.allDone = true
		v.onAllDone()
	}
}

func (v *digestValidator) SourcedSchema(collection string, schema json.RawMessage) {}

func (v *digestValidator) Summarize(w io.Writer) error {
	var names []string
	for name := range v.collections {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		var s = v.collections[name]
		fmt.Fprintf(w, "# ================================\n")
		fmt.Fprintf(w, "# Collection %q: %d Documents\n", name, s.docs)
		fmt.Fprintf(w, "# ================================\n")
		fmt.Fprintf(w, "Digest: %x\n", s.hash.Sum(nil))
		for _, doc := range s.first {
			fmt.Fprintf(w, "%s\n", doc)
		}
	}
	return nil
}

func (v *digestValidator) Reset() {
	v.collections = nil
	v.allDone = false
}

func TestChunkedMatchesUnchunked(t *testing.T) {
	t.Setenv("TEST_DATABASE", "yes")
	requireDsdgen(t)

	var saved = chunkTargetRows
	chunkTargetRows = 500
	t.Cleanup(func() { chunkTargetRows = saved })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var validator = &digestValidator{onAllDone: cancel, bindings: 24}
	var cs = &st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: &config{Scale: 0.01},
		Bindings:     allBindings(t),
		Validator:    validator,
	}
	cs.Capture(ctx, t, nil)
	require.Empty(t, cs.Errors)

	var final state
	require.NoError(t, json.Unmarshal(cs.Checkpoint, &final))
	require.Greater(t, final.Bindings["store_sales"].Chunks, 1)
	require.Equal(t, final.Bindings["store_sales"].Chunks, final.Bindings["store_returns"].Chunks)

	var w strings.Builder
	require.NoError(t, validator.Summarize(&w))
	require.Equal(t, captureSnapshotDigests(t), digestsOf(w.String()))
}

func TestResume(t *testing.T) {
	t.Setenv("TEST_DATABASE", "yes")
	requireDsdgen(t)

	var saved = chunkTargetRows
	chunkTargetRows = 500
	t.Cleanup(func() { chunkTargetRows = saved })

	var validator = &digestValidator{bindings: 24}
	var cs = &st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: &config{Scale: 0.01},
		Bindings:     allBindings(t),
		Validator:    validator,
	}

	ctx, cancel := context.WithCancel(context.Background())
	var seen int
	cs.Capture(ctx, t, func(json.RawMessage) {
		if seen++; seen == 60_000 {
			cancel()
		}
	})
	require.Empty(t, cs.Errors)
	var mid state
	require.NoError(t, json.Unmarshal(cs.Checkpoint, &mid))
	var partial int
	for _, b := range mid.Bindings {
		if !b.Done {
			partial++
		}
	}
	require.Greater(t, partial, 0, "interruption happened after everything completed")

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	validator.onAllDone = cancel
	cs.Capture(ctx, t, nil)
	require.Empty(t, cs.Errors)

	var w strings.Builder
	require.NoError(t, validator.Summarize(&w))
	require.Equal(t, captureSnapshotDigests(t), digestsOf(w.String()))
}

func captureSnapshotDigests(t *testing.T) map[string]string {
	bs, err := os.ReadFile(".snapshots/TestCapture")
	require.NoError(t, err)
	var d = digestsOf(string(bs))
	require.Len(t, d, 24)
	return d
}

var reDigest = regexp.MustCompile(`# Collection "([^"]+)": (\d+) Documents\n# =+\nDigest: ([0-9a-f]+)`)

func digestsOf(summary string) map[string]string {
	var out = map[string]string{}
	for _, m := range reDigest.FindAllStringSubmatch(summary, -1) {
		out[m[1]] = m[2] + " " + m[3]
	}
	return out
}

func TestReturnsWithoutParent(t *testing.T) {
	t.Setenv("TEST_DATABASE", "yes")
	requireDsdgen(t)

	var bindings []*flow.CaptureSpec_Binding
	for _, b := range allBindings(t) {
		if b.StateKey == "store_returns" {
			bindings = append(bindings, b)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var validator = &digestValidator{onAllDone: cancel, bindings: 1}
	var cs = &st.CaptureSpec{
		Driver:       new(driver),
		EndpointSpec: &config{Scale: 0.01},
		Bindings:     bindings,
		Validator:    validator,
	}
	cs.Capture(ctx, t, nil)
	require.Empty(t, cs.Errors)

	var w strings.Builder
	require.NoError(t, validator.Summarize(&w))
	var got = digestsOf(w.String())
	require.Len(t, got, 1)
	require.Equal(t, captureSnapshotDigests(t)["acmeCo/tpcds/store_returns"], got["acmeCo/tpcds/store_returns"])
}

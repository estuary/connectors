package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	firestore "cloud.google.com/go/firestore"
	"github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
)

// The "tests" in this file aren't intended to be run as part of a CI build or other
// lightweight process, instead they are heavyweight "tools" meant to be run by a
// human in order to verify that we can in fact ingest massive quantities of data
// without anything breaking.
//
// They're written as 'go test' compatible tests because it's convenient and they
// reuse most of the same infrastructure, but they skip themselves unless the
// environment variable DATASYNTH is set to "yes".
//
// These tools are incredibly unpolished, the only reason I'm checking them in
// at all is because they're nicely contained within a single file and I'm hoping
// that by checking this in I can find it in the future if/when I have to do some-
// thing similar.
//
//   TestSyntheticData: Writes a whole bunch of documents into a Firestore database.
//   By default this produces a collection of 500k documents and another of 5M, but
//   it should be fairly obvious how to extend this. Note that this process takes
//   about an hour per 1M documents, although it could probably be sped up by dividing
//   it across multiple worker threads.
//
//   TestSyntheticClutter: Extends the dataset produced by TestSyntheticData by adding
//   an absolute *ton* of 'clutter' objects. This increases the overall size of the
//   dataset by another 10x, just to really stress-test things. The new objects are
//   added to a nested collection 'groups/*/users/*/events/*/clutter', so the checksums
//   of the other collections remain unchanged.
//
//   TestMassiveBackfill: Performs an asynchronous backfill of the synthetic dataset
//   and verifies that it looks correct.

const (
	numGroups          = 1000 // 1k groups
	numUsersPerGroup   = 500  // 500k users
	numEventsPerUser   = 10   // 5M events
	numClutterPerEvent = 10   // 50M clutter objects
)

func TestSyntheticData(t *testing.T) {
	if os.Getenv("DATASYNTH") != "yes" {
		t.Skipf("skipping test %q: see datasynth_test.go for explanation", t.Name())
	}

	var ctx, cancel = context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var credentialsPath = strings.ReplaceAll(*testCredentialsPath, "~", os.Getenv("HOME"))
	credentialsJSON, err := ioutil.ReadFile(credentialsPath)
	require.NoError(t, err)

	client, err := firestore.NewClient(ctx, *testProjectID, option.WithCredentialsJSON(credentialsJSON))
	require.NoError(t, err)

	var batcher = &datasynthBatcher{inner: client}

	for group := 0; group < numGroups; group++ {
		log.WithField("group", group).Info("generating group")
		batcher.Write(ctx, t, fmt.Sprintf(`groups/%d`, group), map[string]interface{}{
			"type":           "group",
			"version":        1,
			"name":           fmt.Sprintf("Group Number %d", group),
			"description":    fmt.Sprintf("An object representing a 'group' in some synthetic dataset. This one is group number %d.", group),
			"sequenceNumber": group,
			"properties": map[string]interface{}{
				"foo": rand.Intn(1024),
				"bar": strings.Repeat(string([]byte{byte('A' + rand.Intn(26))}), rand.Intn(1024)),
			},
		})
		for user := 0; user < numUsersPerGroup; user++ {
			log.WithFields(log.Fields{
				"group": group,
				"user":  user,
			}).Debug("generating user")
			batcher.Write(ctx, t, fmt.Sprintf(`groups/%d/users/%d`, group, user), map[string]interface{}{
				"type":           "user",
				"version":        1,
				"name":           fmt.Sprintf("User Number %d", group),
				"description":    fmt.Sprintf("An object representing a 'user' in some synthetic dataset. This one is user %d of group %d.", user, group),
				"sequenceNumber": user,
				"properties": map[string]interface{}{
					"foo": rand.Intn(1024),
					"bar": strings.Repeat(string([]byte{byte('A' + rand.Intn(26))}), rand.Intn(16)) + strings.Repeat(string([]byte{byte('A' + rand.Intn(26))}), rand.Intn(16)),
				},
			})
			for event := 0; event < numEventsPerUser; event++ {
				log.WithFields(log.Fields{
					"group": group,
					"user":  user,
					"event": event,
				}).Debug("generating event")
				batcher.Write(ctx, t, fmt.Sprintf(`groups/%d/users/%d/events/%d`, group, user, event), map[string]interface{}{
					"type":           "event",
					"name":           fmt.Sprintf("Event Number %d", group),
					"description":    fmt.Sprintf("An object representing an 'event' in some synthetic dataset. This one is event %d of user %d of group %d.", event, user, group),
					"sequenceNumber": event,
					"state":          "completed",
					"version":        2,
					"extensions": []interface{}{
						map[string]interface{}{
							"type":      "extension asdf",
							"version":   1,
							"timestamp": time.Now().Format(time.RFC3339Nano),
							"data":      strings.Repeat(string([]byte{byte('A' + rand.Intn(26))}), rand.Intn(16)) + strings.Repeat(string([]byte{byte('A' + rand.Intn(26))}), rand.Intn(16)),
						},
						map[string]interface{}{
							"type":      "extension fdsa",
							"version":   2,
							"timestamp": time.Now().Format(time.RFC3339Nano),
							"data":      strings.Repeat(string([]byte{byte('A' + rand.Intn(26))}), rand.Intn(16)) + strings.Repeat(string([]byte{byte('A' + rand.Intn(26))}), rand.Intn(16)),
						},
					},
					"properties": map[string]interface{}{
						"foo": rand.Intn(1024),
						"bar": strings.Repeat(string([]byte{byte('A' + rand.Intn(26))}), rand.Intn(16)) + strings.Repeat(string([]byte{byte('A' + rand.Intn(26))}), rand.Intn(16)),
					},
				})
			}
		}
	}
	batcher.Flush(ctx, t)
}

func TestSyntheticClutter(t *testing.T) {
	if os.Getenv("DATASYNTH") != "yes" {
		t.Skipf("skipping test %q: see datasynth_test.go for explanation", t.Name())
	}

	var ctx, cancel = context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var credentialsPath = strings.ReplaceAll(*testCredentialsPath, "~", os.Getenv("HOME"))
	credentialsJSON, err := ioutil.ReadFile(credentialsPath)
	require.NoError(t, err)

	client, err := firestore.NewClient(ctx, *testProjectID, option.WithCredentialsJSON(credentialsJSON))
	require.NoError(t, err)

	var eg = new(errgroup.Group)
	eg.SetLimit(4)
	for group := 0; group < numGroups; group++ {
		group := group
		log.WithField("group", group).Info("spawning worker")
		eg.Go(func() error {
			var batcher = &datasynthBatcher{inner: client}
			for user := 0; user < numUsersPerGroup; user++ {
				for event := 0; event < numEventsPerUser; event++ {
					for clutter := 0; clutter < numClutterPerEvent; clutter++ {
						batcher.Write(ctx, t, fmt.Sprintf(`groups/%d/users/%d/events/%d/clutter/%d`, group, user, event, clutter), map[string]interface{}{
							"type":           "clutter",
							"name":           fmt.Sprintf("Clutter Number %d", group),
							"description":    fmt.Sprintf("A very nested object in a synthetic dataset. This one is clutter object %d of event %d of user %d of group %d.", clutter, event, user, group),
							"sequenceNumber": clutter,
							"version":        1,
							"timestamp":      time.Now().Format(time.RFC3339Nano),
							"foo":            rand.Intn(1024),
							"bar":            strings.Repeat(string([]byte{byte('A' + rand.Intn(26))}), rand.Intn(16)) + strings.Repeat(string([]byte{byte('A' + rand.Intn(26))}), rand.Intn(16)),
							"baz":            strings.Repeat(string([]byte{byte('A' + rand.Intn(26))}), rand.Intn(16)) + strings.Repeat(string([]byte{byte('A' + rand.Intn(26))}), rand.Intn(16)),
						})
					}
				}
			}
			batcher.Flush(ctx, t)
			return nil
		})
	}
	eg.Wait()
}

const datasynthBatchSize = 500

type datasynthBatcher struct {
	inner *firestore.Client
	batch *firestore.WriteBatch
	count int
}

func (db *datasynthBatcher) Write(ctx context.Context, t testing.TB, key string, data interface{}) {
	t.Helper()
	if db.batch == nil {
		db.batch = db.inner.Batch()
		db.count = 0
	}
	db.batch = db.batch.Set(db.inner.Doc(key), data, firestore.MergeAll)
	db.count++
	db.MaybeFlush(ctx, t)
}

func (db *datasynthBatcher) MaybeFlush(ctx context.Context, t testing.TB) {
	t.Helper()
	if db.count >= datasynthBatchSize {
		db.Flush(ctx, t)
	}
}

func (db *datasynthBatcher) Flush(ctx context.Context, t testing.TB) {
	t.Helper()
	if db.count > 0 {
		var _, err = db.batch.Commit(ctx)
		require.NoError(t, err)
		db.batch = nil
		db.count = 0
	}
}

func TestMassiveBackfill(t *testing.T) {
	if os.Getenv("DATASYNTH") != "yes" {
		t.Skipf("skipping test %q: see datasynth_test.go for explanation", t.Name())
	}

	// Set up a capture which will use async backfills to ingest the massive dataset
	// created by TestSyntheticData.
	var capture = simpleCapture(t)
	capture.Bindings = nil
	for _, name := range []string{
		"groups",
		"groups/*/users",
		"groups/*/users/*/events",
	} {
		capture.Bindings = append(capture.Bindings, &flow.CaptureSpec_Binding{
			ResourceSpecJson: json.RawMessage(fmt.Sprintf(`{"path": %q, "backfillMode": "async"}`, name)),
			ResourcePath:     []string{name},
		})
	}

	// Set up a watchdog timeout which will terminate the overall context after nothing
	// has been captured for 120s. The WDT will be fed via the `watchdogValidator` wrapper
	// around the actual capture output validator.
	const quiescentTimeout = 120 * time.Second
	var ctx, cancel = context.WithCancel(context.Background())
	var wdt = time.AfterFunc(quiescentTimeout, func() {
		log.WithField("timeout", quiescentTimeout.String()).Debug("capture watchdog timeout expired")
		cancel()
	})
	capture.Validator = &watchdogValidator{
		inner:       &checksumValidator{},
		wdt:         wdt,
		resetPeriod: quiescentTimeout,
	}

	// Run the capture and verify the results. The capture is killed every 30s and
	// then restarted from the previous checkpoint, in order to exercise incremental
	// backfill resuming behavior. Since the backfill takes about 1500s to complete
	// this should restart about 50 times along the way.
	for ctx.Err() == nil {
		var ctx, cancel = context.WithCancel(ctx)
		time.AfterFunc(30*time.Second, cancel)
		capture.Capture(ctx, t, "THIS-SENTINEL-VALUE-SHOULD-NEVER-EXIST")
	}
	capture.Verify(t)
}

type watchdogValidator struct {
	inner captureValidator

	wdt         *time.Timer
	resetPeriod time.Duration
}

func (v *watchdogValidator) Output(collection string, data json.RawMessage) {
	v.wdt.Reset(v.resetPeriod)
	v.inner.Output(collection, data)
}

func (v *watchdogValidator) Summarize(w io.Writer) error { return v.inner.Summarize(w) }
func (v *watchdogValidator) Reset()                      { v.inner.Reset() }

type checksumValidator struct {
	collections map[string]*checksumValidatorState
}

type checksumValidatorState struct {
	count    int
	checksum [32]byte
}

func (v *checksumValidator) Output(collection string, data json.RawMessage) {
	if v.collections == nil {
		v.collections = make(map[string]*checksumValidatorState)
	}
	state, ok := v.collections[collection]
	if !ok {
		state = &checksumValidatorState{}
		v.collections[collection] = state
	}
	state.Reduce(data)
}

func (s *checksumValidatorState) Reduce(data json.RawMessage) {
	s.count++
	var docSum = sha256.Sum256([]byte(data))
	for idx := range s.checksum {
		s.checksum[idx] ^= docSum[idx]
	}
}

func (v *checksumValidator) Summarize(w io.Writer) error {
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

func (v *checksumValidator) Reset() {
	v.collections = nil
}

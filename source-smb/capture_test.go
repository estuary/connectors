package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/estuary/connectors/filesource"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestPullResume(t *testing.T) {
	// Pass JSON lines through a stand-in parser to exercise listing and checkpoint
	// recovery without an installed flow-parser. The end-to-end test covers CSV parsing.
	var bin = t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(bin, "flow-parser"), []byte("#!/bin/sh\nexec cat\n"), 0700))
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	var modified = time.Now().Add(-2 * time.Hour).Truncate(time.Second).UTC()
	var updated = modified.Add(time.Hour)
	var fs = fstest.MapFS{
		"inbox/a.jsonl": &fstest.MapFile{
			Data:    []byte("{\"id\":\"1\",\"canary\":\"a1\"}\n{\"id\":\"2\",\"canary\":\"a2\"}\n{\"id\":\"3\",\"canary\":\"a3\"}\n"),
			ModTime: modified,
		},
		"inbox/sub/b.jsonl": &fstest.MapFile{
			Data:    []byte("{\"id\":\"4\",\"canary\":\"b4\"}\n{\"id\":\"5\",\"canary\":\"b5\"}\n"),
			ModTime: modified,
		},
		"inbox/notes.txt": &fstest.MapFile{
			Data:    []byte("not a captured file\n"),
			ModTime: updated,
		},
	}

	run := func(state json.RawMessage, expectedPaths []string, expectedDocs string) *captureStream {
		t.Helper()
		var driver = newDriver()
		cfg, err := driver.NewConfig(json.RawMessage(testConfig))
		require.NoError(t, err)
		var endpoint = cfg.(config)
		endpoint.MatchFiles = `.*\.jsonl$`
		raw, err := json.Marshal(endpoint)
		require.NoError(t, err)

		var store = &captureStore{tree: tree{fs: fs}}
		driver.Connect = func(context.Context, filesource.Config) (filesource.Store, error) {
			return store, nil
		}
		var stream = &captureStream{}
		require.NoError(t, driver.Pull(&pc.Request_Open{
			Capture: &pf.CaptureSpec{
				ConfigJson: raw,
				Bindings: []*pf.CaptureSpec_Binding{{
					ResourceConfigJson: json.RawMessage(`{"stream":"/inbox"}`),
					StateKey:           "inbox",
				}},
			},
			StateJson: state,
			Range:     &pf.RangeSpec{KeyEnd: ^uint32(0), RClockEnd: ^uint32(0)},
		}, &boilerplate.PullOutput{Connector_CaptureServer: stream, NumBindings: 1}))
		require.Equal(t, expectedPaths, store.opened)
		require.Equal(t, len(expectedPaths), store.closedFiles)
		require.Equal(t, 1, store.closed)
		require.JSONEq(t, expectedDocs, "["+strings.Join(stream.docs, ",")+"]")
		require.NotEmpty(t, stream.checkpoints)
		return stream
	}

	var first = run(nil, []string{"/inbox/a.jsonl", "/inbox/sub/b.jsonl"}, `[
		{"id":"1","canary":"a1"}, {"id":"2","canary":"a2"}, {"id":"3","canary":"a3"},
		{"id":"4","canary":"b4"}, {"id":"5","canary":"b5"}
	]`)
	var state = first.checkpoints[len(first.checkpoints)-1]
	var checkpoint filesource.CaptureState
	require.NoError(t, json.Unmarshal(state, &checkpoint))
	require.Equal(t, modified, checkpoint.States["inbox"].MinBound)
	require.Nil(t, checkpoint.States["inbox"].MaxBound)
	run(state, nil, `[]`)

	// An interrupted sweep resumes after its last completed file.
	var partial json.RawMessage
	for _, candidate := range first.checkpoints {
		require.NoError(t, json.Unmarshal(candidate, &checkpoint))
		if progress := checkpoint.States["inbox"]; progress.Path == "/inbox/a.jsonl" && progress.Complete {
			partial = candidate
			break
		}
	}
	require.NotEmpty(t, partial)
	run(partial, []string{"/inbox/sub/b.jsonl"}, `[{"id":"4","canary":"b4"}, {"id":"5","canary":"b5"}]`)

	fs["inbox/c.jsonl"] = &fstest.MapFile{
		Data:    []byte("{\"id\":\"6\",\"canary\":\"c6\"}\n{\"id\":\"7\",\"canary\":\"c7\"}\n"),
		ModTime: updated,
	}
	fs["inbox/sub/b.jsonl"].Data = []byte("{\"id\":\"4\",\"canary\":\"b4-v2\"}\n{\"id\":\"5\",\"canary\":\"b5\"}\n")
	fs["inbox/sub/b.jsonl"].ModTime = updated
	var second = run(state, []string{"/inbox/c.jsonl", "/inbox/sub/b.jsonl"}, `[
		{"id":"6","canary":"c6"}, {"id":"7","canary":"c7"},
		{"id":"4","canary":"b4-v2"}, {"id":"5","canary":"b5"}
	]`)
	state = second.checkpoints[len(second.checkpoints)-1]
	require.NoError(t, json.Unmarshal(state, &checkpoint))
	require.Equal(t, updated, checkpoint.States["inbox"].MinBound)
	require.Nil(t, checkpoint.States["inbox"].MaxBound)
	run(state, nil, `[]`)
}

func TestPullUnreadableDirectoryRecovery(t *testing.T) {
	var bin = t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(bin, "flow-parser"), []byte("#!/bin/sh\nexec cat\n"), 0700))
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))

	for _, tt := range []struct {
		name      string
		ascending bool
	}{
		{"modification time", false},
		{"ascending keys", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var modified = time.Now().Add(-2 * time.Hour).Truncate(time.Second).UTC()
			var fs = fstest.MapFS{
				"inbox/a.jsonl":         &fstest.MapFile{Data: []byte("{\"id\":\"a\"}\n"), ModTime: modified.Add(10 * time.Minute)},
				"inbox/private/b.jsonl": &fstest.MapFile{Data: []byte("{\"id\":\"b\"}\n"), ModTime: modified},
				"inbox/z.jsonl":         &fstest.MapFile{Data: []byte("{\"id\":\"z\"}\n"), ModTime: modified.Add(5 * time.Minute)},
			}
			var readErrors = map[string]error{
				"/inbox/private": &os.PathError{Op: "readdir", Path: "/inbox/private", Err: os.ErrPermission},
			}

			run := func(state json.RawMessage, expectedPaths []string, expectedDocs string) (json.RawMessage, error) {
				t.Helper()
				var driver = newDriver()
				cfg, err := driver.NewConfig(json.RawMessage(testConfig))
				require.NoError(t, err)
				var endpoint = cfg.(config)
				endpoint.MatchFiles = `.*\.jsonl$`
				endpoint.Advanced.AscendingKeys = tt.ascending
				raw, err := json.Marshal(endpoint)
				require.NoError(t, err)
				var store = &captureStore{tree: tree{fs: fs, readErrors: readErrors}}
				driver.Connect = func(context.Context, filesource.Config) (filesource.Store, error) {
					return store, nil
				}
				var stream = &captureStream{}
				err = driver.Pull(&pc.Request_Open{
					Capture: &pf.CaptureSpec{
						ConfigJson: raw,
						Bindings: []*pf.CaptureSpec_Binding{{
							ResourceConfigJson: json.RawMessage(`{"stream":"/inbox"}`),
							StateKey:           "inbox",
						}},
					},
					StateJson: state,
					Range:     &pf.RangeSpec{KeyEnd: ^uint32(0), RClockEnd: ^uint32(0)},
				}, &boilerplate.PullOutput{Connector_CaptureServer: stream, NumBindings: 1})
				require.Equal(t, expectedPaths, store.opened)
				require.JSONEq(t, expectedDocs, "["+strings.Join(stream.docs, ",")+"]")
				if len(stream.checkpoints) != 0 {
					state = stream.checkpoints[len(stream.checkpoints)-1]
				}
				return state, err
			}

			// An unreadable subdirectory fails the sweep instead of letting capture
			// progress pass the files inside it.
			state, err := run(nil, []string{"/inbox/a.jsonl"}, `[{"id":"a"}]`)
			require.ErrorIs(t, err, os.ErrPermission)
			state, err = run(state, nil, `[]`)
			require.ErrorIs(t, err, os.ErrPermission)

			// Once access is restored, the resumed sweep captures the older files.
			delete(readErrors, "/inbox/private")
			state, err = run(state, []string{"/inbox/private/b.jsonl", "/inbox/z.jsonl"}, `[{"id":"b"},{"id":"z"}]`)
			require.NoError(t, err)
			_, err = run(state, nil, `[]`)
			require.NoError(t, err)
		})
	}
}

type captureStream struct {
	testStream
	docs        []string
	checkpoints []json.RawMessage
}

func (s *captureStream) Send(response *pc.Response) error {
	if response.Captured != nil {
		s.docs = append(s.docs, string(response.Captured.DocJson))
	}
	if response.Checkpoint != nil {
		s.checkpoints = append(s.checkpoints, append(json.RawMessage(nil), response.Checkpoint.State.UpdatedJson...))
	}
	return nil
}

type captureStore struct {
	tree
	testStore
	opened      []string
	closedFiles int
}

func (s *captureStore) List(_ context.Context, query filesource.Query) (filesource.Listing, error) {
	return newSmbListing(&s.tree, query.Prefix, query.Recursive, query.StartAt)
}

func (s *captureStore) Read(_ context.Context, obj filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	file, err := s.fs.Open(strings.TrimPrefix(obj.Path, "/"))
	if err != nil {
		return nil, obj, err
	}
	s.opened = append(s.opened, obj.Path)
	return &captureFile{ReadCloser: file, store: s}, obj, nil
}

type captureFile struct {
	io.ReadCloser
	store *captureStore
}

func (f *captureFile) Close() error {
	f.store.closedFiles++
	return f.ReadCloser.Close()
}

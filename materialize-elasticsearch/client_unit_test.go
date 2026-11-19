package connector

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTasksListResponseMatching(t *testing.T) {
	// Shaped like a response of Elasticsearch 8.9.0 to
	// GET _tasks?actions=indices:data/write/delete/byquery&detailed=true.
	const body = `{
	"nodes": {
		"nodeA": {
			"name": "15b9bf489ebf",
			"tasks": {
				"nodeA:162": {
					"node": "nodeA",
					"id": 162,
					"action": "indices:data/write/delete/byquery",
					"description": "delete-by-query [idx]",
					"headers": {"X-Opaque-Id": "estuary-truncate:acmeCo/a:idx:1111"}
				},
				"nodeA:164": {
					"node": "nodeA",
					"id": 164,
					"action": "indices:data/write/delete/byquery",
					"headers": {}
				}
			}
		},
		"nodeB": {
			"tasks": {
				"nodeB:170": {
					"node": "nodeB",
					"id": 170,
					"action": "indices:data/write/delete/byquery",
					"headers": {"X-Opaque-Id": "estuary-truncate:acmeCo/a:other:2222"}
				},
				"nodeB:171": {
					"node": "nodeB",
					"id": 171,
					"action": "indices:data/write/delete/byquery",
					"headers": {"X-Opaque-Id": "estuary-truncate:acmeCo/a:idx2:3333"}
				}
			}
		}
	}
}`

	var parsed tasksListResponse
	require.NoError(t, json.Unmarshal([]byte(body), &parsed))

	for _, tt := range []struct {
		name     string
		prefixes []string
		want     map[string]string
	}{
		{
			name:     "matching task",
			prefixes: []string{"estuary-truncate:acmeCo/a:idx:"},
			want:     map[string]string{"nodeA:162": "estuary-truncate:acmeCo/a:idx:1111"},
		},
		{
			name:     "task on another node",
			prefixes: []string{"estuary-truncate:acmeCo/a:other:"},
			want:     map[string]string{"nodeB:170": "estuary-truncate:acmeCo/a:other:2222"},
		},
		{
			name:     "several prefixes",
			prefixes: []string{"estuary-truncate:acmeCo/a:idx:", "estuary-truncate:acmeCo/a:other:"},
			want: map[string]string{
				"nodeA:162": "estuary-truncate:acmeCo/a:idx:1111",
				"nodeB:170": "estuary-truncate:acmeCo/a:other:2222",
			},
		},
		{
			name:     "another materialization",
			prefixes: []string{"estuary-truncate:acmeCo/b:idx:"},
			want:     map[string]string{},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, parsed.matching(tt.prefixes))
		})
	}
}

func TestDeleteByQueryResultErr(t *testing.T) {
	for _, tt := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "completed",
			body: `{
				"deleted": 10,
				"timed_out": false,
				"failures": []
			}`,
			want: "",
		},
		{
			name: "document failed",
			body: `{
				"deleted": 3,
				"failures": [{
					"index": "idx",
					"id": "abc",
					"cause": {
						"type": "mapper_parsing_exception",
						"reason": "failed to parse"
					},
					"status": 400
				}]
			}`,
			want: "failed after deleting 3 documents: mapper_parsing_exception: failed to parse",
		},
		{
			name: "search failed",
			body: `{
				"deleted": 5,
				"failures": [{
					"index": "idx",
					"shard": 0,
					"node": "n",
					"reason": {
						"type": "search_context_missing_exception",
						"reason": "no search context found"
					}
				}]
			}`,
			want: "failed after deleting 5 documents: search_context_missing_exception: no search context found",
		},
		{
			// From Elasticsearch 8.9.0 and OpenSearch 2.13.0, a delete by
			// query that found a document already deleted by another.
			name: "version conflict",
			body: `{
				"deleted": 3,
				"version_conflicts": 1,
				"failures": [{
					"index": "vc",
					"id": "437Q66EBUXbhhvU1Fl0n",
					"cause": {
						"type": "version_conflict_engine_exception",
						"reason": "[437Q66EBUXbhhvU1Fl0n]: version conflict, required seqNo [3], primary term [1]. but no document was found",
						"index_uuid": "POlmVpulRre_cAyXKfOa9A",
						"shard": "0",
						"index": "vc"
					},
					"status": 409
				}]
			}`,
			want: "failed after deleting 3 documents: version_conflict_engine_exception: [437Q66EBUXbhhvU1Fl0n]: version conflict, required seqNo [3], primary term [1]. but no document was found",
		},
		{
			name: "failed without a cause",
			body: `{
				"deleted": 6,
				"failures": [{
					"index": "idx"
				}]
			}`,
			want: "failed after deleting 6 documents",
		},
		{
			name: "timed out",
			body: `{
				"deleted": 4,
				"timed_out": true,
				"failures": []
			}`,
			want: "timed out after deleting 4 documents",
		},
		{
			// From an Elasticsearch 8.9.0 delete by query cancelled while
			// running.
			name: "cancelled",
			body: `{
				"took": 2023,
				"timed_out": false,
				"total": 10,
				"deleted": 2,
				"batches": 2,
				"version_conflicts": 0,
				"noops": 0,
				"canceled": "by user request",
				"failures": []
			}`,
			want: "was cancelled (by user request) after deleting 2 documents",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var res deleteByQueryResult
			require.NoError(t, json.Unmarshal([]byte(tt.body), &res))
			if err := res.err(); tt.want == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.want)
			}
		})
	}
}

func TestCancelTaskResponseErr(t *testing.T) {
	for _, tt := range []struct {
		name string
		body string
		want string
	}{
		{
			name: "cancelled",
			body: `{
				"nodes": {
					"n": {
						"tasks": {
							"n:1": {
								"action": "indices:data/write/delete/byquery",
								"cancelled": true
							}
						}
					}
				}
			}`,
			want: "",
		},
		{
			// From Elasticsearch 8.9.0 and OpenSearch 2.13.0, cancelling a
			// task that already completed.
			name: "already stopped",
			body: `{
				"node_failures": [{
					"type": "failed_node_exception",
					"reason": "Failed node [n]",
					"node_id": "n",
					"caused_by": {
						"type": "resource_not_found_exception",
						"reason": "task [n:124] is not found"
					}
				}],
				"nodes": {}
			}`,
			want: "",
		},
		{
			name: "node failure",
			body: `{
				"node_failures": [{
					"type": "failed_node_exception",
					"reason": "Failed node [n]",
					"caused_by": {
						"type": "node_disconnected_exception",
						"reason": "disconnected"
					}
				}],
				"nodes": {}
			}`,
			want: "cancelling task n:1 failed: node_disconnected_exception: disconnected",
		},
		{
			name: "task failure",
			body: `{
				"task_failures": [{
					"task_id": 1,
					"node_id": "n",
					"status": "INTERNAL_SERVER_ERROR",
					"reason": {
						"type": "illegal_state_exception",
						"reason": "task is not cancellable"
					}
				}],
				"nodes": {}
			}`,
			want: "cancelling task n:1 failed: illegal_state_exception: task is not cancellable",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var res cancelTaskResponse
			require.NoError(t, json.Unmarshal([]byte(tt.body), &res))
			if err := res.err("n:1"); tt.want == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.want)
			}
		})
	}
}

package sql

import (
	"github.com/estuary/flow/go/protocols/flow"
)

const (
	// DefaultFlowCheckpoints is the default table for checkpoints.
	DefaultFlowCheckpoints = "flow_checkpoints_v1"
)

// FlowCheckpointsTable returns the TableShape that (optionally) holds fenced
// materialization checkpoints, where supported by the endpoint and connector.
func FlowCheckpointsTable(base TablePath) *TableShape {
	return &TableShape{
		Path:         append(base, DefaultFlowCheckpoints),
		Binding:      -1,
		Source:       "",
		Comment:      "This table holds Flow processing checkpoints used for exactly-once processing of materializations",
		DeltaUpdates: false,
		Keys: []Projection{
			{
				Projection: flow.Projection{
					Field: "materialization",
					Inference: flow.Inference{
						Types:   []string{"string"},
						Exists:  flow.Inference_MUST,
						String_: &flow.Inference_String{},
					},
				},
				Comment: "The name of the materialization.",
			},
			{
				Projection: flow.Projection{
					Field: "key_begin",
					Inference: flow.Inference{
						Types:  []string{"integer"},
						Exists: flow.Inference_MUST,
					},
				},
				Comment: "The inclusive lower-bound key hash covered by this checkpoint.",
			},
			{
				Projection: flow.Projection{
					Field: "key_end",
					Inference: flow.Inference{
						Types:  []string{"integer"},
						Exists: flow.Inference_MUST,
					},
				},
				Comment: "The inclusive upper-bound key hash covered by this checkpoint.",
			},
		},
		Values: []Projection{
			{
				Projection: flow.Projection{
					Field: "fence",
					Inference: flow.Inference{
						Types:  []string{"integer"},
						Exists: flow.Inference_MUST,
					},
				},
				Comment: "This nonce is used to uniquely identify unique process assignments of a shard and prevent them from conflicting.",
			},
			{
				Projection: flow.Projection{
					Field: "checkpoint",
					Inference: flow.Inference{
						Types:  []string{"string"},
						Exists: flow.Inference_MUST,
						String_: &flow.Inference_String{
							ContentType: "application/x-protobuf; proto=consumer.Checkpoint",
						},
					},
				},
				Comment: "Checkpoint of the Flow consumer shard, encoded as base64 protobuf.",
			},
		},
		Document: nil,
	}
}

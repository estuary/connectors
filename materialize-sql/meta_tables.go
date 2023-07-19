package sql

import (
	"github.com/estuary/flow/go/protocols/flow"
)

const (
	// DefaultFlowCheckpoints is the default table for checkpoints.
	DefaultFlowCheckpoints = "flow_checkpoints_v1"
	// DefaultFlowMaterializations is the default table for materialization specs.
	DefaultFlowMaterializations = "flow_materializations_v2"
)

// MetaTables returns *Table configurations for Flow materialization metadata tables,
// having the given `base` TablePath extended with their well-known table names.
func MetaTables(base TablePath) (specs, checkpoints TableShape) {
	return FlowMaterializationsTable(append(base, DefaultFlowMaterializations)...),
		FlowCheckpointsTable(append(base, DefaultFlowCheckpoints)...)
}

// FlowCheckpointsTable returns the TableShape that (optionally) holds fenced
// materialization checkpoints, where supported by the endpoint and connector.
func FlowCheckpointsTable(path ...string) TableShape {
	return TableShape{
		Path:         path,
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
				RawFieldConfig: nil,
				Comment:        "The name of the materialization.",
			},
			{
				Projection: flow.Projection{
					Field: "key_begin",
					Inference: flow.Inference{
						Types:  []string{"integer"},
						Exists: flow.Inference_MUST,
					},
				},
				RawFieldConfig: nil,
				Comment:        "The inclusive lower-bound key hash covered by this checkpoint.",
			},
			{
				Projection: flow.Projection{
					Field: "key_end",
					Inference: flow.Inference{
						Types:  []string{"integer"},
						Exists: flow.Inference_MUST,
					},
				},
				RawFieldConfig: nil,
				Comment:        "The inclusive upper-bound key hash covered by this checkpoint.",
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
				RawFieldConfig: nil,
				Comment:        "This nonce is used to uniquely identify unique process assignments of a shard and prevent them from conflicting.",
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
				RawFieldConfig: nil,
				Comment:        "Checkpoint of the Flow consumer shard, encoded as base64 protobuf.",
			},
		},
		Document: nil,
	}
}

// FlowMaterializationsTable returns the TableShape that holds MaterializationSpecs.
func FlowMaterializationsTable(path ...string) TableShape {

	return TableShape{
		Path:         path,
		Binding:      -1,
		Source:       "",
		Comment:      "This table is the source of truth for all materializations into this system.",
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
				RawFieldConfig: nil,
				Comment:        "The name of the materialization.",
			},
		},
		Values: []Projection{
			{
				Projection: flow.Projection{
					Field: "version",
					Inference: flow.Inference{
						Types:   []string{"string"},
						Exists:  flow.Inference_MUST,
						String_: &flow.Inference_String{},
					},
				},
				RawFieldConfig: nil,
				Comment:        "Version of the materialization.",
			},
			{
				Projection: flow.Projection{
					Field: "spec",
					Inference: flow.Inference{
						Types:  []string{"string"},
						Exists: flow.Inference_MUST,
						String_: &flow.Inference_String{
							ContentType: "application/x-protobuf; proto=flow.MaterializationSpec",
						},
					},
				},
				RawFieldConfig: nil,
				Comment:        "Specification of the materialization, encoded as base64 protobuf.",
			},
		},
		Document: nil,
	}
}

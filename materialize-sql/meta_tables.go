package sql

import (
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
)

const (
	// DefaultFlowCheckpoints is the default table for checkpoints.
	DefaultFlowCheckpoints = "flow_checkpoints_v1"
	// DefaultFlowMaterializations is the default table for materialization specs.
	DefaultFlowMaterializations = "flow_materializations_v2"
)

// MetaTables returns Table configurations for Flow materialization metadata tables,
// having the given `base` TablePath extended with their well-known table names.
func MetaTables(dialect Dialect, base TablePath) (specs, checkpoints Table) {
	return FlowMaterializationsTable(dialect, append(base, DefaultFlowMaterializations)...),
		FlowCheckpointsTable(dialect, append(base, DefaultFlowCheckpoints)...)
}

// FlowCheckpointsTable returns the Table that (optionally) holds fenced
// materialization checkpoints, where supported by the endpoint and connector.
func FlowCheckpointsTable(dialect Dialect, path ...string) Table {
	shape := TableShape{
		Path:         path,
		Binding:      -1,
		Source:       "",
		Comment:      "This table holds Flow processing checkpoints used for exactly-once processing of materializations",
		DeltaUpdates: false,
		Keys: []pf.Projection{
			{
				Field: "materialization",
				Inference: pf.Inference{
					Types:   []string{"string"},
					Exists:  pf.Inference_MUST,
					String_: &pf.Inference_String{},
				},
			},
			{
				Field: "key_begin",
				Inference: pf.Inference{
					Types:  []string{"integer"},
					Exists: pf.Inference_MUST,
				},
			},
			{
				Field: "key_end",
				Inference: pf.Inference{
					Types:  []string{"integer"},
					Exists: pf.Inference_MUST,
				},
			},
		},
		Values: []pf.Projection{
			{
				Field: "fence",
				Inference: pf.Inference{
					Types:  []string{"integer"},
					Exists: pf.Inference_MUST,
				},
			},
			{
				Field: "checkpoint",
				Inference: pf.Inference{
					Types:  []string{"string"},
					Exists: pf.Inference_MUST,
					String_: &pf.Inference_String{
						ContentType: "application/x-protobuf; proto=consumer.Checkpoint",
					},
				},
			},
		},
		Document: nil,
	}

	table, err := ResolveTable(shape, dialect)
	if err != nil {
		panic(fmt.Errorf("failed to resolve checkpoints table: %w", err))
	}

	table.Keys[0].Comment = "The name of the materialization."
	table.Keys[1].Comment = "The inclusive lower-bound key hash covered by this checkpoint."
	table.Keys[2].Comment = "The inclusive upper-bound key hash covered by this checkpoint."

	table.Values[0].Comment = "This nonce is used to uniquely identify unique process assignments of a shard and prevent them from conflicting."
	table.Values[1].Comment = "Checkpoint of the Flow consumer shard, encoded as base64 protobuf."

	return table
}

// FlowMaterializationsTable returns the TableShape that holds MaterializationSpecs.
func FlowMaterializationsTable(dialect Dialect, path ...string) Table {

	shape := TableShape{
		Path:         path,
		Binding:      -1,
		Source:       "",
		Comment:      "This table is the source of truth for all materializations into this system.",
		DeltaUpdates: false,
		Keys: []pf.Projection{
			{
				Field: "materialization",
				Inference: pf.Inference{
					Types:   []string{"string"},
					Exists:  pf.Inference_MUST,
					String_: &pf.Inference_String{},
				},
			},
		},
		Values: []pf.Projection{
			{
				Field: "version",
				Inference: pf.Inference{
					Types:   []string{"string"},
					Exists:  pf.Inference_MUST,
					String_: &pf.Inference_String{},
				},
			},
			{
				Field: "spec",
				Inference: pf.Inference{
					Types:  []string{"string"},
					Exists: pf.Inference_MUST,
					String_: &pf.Inference_String{
						ContentType: "application/x-protobuf; proto=pf.MaterializationSpec",
					},
				},
			},
		},
		Document: nil,
	}

	table, err := ResolveTable(shape, dialect)
	if err != nil {
		panic(fmt.Errorf("failed to resolve meta specs table: %w", err))
	}

	table.Keys[0].Comment = "The name of the materialization."
	table.Values[0].Comment = "Version of the materialization."
	table.Values[1].Comment = "Specification of the materialization, encoded as base64 protobuf."

	return table
}

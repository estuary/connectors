package main

import (
	"github.com/apache/iceberg-go"
)

type tableMetadata struct {
	FormatVersion   int              `json:"format-version"`
	Location        string           `json:"location"`
	CurrentSchemaID int              `json:"current-schema-id"`
	Schemas         []iceberg.Schema `json:"schemas"`
}

func (m *tableMetadata) currentSchema() *iceberg.Schema {
	for idx := range m.Schemas {
		sc := &m.Schemas[idx] // avoid copying the locks used in iceberg.Schema
		if sc.ID == m.CurrentSchemaID {
			return sc
		}
	}
	panic("not reached")
}

// tableRequirement is a condition that must hold when submitting a request to a
// catalog. Right now there's only assertCurrentSchemaID but it is structured to
// allow for adding more in the future, which may be particularly useful for
// performing atomic table appends of data files if we ever implement that.
type tableRequirement interface {
	isTableRequirement()
}

type baseRequirement struct {
	Type string `json:"type"`
}

type assertCurrentSchemaIdReq struct {
	baseRequirement
	CurrentSchemaID int `json:"current-schema-id"`
}

func (assertCurrentSchemaIdReq) isTableRequirement() {}

func assertCurrentSchemaID(id int) tableRequirement {
	return &assertCurrentSchemaIdReq{
		baseRequirement: baseRequirement{Type: "assert-current-schema-id"},
		CurrentSchemaID: id,
	}
}

// tableUpdate is an update to a table. As above, this is currently fairly
// limited to adding a new schema to a table and setting the current schema ID -
// both of which should typically be done to accomplish a DDL change.
type tableUpdate interface {
	isTableUpdate()
}

type baseUpdate struct {
	Action string `json:"action"`
}

type addSchemaUpdateReq struct {
	baseUpdate
	Schema *iceberg.Schema `json:"schema"`
}

func (addSchemaUpdateReq) isTableUpdate() {}

func addSchemaUpdate(schema *iceberg.Schema) tableUpdate {
	return &addSchemaUpdateReq{
		baseUpdate: baseUpdate{Action: "add-schema"},
		Schema:     schema,
	}
}

type setCurrentSchemaUpdateReq struct {
	baseUpdate
	SchemaID int `json:"schema-id"`
}

func (setCurrentSchemaUpdateReq) isTableUpdate() {}

func setCurrentSchemaUpdate(id int) tableUpdate {
	return &setCurrentSchemaUpdateReq{
		baseUpdate: baseUpdate{Action: "set-current-schema"},
		SchemaID:   id,
	}
}

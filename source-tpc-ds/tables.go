package main

import "fmt"

//go:generate go run gen_tables.go

// columnKind is how a dsdgen output field is typed in emitted documents.
type columnKind int

const (
	kindInteger columnKind = iota // JSON integer
	kindDecimal                   // JSON string, schema format "number", exact
	kindDate                      // JSON string YYYY-MM-DD, schema format "date"
	kindString                    // JSON string
)

type column struct {
	Name    string
	Kind    columnKind
	NotNull bool
}

type tableDef struct {
	Name    string
	Columns []column
	Key     []string // primary key column names
	Parent  string   // sales table whose dsdgen process emits this returns table, or ""
}

// tableByName is the definition for a TPC-DS table, or nil.
func tableByName(name string) *tableDef {
	for _, t := range tables {
		if t.Name == name {
			return t
		}
	}
	return nil
}

// streamOf is the table whose dsdgen process emits rows of t.
func streamOf(t *tableDef) *tableDef {
	if t.Parent == "" {
		return t
	}
	return tableByName(t.Parent)
}

// children are the returns tables emitted by parent's process.
func children(parent *tableDef) []*tableDef {
	var out []*tableDef
	for _, t := range tables {
		if t.Parent == parent.Name {
			out = append(out, t)
		}
	}
	return out
}

// routeLine picks, among the parent table and its children, the table whose
// column count matches the line. Field counts are distinct within a stream.
func routeLine(parent *tableDef, line []byte) (*tableDef, error) {
	var n = countFields(line)
	if n == len(parent.Columns) {
		return parent, nil
	}
	for _, c := range children(parent) {
		if n == len(c.Columns) {
			return c, nil
		}
	}
	return nil, fmt.Errorf("line with %d fields matches no table in the %s stream", n, parent.Name)
}

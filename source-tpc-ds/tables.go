package main

import "fmt"

//go:generate go run gen_tables.go

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

func tableByName(name string) *tableDef {
	for _, t := range tables {
		if t.Name == name {
			return t
		}
	}
	return nil
}

func streamOf(t *tableDef) *tableDef {
	if t.Parent == "" {
		return t
	}
	return tableByName(t.Parent)
}

func children(parent *tableDef) []*tableDef {
	var out []*tableDef
	for _, t := range tables {
		if t.Parent == parent.Name {
			out = append(out, t)
		}
	}
	return out
}

// Field counts differ between a parent and its children, so the count alone
// identifies the table.
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

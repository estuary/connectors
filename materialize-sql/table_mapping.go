package sql

import (
	"encoding/json"
	"fmt"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
)

// TablePath is a fully qualified table name (for example, with its schema).
type TablePath []string

// TableShape describes a database table, which can be used to generate various types of SQL statements.
type TableShape struct {
	// The TablePath of this Table.
	Path TablePath
	// The index of the binding which produced this table, or -1 if not from a binding.
	Binding int
	// The source Collection of the table, or empty if not sourced from a collection.
	Source pf.Collection
	// Comment for this table.
	Comment string
	// The table is operating in delta-updates mode (instead of a standard materialization).
	DeltaUpdates bool

	Keys, Values []Projection
	Document     *Projection
}

// Table is a database table which is fully resolved using the database Dialect.
type Table struct {
	TableShape

	// Quoted identifier for this table, suited for direct inclusion in SQL.
	Identifier string
	// Keys, Values, and the (optional) Document column.
	Keys, Values []Column
	Document     *Column
}

// Column is a database table column which is fully resolved using the database Dialect.
type Column struct {
	Projection
	MappedType

	// Quoted identifier for this column, suited for direct inclusion in SQL.
	Identifier string
	// Placeholder for this column's converted value in parameterized statements.
	Placeholder string
}

// ConvertKey converts a key Tuple to database parameters.
func (t *Table) ConvertKey(key tuple.Tuple) (out []interface{}, err error) {
	return convertTuple(key, t.Keys, out)
}

// ConvertAll concerts key and values Tuples, as well as a document RawMessage into database parameters.
func (t *Table) ConvertAll(key, values tuple.Tuple, doc json.RawMessage) (out []interface{}, err error) {
	out = make([]interface{}, 0, len(t.Keys)+len(t.Values)+1)
	if out, err = convertTuple(key, t.Keys, out); err != nil {
		return nil, err
	} else if out, err = convertTuple(values, t.Values, out); err != nil {
		return nil, err
	}

	if t.Document != nil {
		if m, err := t.Document.MappedType.Converter(doc); err != nil {
			return nil, fmt.Errorf("converting document %s: %w", t.Document.Field, err)
		} else {
			out = append(out, m)
		}
	}

	return out, nil
}

func convertTuple(in tuple.Tuple, columns []Column, out []interface{}) ([]interface{}, error) {
	if a, b := len(in), len(columns); a != b {
		panic(fmt.Sprintf("len(in) is %d but len(columns) is %d", a, b))
	}

	for i := range in {
		if m, err := columns[i].MappedType.Converter(in[i]); err != nil {
			return nil, fmt.Errorf("converting field %s: %w", columns[i].Field, err)
		} else {
			out = append(out, m)
		}
	}
	return out, nil
}

// Columns returns all columns of the Table as a single slice,
// ordered as Keys, then Values, then the Document.
func (t *Table) Columns() []*Column {
	var out []*Column
	for i := range t.Keys {
		out = append(out, &t.Keys[i])
	}
	for i := range t.Values {
		out = append(out, &t.Values[i])
	}
	if t.Document != nil {
		out = append(out, t.Document)
	}
	return out
}

// ResolveTable maps a TableShape into a Table using the given Dialect.
func ResolveTable(shape TableShape, dialect Dialect) (Table, error) {
	var table = Table{
		TableShape: shape,
		Identifier: dialect.Identifier(shape.Path...),
	}

	for _, key := range shape.Keys {
		table.Keys = append(table.Keys, Column{Projection: key})
	}
	for _, val := range shape.Values {
		table.Values = append(table.Values, Column{Projection: val})
	}
	if shape.Document != nil {
		table.Document = &Column{Projection: *shape.Document}
	}

	for index, col := range table.Columns() {
		var err error

		if col.MappedType, err = dialect.MapType(&col.Projection); err != nil {
			return Table{}, fmt.Errorf("mapping column %s of %s: %w", col.Field, shape.Path, err)
		}
		col.Identifier = dialect.Identifier(col.Field)
		col.Placeholder = dialect.Placeholder(index)
	}

	return table, nil
}

// BuildTableShape for the indexed specification binding, which has a corresponding database Resource.
func BuildTableShape(spec *pf.MaterializationSpec, index int, resource Resource) TableShape {
	var (
		binding = spec.Bindings[index]
		comment = fmt.Sprintf("Generated for materialization %s of collection %s",
			spec.Materialization, binding.Collection.Collection)
		keys, values, document = BuildProjections(binding)
	)

	return TableShape{
		Path:         resource.Path(),
		Binding:      index,
		Source:       binding.Collection.Collection,
		Comment:      comment,
		DeltaUpdates: resource.DeltaUpdates(),
		Keys:         keys,
		Values:       values,
		Document:     document,
	}
}

// Pop the last component from the TablePath and return its cloned result.
func (p TablePath) Pop() TablePath {
	if len(p) == 0 {
		return TablePath{}
	} else {
		return append(TablePath{}, p[:len(p)-1]...)
	}
}

// Push a component onto the TablePath and returned its cloned result.
func (p TablePath) Push(component string) TablePath {
	return append(append(TablePath{}, p...), component)
}

func (p TablePath) equals(other []string) bool {
	if len(p) != len(other) {
		return false
	}
	for i := 0; i != len(p); i++ {
		if p[i] != other[i] {
			return false
		}
	}
	return true
}

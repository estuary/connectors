package sql

import (
	"encoding/json"
	"fmt"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
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

	Keys, Values       []pf.Projection
	Document           *pf.Projection
	FieldConfigJsonMap map[string]json.RawMessage
}

// Table is a database table which is fully resolved using the database Dialect.
type Table struct {
	TableShape

	// Quoted identifier for this table, suited for direct inclusion in SQL.
	Identifier string

	// Can be used to "locate" the table in a query result from the INFORMATION_SCHEMAS view.
	InfoLocation InfoTableLocation

	// Keys, Values, and the (optional) Document column.
	Keys, Values []Column
	Document     *Column

	// The stateKey associated with this table's binding
	StateKey string
}

// Column is a database table column which is fully resolved using the database Dialect.
type Column struct {
	ColumnDef
	Converter boilerplate.ElementConverter

	// Quoted identifier for this column, suited for direct inclusion in SQL.
	Identifier string
	// Placeholder for this column's converted value in parameterized statements.
	Placeholder string
	// Flow collection field name.
	Field string
}

// ConvertKey converts a key Tuple to database parameters.
func (t *Table) ConvertKey(key tuple.Tuple) (out []interface{}, err error) {
	return t.convertTuple(key, t.Keys, out)
}

// ConvertAll concerts key and values Tuples, as well as a document RawMessage into database parameters.
func (t *Table) ConvertAll(key, values tuple.Tuple, doc json.RawMessage) (out []interface{}, err error) {
	out = make([]interface{}, 0, len(t.Keys)+len(t.Values)+1)
	if out, err = t.convertTuple(key, t.Keys, out); err != nil {
		return nil, err
	} else if out, err = t.convertTuple(values, t.Values, out); err != nil {
		return nil, err
	}

	if t.Document != nil {
		if m, err := t.Document.Converter(doc); err != nil {
			return nil, fmt.Errorf("converting document %s: %w", t.Document.Field, err)
		} else {
			out = append(out, m)
		}
	}

	return out, nil
}

func (t *Table) convertTuple(in tuple.Tuple, columns []Column, out []interface{}) ([]interface{}, error) {
	if a, b := len(in), len(columns); a != b {
		panic(fmt.Sprintf("len(in) is %d but len(columns) is %d", a, b))
	}

	for i := range in {
		if m, err := columns[i].Converter(in[i]); err != nil {
			return nil, fmt.Errorf("converting field %s of collection %s: %w", columns[i].Field, t.Source.String(), err)
		} else {
			out = append(out, m)
		}
	}
	return out, nil
}

// Columns returns all columns of the Table as a single slice,
// ordered as Keys, then Values, then the Document.
func (t *Table) Columns() []*Column {
	out := t.KeyPtrs()
	for i := range t.Values {
		out = append(out, &t.Values[i])
	}
	if t.Document != nil {
		out = append(out, t.Document)
	}
	return out
}

// KeyPtrs returns all keys of the Table as a single slice.
func (t *Table) KeyPtrs() []*Column {
	var out []*Column
	for i := range t.Keys {
		out = append(out, &t.Keys[i])
	}
	return out
}

// KeyNames returns all key names of the Table as a single slice.
func (t *Table) KeyNames() []string {
	keys := t.KeyPtrs()
	out := make([]string, 0, len(keys))
	for _, c := range keys {
		out = append(out, c.Field)
	}
	return out
}

// ColumnNames returns all column names of the Table as a single slice,
// ordered as Keys, then Values, then the Document.
func (t *Table) ColumnNames() []string {
	cols := t.Columns()
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		out = append(out, c.Field)
	}
	return out
}

// ResolveTable maps a TableShape into a Table using the given Dialect.
func ResolveTable(shape TableShape, dialect Dialect) (Table, error) {
	var table = Table{
		TableShape:   shape,
		Identifier:   dialect.Identifier(shape.Path...),
		InfoLocation: dialect.TableLocator(shape.Path),
	}

	idx := 0
	do := func(dst *[]Column, p pf.Projection) error {
		col, err := ResolveColumn(idx, p, dialect, shape.FieldConfigJsonMap)
		if err != nil {
			return err
		}
		*dst = append(*dst, col)
		idx++
		return nil
	}

	for _, key := range shape.Keys {
		key.IsPrimaryKey = true
		if err := do(&table.Keys, key); err != nil {
			return Table{}, err
		}
	}
	for _, val := range shape.Values {
		if err := do(&table.Values, val); err != nil {
			return Table{}, err
		}
	}
	if shape.Document != nil {
		col, err := ResolveColumn(idx, *shape.Document, dialect, shape.FieldConfigJsonMap)
		if err != nil {
			return Table{}, err
		}
		table.Document = &col
	}

	return table, nil
}

func ResolveColumn(index int, projection pf.Projection, dialect Dialect, fieldConfig map[string]json.RawMessage) (Column, error) {
	mappedType, err := boilerplate.NewTypeMapper(dialect.MapType).Map(&projection, fieldConfig)
	if err != nil {
		return Column{}, err
	}

	return Column{
		ColumnDef:   mappedType.EndpointType,
		Converter:   mappedType.Converter,
		Identifier:  dialect.Identifier(projection.Field),
		Placeholder: dialect.Placeholder(index),
		Field:       projection.Field,
	}, nil
}

// BuildTableShape for the indexed specification binding, which has a corresponding database Resource.
func BuildTableShape(spec *pf.MaterializationSpec, index int, resource Resource) TableShape {
	var (
		binding = spec.Bindings[index]
		comment = fmt.Sprintf("Generated for materialization %s of collection %s",
			spec.Name, binding.Collection.Name)
		keys, values, document = buildProjections(binding)
	)

	return TableShape{
		Path:               resource.Path(),
		Binding:            index,
		Source:             binding.Collection.Name,
		Comment:            comment,
		DeltaUpdates:       resource.DeltaUpdates(),
		Keys:               keys,
		Values:             values,
		Document:           document,
		FieldConfigJsonMap: binding.FieldSelection.FieldConfigJsonMap,
	}
}

func buildProjections(binding *pf.MaterializationSpec_Binding) (keys, values []pf.Projection, document *pf.Projection) {
	for _, field := range binding.FieldSelection.Keys {
		keys = append(keys, *binding.Collection.GetProjection(field))
	}
	for _, field := range binding.FieldSelection.Values {
		values = append(values, *binding.Collection.GetProjection(field))
	}
	if field := binding.FieldSelection.Document; field != "" {
		document = binding.Collection.GetProjection(field)
	}

	return
}

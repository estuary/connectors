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
	// Field configurations for this table, keyed by field name.
	// TODO(whb): This is somewhat of a temporary workaround, until the
	// Materializer constructs are more thoroughly integrated into
	// materialize-sql.
	FieldConfigJsonMap map[string]json.RawMessage

	Keys, Values []Projection
	Document     *Projection
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
	Projection
	MappedType

	// Quoted identifier for this column, suited for direct inclusion in SQL.
	Identifier string
	// Placeholder for this column's converted value in parameterized statements.
	Placeholder string
	// If this column can be null or not.
	MustExist bool
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
		if m, err := t.Document.MappedType.Converter(doc); err != nil {
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
		if m, err := columns[i].MappedType.Converter(in[i]); err != nil {
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

// RootLevelColumns returns only columns that represent root-level properties
func (t *Table) RootLevelColumns() []*Column {
	var rootLevelCols []*Column
	for _, col := range t.Columns() {
		if strings.Count(col.Ptr, "/") == 1 {
			rootLevelCols = append(rootLevelCols, col)
		}
	}
	return rootLevelCols
}

// MetaOpColumn returns the _meta/op column if it exists, nil otherwise
func (t *Table) MetaOpColumn() *Column {
	for _, col := range t.Columns() {
		if col.Field == "_meta/op" {
			return col
		}
	}
	return nil
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

	for _, key := range shape.Keys {
		key.IsPrimaryKey = true
		table.Keys = append(table.Keys, Column{Projection: key})
	}
	for _, val := range shape.Values {
		table.Values = append(table.Values, Column{Projection: val})
	}
	if shape.Document != nil {
		table.Document = &Column{Projection: *shape.Document}
	}

	for index, col := range table.Columns() {
		var fc FieldConfig
		if raw, ok := shape.FieldConfigJsonMap[col.Field]; ok {
			if err := json.Unmarshal(raw, &fc); err != nil {
				return Table{}, fmt.Errorf("unmarshalling field config for %s: %w", col.Field, err)
			}
		}

		resolved := resolveColumn(index, &col.Projection, fc, dialect)
		*col = resolved
	}

	return table, nil
}

func resolveColumn(index int, projection *Projection, fc FieldConfig, dialect Dialect) Column {
	mappedType := dialect.MapType(projection, fc)
	_, mustExist := projection.AsFlatType()

	return Column{
		Projection:  *projection,
		MappedType:  mappedType,
		Identifier:  dialect.Identifier(projection.Field),
		Placeholder: dialect.Placeholder(index),
		MustExist:   mustExist,
	}
}

func BuildTableShape(materializationName string, binding *pf.MaterializationSpec_Binding, bindingIdx int, path []string, deltaUpdates bool) TableShape {
	var (
		comment = fmt.Sprintf("Generated for materialization %s of collection %s",
			materializationName, binding.Collection.Name)
		keys, values, document = BuildProjections(binding)
	)

	return TableShape{
		Path:               path,
		Binding:            bindingIdx,
		Source:             binding.Collection.Name,
		Comment:            comment,
		DeltaUpdates:       deltaUpdates,
		FieldConfigJsonMap: binding.FieldSelection.FieldConfigJsonMap,
		Keys:               keys,
		Values:             values,
		Document:           document,
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

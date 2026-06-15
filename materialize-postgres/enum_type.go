package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	log "github.com/sirupsen/logrus"
)

// PgEnum represents a PostgreSQL ENUM type for a specific column.
// TypeName is empty until resolved via resolveEnumTypes at apply time.
type PgEnum struct {
	Field    string
	Values   []string
	TypeName string // schema-qualified quoted identifier, e.g. "public"."mytable_col_flow_enum"
}

func (e *PgEnum) DDL() string {
	return e.TypeName
}

// pgFieldMeta is attached to ExistingField.Meta by PopulateInfoSchema to carry
// the udt_name and resource path for USER-DEFINED columns, needed for
// compatibility checks.
type pgFieldMeta struct {
	UDTName string
	Path    []string
}

// Compatible returns true when an existing column is a USER-DEFINED type whose
// udt_name matches the enum type name this field would generate. It recomputes
// the expected base name with the same generator used to create the type, so
// the round-trip is exactly the same as when creating the type
func (e *PgEnum) Compatible(existing boilerplate.ExistingField) bool {
	if !strings.EqualFold(existing.Type, "USER-DEFINED") {
		return false
	}
	m, ok := existing.Meta.(pgFieldMeta)
	if !ok {
		return false
	}
	expected, err := enumBaseName(m.Path, e.Field)
	if err != nil {
		return false
	}
	return strings.EqualFold(m.UDTName, expected)
}

var _ sql.DDLer = (*PgEnum)(nil)
var _ sql.CompatibleColumnType = (*PgEnum)(nil)

// EnumMigrationTarget matches any migration whose desired type is a *PgEnum,
// allowing both text→enum and enum→enum (value expansion) migrations.
type EnumMigrationTarget struct{}

func (EnumMigrationTarget) CanMigrate(_ boilerplate.ExistingField, mapped sql.MappedType) bool {
	_, ok := mapped.TargetType.(*PgEnum)
	return ok
}

var _ sql.MigrationTarget = EnumMigrationTarget{}

// MapEnum returns a MapProjectionFn that maps string-only enum projections
// (those with EnumJsonVec populated and non-primary-key) to PgEnum. All other
// projections delegate to fallback.
func MapEnum(fallback sql.MapProjectionFn) sql.MapProjectionFn {
	return func(p *sql.Projection) (sql.DDLer, sql.CompatibleColumnTypes, sql.ElementConverter) {
		if p.IsPrimaryKey || len(p.Inference.EnumJsonVec) == 0 {
			return fallback(p)
		}
		// Only materialize as PG ENUM when the field type is purely string.
		for _, t := range p.Inference.Types {
			if t != "string" && t != "null" {
				return fallback(p)
			}
		}

		data, err := json.Marshal(p.Inference.EnumJsonVec)
		if err != nil {
			log.WithField("field", p.Field).WithError(err).Fatal("marshaling EnumJsonVec")
		}
		var values []string
		if err := json.Unmarshal(data, &values); err != nil {
			log.WithField("field", p.Field).WithError(err).Fatal("unmarshaling enum values")
		}
		slices.Sort(values)
		values = slices.Compact(values)
		if len(values) == 0 {
			return fallback(p)
		}
		e := &PgEnum{
			Field:  p.Field,
			Values: values,
		}
		return e, sql.CompatibleColumnTypes{e}, nil
	}
}

// applyEnumResolution sets the resolved type name on e and patches mt to
// reflect the resolved DDL strings. Callers must pass a pointer to the
// MappedType so that mutations are visible to the surrounding column.
func applyEnumResolution(e *PgEnum, typeName string, mt *sql.MappedType, mustExist bool) {
	e.TypeName = typeName
	mt.BareDDL = typeName
	mt.NullableDDL = typeName
	if mustExist {
		mt.DDL = typeName + " NOT NULL"
	} else {
		mt.DDL = typeName
	}
}

// enumTypeNameParts returns the raw schema, raw base name, and schema-qualified
// quoted identifier for the PostgreSQL ENUM type of a given column.
// e.g. ("public", "mytable_status_flow_enum", `"public"."mytable_status_flow_enum"`).
// The "_flow_enum" suffix distinguishes connector-managed types from pre-existing
// user-defined types. If the base name would exceed 63 bytes, the field component
// is truncated and an 8-char hex hash is inserted before the suffix.
func enumTypeNameParts(dialect sql.Dialect, path []string, field string) (schema, base, typeName string, err error) {
	switch len(path) {
	case 2:
		schema = path[0]
	case 1:
		schema = "public"
	default:
		return "", "", "", fmt.Errorf("unexpected resource path length %d for enum type naming: %v", len(path), path)
	}
	if base, err = enumBaseName(path, field); err != nil {
		return "", "", "", err
	}
	typeName = dialect.Identifier(schema, base)
	return
}

// enumBaseName computes the unqualified base name (the "_flow_enum"-suffixed
// identifier, before schema-qualification and quoting) for a column's enum
// type. Generation (enumTypeNameParts) and the compatibility check (Compatible)
// share this single source of truth so that recognizing the connector's own
// type names is exact regardless of truncation or hashing.
func enumBaseName(path []string, field string) (string, error) {
	if len(path) == 0 {
		return "", fmt.Errorf("unexpected empty resource path for enum type naming")
	}
	tableName := truncatedIdentifier(path[len(path)-1])
	base := tableName + "_" + field + "_flow_enum"

	if len([]byte(base)) > 63 {
		// Hash mode: <tableName>_<truncatedField>_<8hexHash>_flow_enum
		// Fixed overhead (2 separators + 8-char hash + "_flow_enum"): 20 bytes.
		// Cap tableName to 42 bytes so the field portion gets at least 1 byte.
		// Use the full pre-truncation names as hash input for better collision resistance.
		const maxTableBytes = 42
		h := sha256.Sum256([]byte(tableName + "_" + field))
		hash := fmt.Sprintf("%x", h[:4])
		tableName = truncateToBytes(tableName, maxTableBytes)
		fieldPart := truncateToBytes(field, 63-len([]byte(tableName))-20)
		base = tableName + "_" + fieldPart + "_" + hash + "_flow_enum"
	}

	return base, nil
}

// truncateToBytes truncates s to at most maxBytes bytes while retaining valid UTF-8.
func truncateToBytes(s string, maxBytes int) string {
	b := []byte(s)
	if len(b) <= maxBytes {
		return s
	}
	b = b[:maxBytes]
	for !utf8.Valid(b) {
		b = b[:len(b)-1]
	}
	return string(b)
}

// resolveEnumTypes walks the table's columns, sets TypeName on each *PgEnum
// target type, and patches the column's MappedType DDL strings to reflect the
// resolved name. Returns the de-duplicated list of PgEnum types found.
// The caller must pass a pointer to the Table so that column modifications
// (which are made via pointer) are visible when the table is later rendered.
func resolveEnumTypes(dialect sql.Dialect, table *sql.Table) ([]*PgEnum, error) {
	seen := make(map[string]*PgEnum)
	for _, col := range table.Columns() {
		e, ok := col.MappedType.TargetType.(*PgEnum)
		if !ok {
			continue
		}
		_, _, typeName, err := enumTypeNameParts(dialect, table.Path, col.Field)
		if err != nil {
			return nil, fmt.Errorf("resolving enum type for column %q: %w", col.Field, err)
		}
		applyEnumResolution(e, typeName, &col.MappedType, col.MustExist)

		if _, dup := seen[typeName]; !dup {
			seen[typeName] = e
		}
	}

	out := make([]*PgEnum, 0, len(seen))
	for _, e := range seen {
		out = append(out, e)
	}
	// Sort for deterministic output.
	slices.SortFunc(out, func(a, b *PgEnum) int {
		return strings.Compare(a.TypeName, b.TypeName)
	})
	return out, nil
}

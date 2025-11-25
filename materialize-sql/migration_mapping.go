package sql

import (
	"fmt"
	"slices"
	"strings"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
)

// MigrationSpecs maps existing column types to column types that can be
// migrated to.
type MigrationSpecs map[string][]MigrationSpec

// FindMigrationSpec returns the first MigrationSpec where existing can be
// migrated to mapped.
func (ms MigrationSpecs) FindMigrationSpec(existing boilerplate.ExistingField, mapped MappedType) *MigrationSpec {
	var specs []MigrationSpec
	if matched, ok := ms[strings.ToLower(existing.Type)]; ok {
		specs = append(specs, matched...)
	}
	if wildcard, ok := ms["*"]; ok {
		specs = append(specs, wildcard...)
	}
	for _, spec := range specs {
		if slices.ContainsFunc(spec.targets, func(mt MigrationTarget) bool {
			return mt.CanMigrate(existing, mapped)
		}) {
			return &spec
		}
	}

	return nil
}

type MigrationTarget interface {
	CanMigrate(existing boilerplate.ExistingField, mapped MappedType) bool
}

type StringTarget string

func (t StringTarget) CanMigrate(_ boilerplate.ExistingField, mapped MappedType) bool {
	return strings.EqualFold(string(t), mapped.NullableDDL)
}

type CastSQLFunc func(migration ColumnTypeMigration) string

type MigrationSpec struct {
	targets []MigrationTarget
	CastSQL CastSQLFunc
}

func NewMigrationSpec(targetDDLs []string, opts ...MigrationSpecOption) MigrationSpec {
	var targets []MigrationTarget
	for _, ddl := range targetDDLs {
		targets = append(targets, StringTarget(ddl))
	}
	out := MigrationSpec{
		targets: targets,
		CastSQL: func(m ColumnTypeMigration) string {
			return fmt.Sprintf("CAST(%s AS %s)", m.Identifier, m.NullableDDL)
		},
	}

	for _, o := range opts {
		o(&out)
	}

	return out
}

func NewMigrationSpecTarget(mt MigrationTarget, opts ...MigrationSpecOption) MigrationSpec {
	out := MigrationSpec{
		targets: []MigrationTarget{mt},
		CastSQL: func(m ColumnTypeMigration) string {
			return fmt.Sprintf("CAST(%s AS %s)", m.Identifier, m.NullableDDL)
		},
	}

	for _, o := range opts {
		o(&out)
	}

	return out
}

type MigrationSpecOption func(*MigrationSpec)

// WithCastSQL sets the castSQL template of the migration
func WithCastSQL(castSQL CastSQLFunc) MigrationSpecOption {
	return func(m *MigrationSpec) {
		m.CastSQL = castSQL
	}
}

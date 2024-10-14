package sql

import (
	"fmt"
	"slices"
	"strings"
)

type MigrationSpecs map[string][]MigrationSpec

func (ms MigrationSpecs) FindMigrationSpec(sourceType string, targetDDL string) *MigrationSpec {
	var target = strings.ToLower(targetDDL)

	var specs []MigrationSpec
	if matched, ok := ms[strings.ToLower(sourceType)]; ok {
		specs = append(specs, matched...)
	}
	if wildcard, ok := ms["*"]; ok {
		specs = append(specs, wildcard...)
	}
	for _, m := range specs {
		if slices.ContainsFunc(m.targetDDLs, func(ddl string) bool {
			return strings.ToLower(ddl) == target
		}) {
			return &m
		}
	}

	return nil
}

type CastSQLFunc func(migration ColumnTypeMigration) string

type MigrationSpec struct {
	targetDDLs []string
	CastSQL    CastSQLFunc
}

func NewMigrationSpec(targetDDLs []string, opts ...MigrationSpecOption) MigrationSpec {
	out := MigrationSpec{
		targetDDLs: targetDDLs,
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

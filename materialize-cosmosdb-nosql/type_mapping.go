package connector

import boilerplate "github.com/estuary/connectors/materialize-boilerplate"

type fieldConfig struct{}

func (fieldConfig) Validate() error {
	return nil
}

func (fieldConfig) CastToString() bool {
	return false
}

type mappedType struct{}

func (mappedType) String() string {
	return "document"
}

func (mappedType) Compatible(existing boilerplate.ExistingField) bool {
	return true
}

func (mappedType) CanMigrate(existing boilerplate.ExistingField) bool {
	return false
}

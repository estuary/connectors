package main

import boilerplate "github.com/estuary/connectors/materialize-boilerplate"

type FieldConfig struct{}

func (FieldConfig) Validate() error {
	return nil
}

func (FieldConfig) CastToString() bool {
	return false
}

type mappedType struct{}

func (m mappedType) String() string {
	return "document"
}

func (m mappedType) Compatible(existing boilerplate.ExistingField) bool {
	return true
}

func (m mappedType) CanMigrate(existing boilerplate.ExistingField) bool {
	return false
}

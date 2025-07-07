package main

import (
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
)

type fieldConfig struct{}

func (fieldConfig) Validate() error {
	return nil
}

func (fieldConfig) CastToString() bool {
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

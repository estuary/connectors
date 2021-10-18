package testdata

import (
	"github.com/brianvoe/gofakeit/v6"
	"github.com/estuary/protocols/fdb/tuple"
)

// TestData is an interface that supports all the needed functions required to be used for testing.
type TestData interface {
	KeyFields() []string
	RequiredFields() []string
	Keys() tuple.Tuple
	Values() tuple.Tuple
	UpdateValues()
	Equal(TestData) bool
}

// BasicData is the default test data struct.
type Basic struct {
	Key1    int     `json:"key1" flowsim:"key1,key"`
	Key2    bool    `json:"key2" flowsim:"key2,key"`
	Boolean bool    `json:"boolean" flowsim:"boolean"`
	Integer int     `json:"integer" flowsim:"integer"`
	Number  float32 `json:"number" flowsim:"number"`
	String  string  `json:"string" flowsim:"string,required"`
}

// KeyFields returns a slice of the keys in the struct.
func (d *Basic) KeyFields() []string {
	return []string{"key1", "key2"}
}

// RequiredFields returns all the required fields (including keys) of the struct.
func (d *Basic) RequiredFields() []string {
	return []string{"key1", "key2", "string"}
}

// Keys returns a tuple of the keys of the struct.
func (d *Basic) Keys() tuple.Tuple {
	return tuple.Tuple{d.Key1, d.Key2}
}

// Values returns a tuple of all the non key values.
func (d *Basic) Values() tuple.Tuple {
	return tuple.Tuple{d.Boolean, d.Integer, d.Number, d.String}
}

// Equal returns true if the passed value is equal.
func (d *Basic) Equal(t TestData) bool {
	if td, ok := t.(*Basic); ok {
		return *d == *td
	}
	return false
}

// UpdateValues randomizes the values (keeping the same keys)
func (d *Basic) UpdateValues() {
	var key1 = d.Key1
	var key2 = d.Key2
	gofakeit.Struct(&d)
	d.Key1 = key1
	d.Key2 = key2
}

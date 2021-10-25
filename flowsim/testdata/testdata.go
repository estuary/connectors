package testdata

import (
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/estuary/protocols/fdb/tuple"
)

// TestData is an interface that supports all the needed functions required to be used for testing.
// When returning Tuple values, it must be standard types. For example, if you want to use time.Time
// the Keys()/Values() functions expect a string version in time.RFC3339Nano format.
type TestData interface {
	// KeyFields must return the names of the key fields in the struct.
	KeyFields() []string
	// RequiredFields must return all of the names of the required fields in the struct (including keys).
	RequiredFields() []string
	// Keys should return a tuple of the key fields sorted by alphabetical order.
	Keys() tuple.Tuple
	// Values should return a tuple of the value fields sorted by alphabetical order.
	Values() tuple.Tuple
	// UpdateValues will be called by the test framework to change the non-key values of the value.
	UpdateValues()
	// Equal should test for equality of the passed in TestData.
	Equal(TestData) bool
}

// BasicData is the default test data struct.
type Basic struct {
	Key1     int       `json:"key1" flowsim:"key1,key"`
	Key2     bool      `json:"key2" flowsim:"key2,key"`
	Boolean  bool      `json:"boolean" flowsim:"boolean"`
	Integer  int       `json:"integer" flowsim:"integer"`
	Number   float32   `json:"number" flowsim:"number"`
	String   string    `json:"string" flowsim:"string,required"`
	DateTime time.Time `json:"dateTime" flowsim:"dateTime"`
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
	return tuple.Tuple{d.Boolean, d.DateTime.Format(time.RFC3339Nano), d.Integer, d.Number, d.String}
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

package main

import (
	"context"
	"fmt"
	random "math/rand"

	"github.com/estuary/connectors/flowsim/matsim"
	"github.com/estuary/connectors/flowsim/testdata"
	connector "github.com/estuary/connectors/materialize-rockset"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

type FakeEvent struct {
	Id         string `json:"id" flowsim:"id,key"`
	ChangeType string `json:"_change_type"`
	Foo        string `json:"foo"`
}

func (e *FakeEvent) KeyFields() []string {
	return []string{"id"}
}

func (e *FakeEvent) RequiredFields() []string {
	return []string{"_change_type", "id", "foo"}
}

func (e *FakeEvent) Keys() tuple.Tuple {
	return tuple.Tuple{e.Id}
}

func (e *FakeEvent) Values() tuple.Tuple {
	return tuple.Tuple{e.ChangeType, e.Foo}
}

func (e *FakeEvent) UpdateValues() {
	if random.Intn(5) == 0 {
		e.ChangeType = "Update"
		e.Foo = connector.RandString(6)
	} else {
		e.ChangeType = "Delete"
	}
}

func (e *FakeEvent) Equal(other testdata.TestData) bool {
	if o, ok := other.(*FakeEvent); ok {
		return e.Id == o.Id && e.Foo == o.Foo
	}
	return false
}

var _ testdata.TestData = &FakeEvent{}

func fakeCdcEvent() func() testdata.TestData {
	idSeq := 0
	return func() testdata.TestData {
		idSeq += 1
		return &FakeEvent{Id: fmt.Sprintf("%v", idSeq), ChangeType: "Insert", Foo: connector.RandString(6)}
	}
}

// Matsim Main
func main() {
	matsim.Run(context.Background(), connector.NewRocksetDriver(), fakeCdcEvent())
}

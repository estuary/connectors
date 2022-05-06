package main

import "errors"

type BigQueryCheckPoint struct {
	Bindings []DriverCheckPointBinding
}

type DriverCheckPointBinding struct {
	Query        string
	FilePath     string
	BindingIndex int
}

var ErrDriverCheckPointBindingInvalid = errors.New("no driver checkpoint binding found at this index")

func NewBigQueryCheckPoint() *BigQueryCheckPoint {
	return &BigQueryCheckPoint{
		Bindings: make([]DriverCheckPointBinding, 0),
	}
}

func (cp *BigQueryCheckPoint) Binding(bindingIndex int) (*DriverCheckPointBinding, error) {
	var chpb *DriverCheckPointBinding
	for _, bd := range cp.Bindings {
		if bd.BindingIndex == bindingIndex {
			chpb = &bd
			break
		}
	}

	if chpb == nil {
		return nil, ErrDriverCheckPointBindingInvalid
	}

	return chpb, nil
}

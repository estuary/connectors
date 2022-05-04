package main

import "errors"

type BigQueryCheckPoint struct {
	Bindings []DriverCheckPointBinding
}

type DriverCheckPointBinding struct {
	Query    string
	FilePath string
}

var ErrDriverCheckPointBindingInvalid = errors.New("no driver checkpoint binding found at this index")

func NewBigQueryCheckPoint() *BigQueryCheckPoint {
	return &BigQueryCheckPoint{
		Bindings: make([]DriverCheckPointBinding, 0),
	}
}

func (cp *BigQueryCheckPoint) Binding(i int) (DriverCheckPointBinding, error) {
	if len(cp.Bindings) < i {
		return cp.Bindings[i], nil
	}

	return DriverCheckPointBinding{}, ErrDriverCheckPointBindingInvalid
}

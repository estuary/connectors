package main

import "encoding/json"

type BigQueryCheckPoint struct {
	Bindings []DriverCheckPointBinding
}

type DriverCheckPointBinding struct {
	FilePath        string
	BindingSpecJson json.RawMessage
	Version         string
}

func NewBigQueryCheckPoint() *BigQueryCheckPoint {
	return &BigQueryCheckPoint{
		Bindings: make([]DriverCheckPointBinding, 0),
	}
}

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
)

func main() {
	boilerplate.RunMain(new(driver))
}

const (
	// dsdgen rejects scale factors above this.
	maxScale = 100000
	// Below this dsdgen walks through many empty days to find each sale's
	// date, and a sales table takes minutes.
	minScale = 0.01
)

type config struct {
	Scale float64 `json:"scale" jsonschema:"title=Scale Factor,default=1" jsonschema_description:"TPC-DS scale factor. Values of 1 and above should be one of the benchmark's official factors (1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000, 100000) and give roughly that many GB of raw data. Values from 0.01 up to 1 produce a proportionally smaller dataset for smoke tests and demos; fixed-size tables such as date_dim and time_dim keep their full size. Values below 0.01 are rejected." jsonschema_extras:"nonsensitive=true"`
}

func (c config) Validate() error {
	if math.IsNaN(c.Scale) || math.IsInf(c.Scale, 0) || c.Scale <= 0 {
		return fmt.Errorf("scale factor must be a positive number (got %v)", c.Scale)
	}
	if c.Scale < minScale {
		return fmt.Errorf("scale factor must be at least %v (got %v)", minScale, c.Scale)
	}
	if c.Scale > maxScale {
		return fmt.Errorf("scale factor must be at most %d (got %v)", maxScale, c.Scale)
	}
	return nil
}

func (c config) scaleString() string {
	return strconv.FormatFloat(c.Scale, 'f', -1, 64)
}

type resource struct {
	Table string `json:"table" jsonschema:"title=Table,description=Name of the TPC-DS table to emit."`
}

func (r resource) Validate() error {
	if tableByName(r.Table) == nil {
		return fmt.Errorf("unknown TPC-DS table %q", r.Table)
	}
	return nil
}

type driver struct{}

var _ boilerplate.Connector = &driver{}

func (driver) Spec(ctx context.Context, req *pc.Request_Spec) (*pc.Response_Spec, error) {
	endpointSchema, err := schemagen.GenerateSchema("TPC-DS", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}
	resourceSchema, err := schemagen.GenerateSchema("TPC-DS Table", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}
	return &pc.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://github.com/estuary/connectors/blob/main/source-tpc-ds/README.md",
		ResourcePathPointers:     []string{"/table"},
	}, nil
}

func (driver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}
	var out []*pc.Response_Validated_Binding
	for _, b := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		out = append(out, &pc.Response_Validated_Binding{ResourcePath: []string{res.Table}})
	}
	return &pc.Response_Validated{Bindings: out}, nil
}

func (driver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}
	var out []*pc.Response_Discovered_Binding
	for _, t := range tables {
		resourceJSON, err := json.Marshal(resource{Table: t.Name})
		if err != nil {
			return nil, err
		}
		schemaJSON, err := json.Marshal(documentSchema(t))
		if err != nil {
			return nil, err
		}
		var key []string
		for _, k := range t.Key {
			key = append(key, "/"+k)
		}
		out = append(out, &pc.Response_Discovered_Binding{
			RecommendedName:    t.Name,
			ResourceConfigJson: resourceJSON,
			DocumentSchemaJson: schemaJSON,
			Key:                key,
		})
	}
	return &pc.Response_Discovered{Bindings: out}, nil
}

func documentSchema(t *tableDef) map[string]any {
	var props = make(map[string]any, len(t.Columns))
	var required []string
	for _, c := range t.Columns {
		var p map[string]any
		switch c.Kind {
		case kindInteger:
			p = map[string]any{"type": "integer"}
		case kindDecimal:
			p = map[string]any{"type": "string", "format": "number"}
		case kindDate:
			p = map[string]any{"type": "string", "format": "date"}
		default:
			p = map[string]any{"type": "string"}
		}
		props[c.Name] = p
		if c.NotNull {
			required = append(required, c.Name)
		}
	}
	return map[string]any{
		"type":       "object",
		"title":      t.Name,
		"properties": props,
		"required":   required,
	}
}

func (driver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{ActionDescription: "generated dataset; nothing to apply"}, nil
}

func (driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var cfg config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}
	var prior state
	if open.StateJson != nil {
		if err := json.Unmarshal(open.StateJson, &prior); err != nil {
			return fmt.Errorf("parsing driver checkpoint: %w", err)
		}
	}
	var bindings []*binding
	for idx, b := range open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		var key = boilerplate.StateKey(b.StateKey)
		var bs = prior.Bindings[key]
		if bs == nil {
			bs = &bindingState{}
		}
		bindings = append(bindings, &binding{
			index:    idx,
			table:    tableByName(res.Table),
			stateKey: key,
			state:    bs,
		})
	}
	gen, err := newGenerator(cfg.scaleString())
	if err != nil {
		return err
	}
	return (&capture{out: stream, gen: gen, bindings: bindings}).run()
}

package main

import (
	"fmt"
	"strings"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
)

const (
	maxInterval = 15 * time.Minute
	minInterval = 1 * time.Minute
)

type resource struct {
	Name      string                 `json:"name" jsonschema:"title=Name,description=Unique name for this binding. Cannot be changed once set."`
	StartDate time.Time              `json:"start_date" jsonschema:"title=Start Date,description=Get trades starting at this date. Has no effect if changed after a binding is added."`
	Feed      string                 `json:"feed" jsonschema:"title=Feed,description=The feed to pull market data from.,enum=iex,enum=sip"`
	Symbols   string                 `json:"symbols" jsonschema:"title=Symbols,description=Comma separated list of symbols to monitor."`
	Advanced  advancedResourceConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type advancedResourceConfig struct {
	StopDate        time.Time `json:"stop_date,omitempty" jsonschema:"title=Stop Date,description=Stop backfilling historical data at this date."`
	DisableRealTime bool      `json:"disable_real_time,omitempty" jsonschema:"title=Disable Real-Time Streaming,description=Disables real-time streaming of ticks via the websocket API. Data will only be collected via the backfill mechanism."`
}

func (r *resource) Validate() error {
	var requiredProperties = [][]string{
		{"feed", r.Feed},
		{"symbols", r.Symbols},
		{"name", r.Name},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if r.StartDate.IsZero() {
		return fmt.Errorf("must provide a value for start_date")
	}

	if !r.Advanced.StopDate.IsZero() && r.Advanced.StopDate.Before(r.StartDate) {
		return fmt.Errorf("stop_date %s cannot be before start_date %s", r.Advanced.StopDate, r.StartDate)
	}

	if r.Feed != "iex" && r.Feed != "sip" {
		return fmt.Errorf("feed must be 'iex' or 'sip'")
	}

	return nil
}

func (r *resource) GetSymbols() []string {
	// TODO: Needs some pre-validation that this isn't mangled.
	return strings.Split(r.Symbols, ",")
}

type config struct {
	ApiKey    string         `json:"api_key" jsonschema:"title=Alpaca API Key,description=Your Alpaca API key." jsonschema_extras:"secret=true"`
	ApiSecret string         `json:"api_secret" jsonschema:"title=Alpaca API Key Secret,description=Your Alpaca API key secret." jsonschema_extras:"secret=true"`
	Advanced  advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type advancedConfig struct {
	IsFreePlan bool `json:"is_free_plan,omitempty" jsonschema:"title=Free Plan,description=Set this if you are using a free plan. Delays data by 15 minutes."`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"api_key", c.ApiKey},
		{"api_secret", c.ApiSecret},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	return nil
}

func main() {
	boilerplate.RunMain(new(driver))
}

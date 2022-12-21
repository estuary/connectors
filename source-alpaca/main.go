package main

import (
	"fmt"
	"strings"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
)

type resource struct {
	Name    string `json:"name" jsonschema:"title=Name,description=Unique name for this binding. Cannot be changed once set."`
	Feed    string `json:"feed" jsonschema:"title=Feed,description=The feed to pull market data from.,enum=iex,enum=sip"`
	Symbols string `json:"symbols" jsonschema:"title=Symbols,description=Comma separated list of symbols to monitor."`

	startDate time.Time
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

	if r.Feed != "iex" && r.Feed != "sip" {
		return fmt.Errorf("feed must be iex or sip, got %s", r.Feed)
	}

	return nil
}

func (r *resource) GetSymbols() []string {
	// There does not seem to be any strict standard for what constitutes a "valid" stock symbol
	// format in terms of allowable characters, etc. We will just need to trust that the user has
	// provided a valid list of comma-separated symbols. Practically speaking, there is no such
	// thing as an "invalid" symbol, and if the symbol does not exist it will simply not return any
	// data.
	return strings.Split(r.Symbols, ",")
}

type config struct {
	ApiKey    string         `json:"api_key" jsonschema:"title=Alpaca API Key,description=Your Alpaca API key." jsonschema_extras:"secret=true"`
	ApiSecret string         `json:"api_secret" jsonschema:"title=Alpaca API Key Secret,description=Your Alpaca API key secret." jsonschema_extras:"secret=true"`
	Feed      string         `json:"feed" jsonschema:"title=Feed,description=The feed to pull market data from. May be overridden within the binding resource configuration.,enum=iex,enum=sip"`
	Symbols   string         `json:"symbols" jsonschema:"title=Symbols,description=Comma separated list of symbols to monitor. May be overridden within the binding resource configuration"`
	StartDate time.Time      `json:"start_date" jsonschema:"title=Start Date,description=Get trades starting at this date. Has no effect if changed after a binding is added."`
	Advanced  advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type advancedConfig struct {
	IsFreePlan      bool      `json:"is_free_plan,omitempty" jsonschema:"title=Free Plan,description=Set this if you are using a free plan. Delays data by 15 minutes."`
	StopDate        time.Time `json:"stop_date,omitempty" jsonschema:"title=Stop Date,description=Stop backfilling historical data at this date."`
	DisableRealTime bool      `json:"disable_real_time,omitempty" jsonschema:"title=Disable Real-Time Streaming,description=Disables real-time streaming of ticks via the websocket API. Data will only be collected via the backfill mechanism."`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"api_key", c.ApiKey},
		{"api_secret", c.ApiSecret},
		{"feed", c.Feed},
		{"symbols", c.Symbols},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.StartDate.IsZero() {
		return fmt.Errorf("must provide a value for start_date")
	}

	if !c.Advanced.StopDate.IsZero() && c.Advanced.StopDate.Before(c.StartDate) {
		return fmt.Errorf("stop_date %s cannot be before start_date %s", c.Advanced.StopDate, c.StartDate)
	}

	if c.Feed != "iex" && c.Feed != "sip" {
		return fmt.Errorf("feed must be iex or sip, got %s", c.Feed)
	}

	return nil
}

func main() {
	boilerplate.RunMain(new(driver))
}

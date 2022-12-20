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
	Name      string    `json:"name" jsonschema:"title=Name,description=Unique name for this binding. Cannot be changed once set."`
	StartDate time.Time `json:"start_date" jsonschema:"title=Start Date,description=Get ticks starting at this date. Has no effect if changed after a binding is added."`
	Feed      string    `json:"feed" jsonschema:"title=Feed,description=Feed to pull from. Probably IEX for a free plan."`
	// TODO: Does currency work with streaming?
	Currency string `json:"currency" jsonschema:"title=Currency,description=Currency to report data in. Probably USD."`
	Symbols  string `json:"symbols" jsonschema:"title=Symbols,description=Comma separated list of symbols to get trade data for."`
}

func (r *resource) Validate() error {
	var requiredProperties = [][]string{
		{"feed", r.Feed},
		{"currency", r.Currency},
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

	return nil
}

func (r *resource) GetSymbols() []string {
	// TODO: Needs some pre-validation that this isn't mangled.
	return strings.Split(r.Symbols, ",")
}

type config struct {
	ApiKey     string `json:"api_key" jsonschema:"title=Alpaca API Key,description=Your Alpaca API key."`
	ApiSecret  string `json:"api_secret" jsonschema:"title=Alpaca API Key Secret,description=Your Alpaca API key secret."`
	IsFreePlan bool   `json:"is_free_plan,omitempty" jsonschema:"title=Free Plan,description=If you are using a free plan. Delays data by 15 minutes."`
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

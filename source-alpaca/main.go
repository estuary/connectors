package main

import (
	"fmt"
	"strings"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
)

const (
	defaultMaxBackfillInterval = 15 * time.Minute
	defaultMinBackfillInterval = 1 * time.Minute
	// Unix Nano time of 2016-01-01T00:00:00Z, which is as far back as any Alpaca plan is able to
	// get historical data for.
	minStartTimeNanos = 1451606400000000000
)

type resource struct {
	Name string `json:"name" jsonschema:"title=Name,description=Unique name for this binding. Cannot be changed once set."`
}

func (r *resource) Validate() error {
	var requiredProperties = [][]string{
		{"name", r.Name},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	return nil
}

type config struct {
	ApiKeyID     string         `json:"api_key_id" jsonschema:"title=Alpaca API Key ID,description=Your Alpaca API key ID." jsonschema_extras:"secret=true"`
	ApiSecretKey string         `json:"api_secret_key" jsonschema:"title=Alpaca API Secret Key,description=Your Alpaca API Secret key." jsonschema_extras:"secret=true"`
	Feed         string         `json:"feed" jsonschema:"title=Feed,description=The feed to pull market data from.,enum=iex,enum=sip"`
	Symbols      string         `json:"symbols" jsonschema:"title=Symbols,description=Comma separated list of symbols to monitor." jsonschema_extras:"multiline=true"`
	StartDate    time.Time      `json:"start_date" jsonschema:"title=Start Date,description=Get trades starting at this date. Has no effect if changed after the capture has started. Must be no earlier than 2016-01-01T00:00:00Z."`
	Advanced     advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`

	effectiveMaxBackfillInterval time.Duration
	effectiveMinBackfillInterval time.Duration
}

func (c *config) GetSymbols() []string {
	// There does not seem to be any strict standard for what constitutes a "valid" stock symbol
	// format in terms of allowable characters, etc. We will just need to trust that the user has
	// provided a valid list of comma-separated symbols. Practically speaking, there is no such
	// thing as an "invalid" symbol, and if the symbol does not exist it will simply not return any
	// data.
	return strings.Split(c.Symbols, ",")
}

type advancedConfig struct {
	IsFreePlan          bool      `json:"is_free_plan,omitempty" jsonschema:"title=Free Plan,description=Set this if you are using a free plan. Delays data by 15 minutes."`
	StopDate            time.Time `json:"stop_date,omitempty" jsonschema:"title=Stop Date,description=Stop backfilling historical data at this date."`
	DisableRealTime     bool      `json:"disable_real_time,omitempty" jsonschema:"title=Disable Real-Time Streaming,description=Disables real-time streaming via the websocket API. Data will only be collected via the backfill mechanism."`
	DisableBackfill     bool      `json:"disable_backfill,omitempty" jsonschema:"title=Disable Historical Data Backfill,description=Disables historical data backfill via the historical data API. Data will only be collected via streaming."`
	MaxBackfillInterval string    `json:"max_backfill_interval,omitempty" jsonschema:"title=Maximum Backfill Interval,description=The largest time interval that will be requested for backfills. Using smaller intervals may be useful when tracking many symbols. Must be a valid Go duration string."`
	MinBackfillInterval string    `json:"min_backfill_interval,omitempty" jsonschema:"title=Minimum Backfill Interval,description=The smallest time interval that will be requested for backfills after the initial backfill is complete. Must be a valid Go duration string."`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"api_key_id", c.ApiKeyID},
		{"api_secret_key", c.ApiSecretKey},
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

	if c.StartDate.Before(time.Unix(0, minStartTimeNanos)) {
		return fmt.Errorf("start_date must not be before 2016-01-01T00:00:00Z")
	}

	if !c.Advanced.StopDate.IsZero() && c.Advanced.StopDate.Before(c.StartDate) {
		return fmt.Errorf("stop_date %s cannot be before start_date %s", c.Advanced.StopDate, c.StartDate)
	}

	if c.Feed != "iex" && c.Feed != "sip" {
		return fmt.Errorf("feed must be iex or sip, got %s", c.Feed)
	}

	if c.Advanced.DisableRealTime && c.Advanced.DisableBackfill {
		return fmt.Errorf("cannot disable both real time and backfill data collection")
	}

	if c.Advanced.MaxBackfillInterval != "" {
		parsed, err := time.ParseDuration(c.Advanced.MaxBackfillInterval)
		if err != nil {
			return fmt.Errorf("invalid max_backfill_interval %q: %w", c.Advanced.MaxBackfillInterval, err)
		}

		c.effectiveMaxBackfillInterval = parsed
	} else {
		c.effectiveMaxBackfillInterval = defaultMaxBackfillInterval
	}

	if c.Advanced.MinBackfillInterval != "" {
		parsed, err := time.ParseDuration(c.Advanced.MinBackfillInterval)
		if err != nil {
			return fmt.Errorf("invalid min_backfill_interval %q: %w", c.Advanced.MinBackfillInterval, err)
		}

		c.effectiveMinBackfillInterval = parsed
	} else {
		c.effectiveMinBackfillInterval = defaultMinBackfillInterval
	}

	if c.effectiveMinBackfillInterval > c.effectiveMaxBackfillInterval {
		return fmt.Errorf("min_backfill_interval of %s cannot be greater than max_backfill_interval of %s", c.effectiveMinBackfillInterval, c.effectiveMaxBackfillInterval)
	}

	return nil
}

func main() {
	boilerplate.RunMain(new(driver))
}

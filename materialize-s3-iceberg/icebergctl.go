package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
)

// When running tests locally, PYTHON_PATH should should be the path to the virtualenv python, or
// just "python" (the default if unset) if you are running the test from an "activated" virtual
// environment. The docker image for the connector must set this path accordingly.
var pythonPath = func() string {
	p, ok := os.LookupEnv("PYTHON_PATH")
	if !ok {
		return "python"
	}

	return p
}()

// These structs are for serializing the inputs for iceberg-ctl, and have correspond pydantic models
// for deserializing them.
type existingIcebergColumn struct {
	Name     string      `json:"name"`
	Nullable bool        `json:"nullable"`
	Type     icebergType `json:"type"`
}

type tableCreate struct {
	Fields   []existingIcebergColumn `json:"fields"`
	Location string                  `json:"location"`
}

type tableAlter struct {
	NewColumns           []existingIcebergColumn `json:"new_columns"`
	NewlyNullableColumns []string                `json:"newly_nullable_columns"`
}

// Run a command with iceberg-ctl. The config is required for every command other than
// `print-config-schema`, currently.
func runIcebergctl(cfg *config, args ...string) ([]byte, error) {
	cmd := exec.Command(pythonPath, append([]string{"iceberg-ctl/iceberg_ctl"}, args...)...)

	if cfg != nil {
		configJson, err := json.Marshal(cfg)
		if err != nil {
			return nil, fmt.Errorf("marshalling configJson: %w", err)
		}

		cmd.Env = append(cmd.Env, []string{
			fmt.Sprintf("ICEBERG_ENDPOINT_CONFIG_JSON=%s", string(configJson)),
		}...,
		)
	}

	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("error running iceberg_ctl: %s", string(exitErr.Stderr))
		}

		return nil, err
	}

	return out, nil
}

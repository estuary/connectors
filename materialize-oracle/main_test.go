package connector

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"
)

func integrationTestsEnabled() bool {
	return !testing.Short() && (os.Getenv("TEST_DATABASE") == "yes" || os.Getenv("CONNECTOR") == "materialize-oracle")
}

func TestMain(m *testing.M) {
	flag.Parse()
	if !integrationTestsEnabled() || os.Getenv("ORACLE_ADDRESS") != "" {
		os.Exit(m.Run())
	}

	run := func(script string) error {
		cmd := exec.Command("bash", "../tests/materialize-oracle/"+script)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		return cmd.Run()
	}
	code := 1
	if err := run("setup.sh"); err != nil {
		fmt.Fprintln(os.Stderr, "starting Oracle:", err)
	} else {
		code = m.Run()
	}
	if err := run("cleanup.sh"); err != nil {
		fmt.Fprintln(os.Stderr, "stopping Oracle:", err)
		code = 1
	}
	os.Exit(code)
}

func integrationConfig() config {
	cfg := testConfig()
	cfg.Address = "localhost:1522"
	cfg.HardDelete = true
	for _, item := range []struct {
		env string
		out *string
	}{
		{"ORACLE_ADDRESS", &cfg.Address},
		{"ORACLE_SERVICE_NAME", &cfg.ServiceName},
		{"ORACLE_USER", &cfg.User},
		{"ORACLE_PASSWORD", &cfg.Password},
		{"ORACLE_SCHEMA", &cfg.Schema},
	} {
		if value := os.Getenv(item.env); value != "" {
			*item.out = value
		}
	}
	return cfg
}

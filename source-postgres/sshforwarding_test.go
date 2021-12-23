package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"testing"

	sf "github.com/estuary/connectors/ssh-forwarding-service"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

// Configuration set based on:
// - source-postgres/docker-compose.yaml
// - ssh-forwarding-service/docker-compose.yaml
func createPostgresqlForwardingTestConfig() (*sf.SshForwardingConfig, error) {
	var b, err = os.ReadFile("../ssh-forwarding-service/test_sshd_configs/keys/id_rsa")
	if err != nil {
		return nil, err
	}
	return &sf.SshForwardingConfig{
		SshEndpoint:         "localhost:2222",
		SshPrivateKeyBase64: base64.RawStdEncoding.EncodeToString(b),
		SshUser:             "flowssh",
		RemoteHost:          "127.0.0.1",
		RemotePort:          5432,
	}, nil
}

func TestSshForwardConfig_Postgresql(t *testing.T) {
	config, err := createPostgresqlForwardingTestConfig()
	require.NoError(t, err)
	port, err := config.StartWithDefault(0, 1)
	require.NoError(t, err)
	require.GreaterOrEqual(t, port, uint16(10000))

	var ctx = context.Background()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://flow:flow@localhost:%d/flow", port))
	defer conn.Close(ctx)

	var result int
	err = conn.QueryRow(context.Background(), "select 1 +1").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 2, result)
}

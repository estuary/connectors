package main

import (
	"context"
	"fmt"
	"testing"

	np "github.com/estuary/connectors/network-proxy-service"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

const TestKeyFilePath = "../network-proxy-service/sshforwarding/test_sshd_configs/keys/id_rsa"

func TestSshForwardConfig_Postgresql(t *testing.T) {
	config, err := np.CreateSshForwardingTestConfig(TestKeyFilePath, 5432)
	fmt.Printf("%+v", config)
	require.NoError(t, err)
	err = config.Start()
	require.NoError(t, err)

	var ctx = context.Background()
	conn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://flow:flow@localhost:%d/flow", config.SshForwardingConfig.LocalPort))
	defer conn.Close(ctx)

	var result int
	err = conn.QueryRow(context.Background(), "select 1 + 1").Scan(&result)
	require.NoError(t, err)
	require.Equal(t, 2, result)
}

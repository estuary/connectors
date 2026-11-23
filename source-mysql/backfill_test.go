package main

import (
	"testing"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/stretchr/testify/require"
)

func TestBackfillSelectFromQuoting(t *testing.T) {
	var info = &sqlcapture.DiscoveryInfo{Schema: "my`db", Name: "ev`ents"}
	require.Equal(t, "SELECT * FROM `my``db`.`ev``ents`", backfillSelectFrom(info))
}

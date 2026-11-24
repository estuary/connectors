package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/microsoftgraph/msgraph-sdk-go/models"
	"github.com/stretchr/testify/require"
)

func TestSpec(t *testing.T) {
	parserSpec, err := os.ReadFile("tests/parser_spec.json")
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(configSchema(json.RawMessage(parserSpec)), "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestSortByPath(t *testing.T) {
	var file = func(name string) models.DriveItemable {
		var item = models.NewDriveItem()
		item.SetName(&name)
		return item
	}
	var folder = func(name string) models.DriveItemable {
		var item = file(name)
		item.SetFolder(models.NewFolder())
		return item
	}

	// In the order Graph's `$orderby=name` lists them.
	var items = []models.DriveItemable{
		file("12-FEB.csv"), file("19-SEPT.csv"), file("1-OCT.csv"), folder("a"), file("a-b.csv"),
	}
	sortByPath(items)

	var names []string
	for _, item := range items {
		names = append(names, *item.GetName())
	}
	require.Equal(t, []string{"1-OCT.csv", "12-FEB.csv", "19-SEPT.csv", "a-b.csv", "a"}, names)
}

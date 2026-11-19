package main

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/drive/v3"
)

func TestSortByPath(t *testing.T) {
	var files = []*drive.File{
		{Name: "report", MimeType: folderMimeType},
		{Name: "report.csv"},
		{Name: "report-2.csv"},
	}
	sortByPath(files)

	var names []string
	for _, file := range files {
		names = append(names, file.Name)
	}
	require.Equal(t, []string{"report-2.csv", "report.csv", "report"}, names)
}

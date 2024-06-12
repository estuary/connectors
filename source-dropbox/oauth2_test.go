package main

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCredentials_GetOauthClient(t *testing.T) {
	cfg := getTestConfig(t)
	ctx := context.TODO()
	client := cfg.Credentials.GetOauthClient(ctx)

	url := "https://api.dropboxapi.com/2/files/list_folder"
	method := "POST"

	payload := strings.NewReader(`{
		"path": "/test", 
		"limit": 1,
		"recursive": true, 
	  	"include_media_info": true, 
	  	"include_deleted": true, 
	  	"include_has_explicit_shared_members": false, 
	  	"include_mounted_folders": true, 
	  	"include_non_downloadable_files": true
  	}`)

	req, err := http.NewRequest(method, url, payload)
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
}

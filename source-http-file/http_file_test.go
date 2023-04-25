package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigValidation(t *testing.T) {
	var cases = []struct {
		url  string
		err  string
		name string
	}{
		{
			url: "this is \x7f invalid",
			err: "URL is invalid: parse \"this is \\x7f invalid\": net/url: invalid control character in URL",
		},
		{
			url: "example.com/foobar",
			err: "URL \"example.com/foobar\" is not valid (it must be a complete URL with an HTTP scheme and domain)",
		},
		{
			url: "./relative/path",
			err: "URL \"./relative/path\" is not valid (it must be a complete URL with an HTTP scheme and domain)",
		},
		{
			url:  "https://example.com:8081/path/to//thing",
			name: "thing",
		},
		{
			url:  "https://example.com:8081/path/to//thing//",
			name: "thing",
		},
		{
			url:  "https://foo:bar@example.com:8081",
			name: "example.com",
		},
	}
	for _, tc := range cases {
		if tc.err != "" {
			require.EqualError(t, (&config{URL: tc.url}).Validate(), tc.err)
		} else {
			require.NoError(t, (&config{URL: tc.url}).Validate())
			require.Equal(t, tc.name, (&config{URL: tc.url}).DiscoverRoot())
		}
	}
}

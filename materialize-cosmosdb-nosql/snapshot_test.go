package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/stretchr/testify/require"
)

type snapshotResponse struct {
	status       int
	body         string
	continuation string
}

type snapshotTransport struct {
	base      *cosmosFixture
	responses []snapshotResponse
	next      int
}

func (f *snapshotTransport) Do(req *http.Request) (*http.Response, error) {
	if !strings.EqualFold(req.Header.Get("x-ms-documentdb-query"), "true") {
		return f.base.Do(req)
	}
	if req.Method != http.MethodPost || req.URL.Path != "/dbs/test-database/colls/orders/docs" {
		return nil, fmt.Errorf("unexpected snapshot query: %s %s", req.Method, req.URL.Path)
	}
	if f.next >= len(f.responses) {
		return nil, fmt.Errorf("unexpected query after the last page")
	}
	wantContinuation := ""
	if f.next > 0 {
		wantContinuation = f.responses[f.next-1].continuation
	}
	if req.Header.Get("x-ms-continuation") != wantContinuation {
		return nil, fmt.Errorf("query did not continue from the previous page")
	}
	if req.Header.Get("x-ms-documentdb-query-enablecrosspartition") != "true" || req.Header.Get("x-ms-documentdb-partitionkey") != "" {
		return nil, fmt.Errorf("snapshot must query all partitions")
	}
	var query struct {
		Query string `json:"query"`
	}
	if err := json.NewDecoder(req.Body).Decode(&query); err != nil {
		return nil, err
	} else if query.Query != "SELECT * FROM c" {
		return nil, fmt.Errorf("unexpected query %q", query.Query)
	}
	page := f.responses[f.next]
	f.next++
	header := http.Header{"Content-Type": {"application/json"}}
	if page.continuation != "" {
		header.Set("x-ms-continuation", page.continuation)
	}
	return &http.Response{
		StatusCode: page.status,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(page.body)),
		Request:    req,
	}, nil
}

func TestSnapshotResource(t *testing.T) {
	for _, tt := range []struct {
		name      string
		responses []snapshotResponse
		want      string
		wantError string
	}{
		{
			name: "all pages and exact numbers",
			responses: []snapshotResponse{
				{http.StatusOK, `{"Documents":[{"id":"2","_flow_id":2,"amount":9007199254740993,"_rid":"rid","_self":"self","_etag":"etag","_attachments":"attachments","_ts":123,"_lsn":456}]}`, "next-1"},
				{http.StatusOK, `{"Documents":[]}`, "next-2"},
				{http.StatusOK, `{"Documents":[{"id":"1","_flow_id":1,"nested":{"exact":18446744073709551615}}]}`, ""},
			},
			want: "{\"_flow_id\":1,\"amount\":null,\"id\":\"1\",\"nested\":{\"exact\":18446744073709551615}}\n" +
				"{\"_flow_id\":2,\"amount\":9007199254740993,\"id\":\"2\",\"nested\":null}\n",
		},
		{
			name:      "empty container",
			responses: []snapshotResponse{{http.StatusOK, `{"Documents":[]}`, ""}},
		},
		{
			name: "query failure after a page",
			responses: []snapshotResponse{
				{http.StatusOK, `{"Documents":[{"id":"1"}]}`, "next-1"},
				{http.StatusForbidden, `{"code":"Forbidden","message":"test failure"}`, ""},
			},
			wantError: "querying container",
		},
		{
			name:      "invalid item",
			responses: []snapshotResponse{{http.StatusOK, `{"Documents":[42]}`, ""}},
			wantError: "decoding item",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			base, d := newCosmosFixture()
			transport := &snapshotTransport{base: base, responses: tt.responses}
			d.clientOptions.Transport = transport
			ctx := context.Background()
			mat, err := d.newMaterializer(ctx, "test/snapshot", testConfig(), nil)
			require.NoError(t, err)
			defer mat.Close(ctx)
			got, err := boilerplate.SnapshotResource(ctx, mat, []string{"test-database", "orders"})
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				require.Empty(t, got)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, got)
			}
			require.Equal(t, len(tt.responses), transport.next)
		})
	}
}

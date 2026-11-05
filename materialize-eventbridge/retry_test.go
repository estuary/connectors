package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"testing/synctest"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/smithy-go"
	"github.com/estuary/connectors/go/retry"
	"github.com/stretchr/testify/require"
)

func TestClassify(t *testing.T) {
	require.Equal(t, retry.Retryable, classify(&failedEntriesError{count: 1, code: "ThrottlingException"}))
	require.Equal(t, retry.Fatal, classify(fmt.Errorf("PutEvents: %w", &smithy.GenericAPIError{Code: "ThrottlingException"})))
	require.Equal(t, retry.Fatal, classify(fmt.Errorf("entry 0: MalformedDetail: bad")))
	require.Equal(t, retry.Fatal, classify(context.Canceled))
}

// fakeEventBridge answers each PutEvents call with the next of its responses,
// and keeps the details of the entries each call was sent.
type fakeEventBridge struct {
	responses []string
	sent      [][]string
}

func (f *fakeEventBridge) Do(req *http.Request) (*http.Response, error) {
	var body struct{ Entries []struct{ Detail string } }
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return nil, err
	}
	var details []string
	for _, e := range body.Entries {
		details = append(details, e.Detail)
	}
	f.sent = append(f.sent, details)

	var resp = f.responses[0]
	if len(f.responses) > 1 {
		f.responses = f.responses[1:]
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.1"}},
		Body:       io.NopCloser(bytes.NewBufferString(resp)),
	}, nil
}

func TestPutEventsRetry(t *testing.T) {
	const (
		ok        = `{"EventId":"id"}`
		throttled = `{"ErrorCode":"ThrottlingException","ErrorMessage":"Rate exceeded"}`
		malformed = `{"ErrorCode":"MalformedDetail","ErrorMessage":"Detail is malformed."}`
	)

	for _, tt := range []struct {
		name      string
		responses []string
		wantSent  [][]string
		wantErr   string
	}{
		{
			name: "re-sends only the entries that failed transiently",
			responses: []string{
				`{"FailedEntryCount":1,"Entries":[` + ok + `,` + throttled + `,` + ok + `]}`,
				`{"FailedEntryCount":0,"Entries":[` + ok + `]}`,
			},
			wantSent: [][]string{{"a", "b", "c"}, {"b"}},
		},
		{
			name: "fails fast on a permanent failure",
			responses: []string{
				`{"FailedEntryCount":2,"Entries":[` + throttled + `,` + malformed + `,` + ok + `]}`,
			},
			wantSent: [][]string{{"a", "b", "c"}},
			wantErr:  "entry 1: MalformedDetail: Detail is malformed.",
		},
		{
			name: "gives up on entries that keep failing",
			responses: []string{
				`{"FailedEntryCount":1,"Entries":[` + ok + `,` + throttled + `,` + ok + `]}`,
				`{"FailedEntryCount":1,"Entries":[` + throttled + `]}`,
			},
			wantSent: [][]string{{"a", "b", "c"}, {"b"}, {"b"}, {"b"}},
			wantErr:  "after 4 attempts: 1 entries failed, first with ThrottlingException: Rate exceeded",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var fake = &fakeEventBridge{responses: tt.responses}
				var tr = &transactor{client: eventbridge.New(eventbridge.Options{
					Region:       "us-east-1",
					BaseEndpoint: aws.String("http://events.test"),
					Credentials:  aws.AnonymousCredentials{},
					HTTPClient:   fake,
					Retryer:      aws.NopRetryer{},
				})}

				var entries []types.PutEventsRequestEntry
				for _, d := range []string{"a", "b", "c"} {
					entries = append(entries, types.PutEventsRequestEntry{Detail: aws.String(d)})
				}

				var err = tr.putEvents(t.Context(), entries)
				if tt.wantErr == "" {
					require.NoError(t, err)
				} else {
					require.EqualError(t, err, tt.wantErr)
				}
				require.Equal(t, tt.wantSent, fake.sent)
			})
		})
	}
}

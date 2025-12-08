package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseDocumentMessage(t *testing.T) {
	tests := []struct {
		name           string
		payload        []byte
		wantDoc        string
		wantSplit      bool
		wantDB         string
		wantColl       string
		wantErr        bool
	}{
		{
			name:      "basic document",
			payload:   buildPayload(msgTypeDocument, 0, "mydb", "mycoll", `{"_id":"test"}`),
			wantDoc:   `{"_id":"test"}`,
			wantSplit: false,
			wantDB:    "mydb",
			wantColl:  "mycoll",
		},
		{
			name:      "with split flag",
			payload:   buildPayload(msgTypeDocument, flagCompletedSplitEvent, "mydb", "mycoll", `{"_id":"test"}`),
			wantDoc:   `{"_id":"test"}`,
			wantSplit: true,
			wantDB:    "mydb",
			wantColl:  "mycoll",
		},
		{
			name:     "empty database name",
			payload:  buildPayload(msgTypeDocument, 0, "", "mycoll", `{"_id":"test"}`),
			wantDoc:  `{"_id":"test"}`,
			wantDB:   "",
			wantColl: "mycoll",
		},
		{
			name:     "empty collection name",
			payload:  buildPayload(msgTypeDocument, 0, "mydb", "", `{"_id":"test"}`),
			wantDoc:  `{"_id":"test"}`,
			wantDB:   "mydb",
			wantColl: "",
		},
		{
			name:     "both empty",
			payload:  buildPayload(msgTypeDocument, 0, "", "", `{"_id":"test"}`),
			wantDoc:  `{"_id":"test"}`,
			wantDB:   "",
			wantColl: "",
		},
		{
			name: "long names",
			payload: buildPayload(msgTypeDocument, 0,
				string(make([]byte, 255)),
				string(make([]byte, 255)),
				`{"_id":"test"}`),
			wantDoc:  `{"_id":"test"}`,
			wantDB:   string(make([]byte, 255)),
			wantColl: string(make([]byte, 255)),
		},
		{
			name:     "UTF-8 names",
			payload:  buildPayload(msgTypeDocument, 0, "数据库", "集合", `{"_id":"test"}`),
			wantDoc:  `{"_id":"test"}`,
			wantDB:   "数据库",
			wantColl: "集合",
		},
		{
			name:     "realistic document",
			payload:  buildPayload(msgTypeDocument, 0, "production", "users", `{"_id":"abc123","name":"John Doe","_meta":{"source":{"db":"production","collection":"users"}}}`),
			wantDoc:  `{"_id":"abc123","name":"John Doe","_meta":{"source":{"db":"production","collection":"users"}}}`,
			wantDB:   "production",
			wantColl: "users",
		},
		{
			name:    "payload too short",
			payload: []byte{msgTypeDocument, 0},
			wantErr: true,
		},
		{
			name:    "db length exceeds payload",
			payload: []byte{msgTypeDocument, 0, 100}, // claims 100 bytes but payload ends
			wantErr: true,
		},
		{
			name:    "coll length exceeds payload",
			payload: []byte{msgTypeDocument, 0, 2, 'd', 'b', 100}, // db ok, but coll claims 100 bytes
			wantErr: true,
		},
		{
			name:    "no json after collection",
			payload: buildPayload(msgTypeDocument, 0, "db", "coll", ""),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc, split, db, coll, err := parseDocumentMessage(tt.payload)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantSplit, split)
			require.Equal(t, tt.wantDB, db)
			require.Equal(t, tt.wantColl, coll)

			if tt.wantDoc != "" {
				require.Equal(t, tt.wantDoc, string(doc))
			}
		})
	}
}

func TestParseDocumentMessageRoundTrip(t *testing.T) {
	// Simulate what Rust would send
	db := "testDatabase"
	coll := "testCollection"
	jsonDoc := `{"_id":"abc123","name":"test","_meta":{"source":{"db":"testDatabase","collection":"testCollection"}}}`

	payload := buildPayload(msgTypeDocument, 0, db, coll, jsonDoc)

	doc, split, parsedDB, parsedColl, err := parseDocumentMessage(payload)
	require.NoError(t, err)
	require.False(t, split)
	require.Equal(t, db, parsedDB)
	require.Equal(t, coll, parsedColl)
	require.Equal(t, jsonDoc, string(doc))
}

// buildPayload constructs a binary protocol payload for testing
func buildPayload(msgType, flags uint8, db, coll, jsonDoc string) []byte {
	payload := []byte{msgType, flags}

	// Database
	payload = append(payload, uint8(len(db)))
	payload = append(payload, []byte(db)...)

	// Collection
	payload = append(payload, uint8(len(coll)))
	payload = append(payload, []byte(coll)...)

	// JSON
	payload = append(payload, []byte(jsonDoc)...)

	return payload
}

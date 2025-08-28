package main

import (
	"context"
	"crypto/rsa"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"resty.dev/v3"
)

type fileColumnProperties struct {
	ColumnOrdinal     int             `json:"columnId"`
	NullCount         int             `json:"nullCount"`
	MaxStrValue       *string         `json:"maxStrValue"` // hex-encoded & truncated up to 32 bytes
	MinStrValue       *string         `json:"minStrValue"` // hex-encoded & truncated down to 32 bytes
	MaxLength         int             `json:"maxLength"`   // maximum string length
	MaxIntValue       json.RawMessage `json:"maxIntValue"`
	MinIntValue       json.RawMessage `json:"minIntValue"`
	MaxRealValue      json.RawMessage `json:"maxRealValue"`
	MinRealValue      json.RawMessage `json:"minRealValue"`
	DistinctValues    int             `json:"distinctValues"`    // always -1
	Collation         *string         `json:"collation"`         // always null
	MinStrNonCollated *string         `json:"minStrNonCollated"` // always null
	MaxStrNonCollated *string         `json:"maxStrNonCollated"` // always null
}

type eps struct {
	Rows    int                             `json:"rows"`
	Columns map[string]fileColumnProperties `json:"columns"`
}

type tableColumn struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	LogicalType  string `json:"logical_type"`
	PhysicalType string `json:"physical_type"`
	Precision    *int   `json:"precision"`
	Scale        *int   `json:"scale"`
	ByteLength   *int   `json:"byte_length"`
	Length       *int   `json:"length"`
	Nullable     bool   `json:"nullable"`
	Ordinal      int    `json:"ordinal"`
}

type channelMetadata struct {
	ClientSequencer int           `json:"client_sequencer"`
	RowSequencer    int           `json:"row_sequencer"`
	EncryptionKeyId int           `json:"encryption_key_id"`
	EncryptionKey   string        `json:"encryption_key"`
	TableColumns    []tableColumn `json:"table_columns"`
	OffsetToken     *string       `json:"offset_token"`
}

type uploadChunkChannelMetadata struct {
	Channel         string `json:"channel_name"`
	ClientSequencer int    `json:"client_sequencer"`
	RowSequencer    int    `json:"row_sequencer"`
	OffsetToken     string `json:"offset_token"`
	// Not included: last_token and next_token, which we don't do anything with
	// and don't seem to have any effect.
}

type uploadChunkMetadata struct {
	Database                string                       `json:"database"`
	Schema                  string                       `json:"schema"`
	Table                   string                       `json:"table"`
	ChunkStartOffset        int                          `json:"chunk_start_offset"`
	ChunkLength             int                          `json:"chunk_length"`
	ChunkLengthUncompressed int                          `json:"chunk_length_uncompressed"`
	Channels                []uploadChunkChannelMetadata `json:"channels"`
	ChunkMD5                string                       `json:"chunk_md5"` // hex encoded
	EPS                     eps                          `json:"eps"`
	EncryptionKeyID         int                          `json:"encryption_key_id"`
	FirstInsertTimeInMillis int64                        `json:"first_insert_time_in_ms"`
	LastInsertTimeInMillis  int64                        `json:"last_insert_time_in_ms"`
}

type blobStats struct {
	FlushStartMs     int64 `json:"flush_start_ms"`
	BuildDurationMs  int64 `json:"build_duration_ms"`
	UploadDurationMs int64 `json:"upload_duration_ms"`
}

type blobMetadata struct {
	Path        string                `json:"path"`
	MD5         string                `json:"md5"` // hex encoded
	Chunks      []uploadChunkMetadata `json:"chunks"`
	BDECVersion int                   `json:"bdec_version"` // always 3
	BlobStats   blobStats             `json:"blob_stats"`
}

type streamConfig struct {
	Prefix        string `json:"prefix"`
	StageLocation struct {
		LocationType   string `json:"locationType"`
		Location       string `json:"location"`
		StorageAccount string `json:"storageAccount"` // for Azure
		Endpoint       string `json:"endPoint"`       // for Azure
		Creds          struct {
			// For AWS authorization.
			AwsKeyId     string `json:"AWS_KEY_ID"`
			AwsSecretKey string `json:"AWS_SECRET_KEY"`
			AwsToken     string `json:"AWS_TOKEN"`

			// For GCS authorization.
			GcsAccessToken string `json:"GCS_ACCESS_TOKEN"`

			// For Azure authorization.
			AzureSasToken string `json:"AZURE_SAS_TOKEN"`
		} `json:"creds"`
	} `json:"stage_location"`
	DeploymentID int    `json:"deployment_id"`
	Message      string `json:"message"`
	StatusCode   int    `json:"status_code"`
}

type channel struct {
	Channel         string        `json:"channel"`
	Database        string        `json:"database"`
	Schema          string        `json:"schema"`
	Table           string        `json:"table"`
	OffsetToken     *string       `json:"offset_token"`
	ClientSequencer int           `json:"client_sequencer"`
	RowSequencer    int           `json:"row_sequencer"`
	EncryptionKey   string        `json:"encryption_key"`
	EncryptionKeyId int           `json:"encryption_key_id"`
	TableColumns    []tableColumn `json:"table_columns"`
	Message         string        `json:"message"`
	StatusCode      int           `json:"status_code"`
}

type writeResponse struct {
	Blobs []struct {
		Chunks []struct {
			Channels []struct {
				Channel         string `json:"channel"`
				ClientSequencer int    `json:"client_sequencer"`
				StatusCode      int    `json:"status_code"`
			} `json:"channels"`
			Database string `json:"database"`
			Schema   string `json:"schema"`
			Table    string `json:"table"`
		} `json:"chunks"`
	} `json:"blobs"`
	Message    string `json:"message"`
	StatusCode int    `json:"status_code"`
}

type channelStatusResponse struct {
	Channels []struct {
		PersistedClientSequencer int    `json:"persisted_client_sequencer"`
		PersistedRowSequencer    int    `json:"persisted_row_sequencer"`
		PersistedOffsetToken     string `json:"persisted_offset_token"`
		StatusCode               int    `json:"status_code"`
	} `json:"channels"`
	Message    string `json:"message"`
	StatusCode int    `json:"status_code"`
}

type streamClient struct {
	r        *resty.Client
	key      *rsa.PrivateKey
	user     string
	database string
	account  string
	role     *string

	mu          sync.Mutex
	cachedToken string
	expiry      time.Time
}

func newStreamClient(cfg *config, account string) (*streamClient, error) {
	var role *string

	key, err := cfg.Credentials.ParsePrivateKey()
	if err != nil {
		return nil, err
	}
	if cfg.Role != "" {
		role = &cfg.Role
	}

	return &streamClient{
		r:        resty.New().SetBaseURL("https://" + path.Join(cfg.Host, "v1/streaming")),
		key:      key,
		user:     cfg.Credentials.User,
		database: cfg.Database,
		account:  account,
		role:     role,
	}, nil
}

func (s *streamClient) configure(ctx context.Context) (*streamConfig, error) {
	type req struct {
		Role *string `json:"role,omitempty"`
	}

	res, err := post[streamConfig](ctx, s, "/client/configure", req{
		Role: s.role,
	})
	if err != nil {
		return nil, err
	} else if err := getErrorByCode(res.StatusCode); err != nil {
		return nil, fmt.Errorf("request was not successful: %w", err)
	} else if res.Message != "Success" {
		return nil, fmt.Errorf("unexpected response message: %s", res.Message)
	}

	return res, nil
}

func (s *streamClient) openChannel(ctx context.Context, schema, table, name string) (*channel, error) {
	type req struct {
		Role      *string `json:"role,omitempty"`
		Database  string  `json:"database"`
		Schema    string  `json:"schema"`
		Table     string  `json:"table"`
		Channel   string  `json:"channel"`
		WriteMode string  `json:"write_mode"`
	}

	res, err := post[channel](ctx, s, "/channels/open", req{
		Role:      s.role,
		Database:  s.database,
		Schema:    schema,
		Table:     table,
		Channel:   name,
		WriteMode: "CLOUD_STORAGE",
	})
	if err != nil && res == nil {
		return nil, err
	} else if codeErr := getErrorByCode(res.StatusCode); codeErr != nil {
		return nil, fmt.Errorf("request returned error code: %w", codeErr)
	} else if err != nil {
		return nil, fmt.Errorf("error opening channel with a non-error status code: %w", err)
	} else if res.Message != "Success" {
		return nil, fmt.Errorf("unexpected response message: %s", res.Message)
	}

	return res, nil
}

func (s *streamClient) write(ctx context.Context, blob *blobMetadata) error {
	type req struct {
		Role  *string         `json:"role,omitempty"`
		Blobs []*blobMetadata `json:"blobs"`
	}

	res, err := post[writeResponse](ctx, s, "/channels/write/blobs", req{
		Role: s.role,
		Blobs: []*blobMetadata{
			blob,
		},
	})
	if err != nil {
		return err
	} else if err := getErrorByCode(res.StatusCode); err != nil {
		return fmt.Errorf("request was not successful: %w", err)
	} else if res.Message != "Success" {
		return fmt.Errorf("unexpected response message: %s", res.Message)
	}

	for _, blob := range res.Blobs {
		for _, chunk := range blob.Chunks {
			for _, channel := range chunk.Channels {
				if err := getErrorByCode(channel.StatusCode); err != nil {
					return fmt.Errorf("failed to write chunk (schema: %s, table %s): %w", chunk.Schema, chunk.Table, err)
				}
			}
		}
	}

	log.WithFields(log.Fields{
		"path":               blob.Path,
		"md5":                blob.MD5,
		"rows":               blob.Chunks[0].EPS.Rows,
		"length":             blob.Chunks[0].ChunkLength,
		"lengthUncompressed": blob.Chunks[0].ChunkLengthUncompressed,
	}).Info("registered bdec file")

	return nil
}

func (s *streamClient) channelStatus(ctx context.Context, clientSeq int, schema, table, name string) (*channelStatusResponse, error) {
	type channelReq struct {
		Table           string `json:"table"`
		Database        string `json:"database"`
		Schema          string `json:"schema"`
		Name            string `json:"channel_name"`
		ClientSequencer int    `json:"client_sequencer"`
	}

	type req struct {
		Role     *string      `json:"role,omitempty"`
		Channels []channelReq `json:"channels"`
	}

	res, err := post[channelStatusResponse](ctx, s, "/channels/status", req{
		Role: s.role,
		Channels: []channelReq{
			{
				Table:           table,
				Database:        s.database,
				Schema:          schema,
				Name:            name,
				ClientSequencer: clientSeq,
			},
		},
	})
	if err != nil {
		return nil, err
	} else if err := getErrorByCode(res.StatusCode); err != nil {
		return nil, fmt.Errorf("request was not successful: %w", err)
	} else if res.Message != "Success" {
		return nil, fmt.Errorf("unexpected response message: %s", res.Message)
	}

	for _, channel := range res.Channels {
		if err := getErrorByCode(channel.StatusCode); err != nil {
			return nil, fmt.Errorf("failed to get channel status (schema: %s, table %s): %w", schema, table, err)
		}
	}

	return res, nil
}

func (s *streamClient) waitForTokenPersisted(ctx context.Context, token string, clientSeq int, schema, table, name string) error {
	maxBackoff := 1 * time.Second
	backoff := 100 * time.Millisecond
	ts := time.Now()
	for n := 1; ; n++ {
		if status, err := s.channelStatus(ctx, clientSeq, schema, table, name); err != nil {
			return err
		} else if len(status.Channels) != 1 {
			return fmt.Errorf("expected 1 channel but got %d", len(status.Channels))
		} else if status.Channels[0].PersistedOffsetToken == token {
			break
		}

		if n > 10 {
			log.WithFields(log.Fields{
				"attempt": n,
				"schema":  schema,
				"table":   table,
				"token":   token,
			}).Info("channel offset token not yet persisted")
		}

		time.Sleep(backoff)
		backoff = min(backoff*2, maxBackoff)
	}

	log.WithFields(log.Fields{
		"schema": schema,
		"table":  table,
		"token":  token,
		"took":   time.Since(ts).String(),
	}).Debug("channel offset token persisted")

	return nil
}

func post[T any](ctx context.Context, c *streamClient, path string, body any) (*T, error) {
	c.mu.Lock()
	var token string
	if err := func() error {
		defer c.mu.Unlock()

		if time.Until(c.expiry).Minutes() < 5 {
			var err error
			c.cachedToken, c.expiry, err = generateJWTToken(c.key, c.user, c.account)
			if err != nil {
				return err
			}
		}

		token = c.cachedToken
		return nil
	}(); err != nil {
		return nil, err
	}

	req := c.r.
		NewRequest().
		WithContext(ctx).
		SetHeader("Accept", "application/json").
		SetHeader("User-Agent", "EstuaryTechnologiesFlow/4.0.0 Estuary"). // A semver >= 2.0.0 seems to be necessary for GCS to return Oauth client credentials rather than pre-signed URLs
		SetAuthToken(token).
		SetError(&streamingApiError{})

	var res T
	got, err := req.SetResult(&res).SetBody(body).Post(path)
	if err != nil {
		return nil, fmt.Errorf("failed to POST %s: %w", path, err)
	}
	if !got.IsSuccess() {
		return &res, fmt.Errorf("failed to POST %s: %s: %w", got.Request.URL, got.Status(), got.Error().(error))
	}

	return &res, nil
}

// Ref: https://github.com/snowflakedb/snowflake-ingest-java/blob/3cbaebfe26f59dc3a8b8e973649e3f1a1014438c/src/main/java/net/snowflake/ingest/streaming/internal/StreamingIngestResponseCode.java
var streamingIngestResponseCodes = map[int]string{
	4:  "the supplied table does not exist or is not authorized",
	5:  "channel limit exceeded for the given table, please contact Snowflake support to raise this limit",
	6:  "Snowpipe Streaming is not supported on the type of table resolved, or the table contains columns that are either AUTOINCREMENT or IDENTITY columns or a column with a default value",
	7:  "the table has exceeded its limit of outstanding registration requests, please wait before calling the insertRow or insertRows API",
	8:  "the supplied database does not exist or is not authorized",
	9:  "the supplied schema does not exist or is not authorized",
	10: "Snowflake experienced a transient exception, please retry the request",
	11: "malformed request: missing blobs in request",
	12: "malformed request: missing chunks for blob in request",
	13: "malformed request: missing channels for chunk for blob in request",
	14: "malformed request: missing blob path",
	15: "malformed request: missing database in request",
	16: "malformed request: missing schema in request",
	17: "malformed request: missing table in request",
	18: "malformed request: missing channel name in request",
	19: "the requested channel no longer exists, most likely due to inactivity. Please re-open the channel",
	20: "the channel is owned by another writer at this time. Either re-open the channel to gain exclusive write ownership or close the channel and let the other writer continue",
	21: "the client provided an out-of-order message, please reopen the channel",
	22: "another channel managed by this client had an issue which is preventing the current channel from ingesting data. Please re-open the channel",
	23: "malformed request: duplicate channel in chunk",
	24: "malformed request: missing client sequencer in request",
	25: "the channel does not exist or is not authorized",
	26: "the requested row sequencer is committed",
	27: "the requested row sequencer is not committed",
	28: "malformed request: missing chunk MD5 in request",
	29: "malformed request: missing ep_info in request",
	30: "malformed request: invalid chunk length",
	31: "malformed request: invalid row count in ep_info",
	32: "malformed request: invalid column in ep_info",
	33: "malformed request: invalid ep_info (generic)",
	34: "malformed request: invalid blob name",
	36: "the channel must be reopened",
	37: "malformed request: missing role in request",
	38: "malformed request: blob has wrong format or extension",
	39: "malformed request: blob missing MD5",
	40: "a schema change occurred on the table, please re-open the channel and supply the missing data",
	41: "duplicated blob in request",
	42: "unsupported blob file version",
	43: "outdated Client SDK. Please update your SDK to the latest version",
	44: "malformed request: undefined user agent header",
	45: "malformed request: malformed user agent header",
	46: "interleaving among tables is not supported at this time",
	47: "table is read-only",
	48: "the request contains invalid column metadata, please contact Snowflake support",
	49: "ingestion into this table is not allowed at this time, please contact Snowflake support",
	// I'm not sure what 50...54 are, as they aren't listed in the same place. I
	// found error code 55 by experimentation.
	55: "Snowpipe Streaming does not support columns of type AUTOINCREMENT, IDENTITY, GEO, or columns with a default value or collation",
}

type streamingApiError struct {
	Code    int    `json:"status_code"`
	Message string `json:"message"`
}

func (e *streamingApiError) Error() string {
	return fmt.Sprintf("%s (code %d)", e.Message, e.Code)
}

func getErrorByCode(code int) error {
	if code == 0 {
		return nil
	}

	if msg, ok := streamingIngestResponseCodes[code]; ok {
		return &streamingApiError{Code: code, Message: msg}
	}

	return fmt.Errorf("unknown status message for code %d", code)
}

func generateBlobMetadata(
	tracked *blobStatsTracker,
	channel *channel,
	token string,
) *blobMetadata {
	md5 := hex.EncodeToString(tracked.md5.Sum(nil))

	out := blobMetadata{
		Path:        string(tracked.fileName),
		MD5:         md5,
		BDECVersion: 3,
		BlobStats: blobStats{
			FlushStartMs:     tracked.start.UnixMilli(),
			BuildDurationMs:  tracked.finish.Sub(tracked.start).Milliseconds(),
			UploadDurationMs: tracked.finish.Sub(tracked.start).Milliseconds(),
		},
	}

	chunkMeta := uploadChunkMetadata{
		Database:                channel.Database,
		Schema:                  channel.Schema,
		Table:                   channel.Table,
		ChunkStartOffset:        0,
		ChunkLength:             tracked.length,
		ChunkLengthUncompressed: tracked.lengthUncompressed,
		Channels: []uploadChunkChannelMetadata{
			{
				Channel:         channel.Channel,
				ClientSequencer: 0, // not checkpointed
				RowSequencer:    0, // not checkpointed
				OffsetToken:     token,
			},
		},
		ChunkMD5: md5,
		EPS: eps{
			Rows:    tracked.rows,
			Columns: make(map[string]fileColumnProperties),
		},
		EncryptionKeyID:         channel.EncryptionKeyId,
		FirstInsertTimeInMillis: tracked.start.UnixMilli(),
		LastInsertTimeInMillis:  tracked.finish.UnixMilli(),
	}

	for _, col := range tracked.columns {
		props := fileColumnProperties{
			ColumnOrdinal:  col.ordinal,
			NullCount:      col.nullCount,
			DistinctValues: -1,
		}

		if col.isInt {
			props.MaxIntValue = json.RawMessage(col.intStats.maxVal.BigInt().String())
			props.MinIntValue = json.RawMessage(col.intStats.minVal.BigInt().String())
		} else if col.isNum {
			props.MaxRealValue = marshalFloat(col.numStats.maxVal)
			props.MinRealValue = marshalFloat(col.numStats.minVal)
		} else if col.isStr {
			maxHex := truncateBytesAsHex([]byte(col.strStats.maxVal), true)
			minHex := truncateBytesAsHex([]byte(col.strStats.minVal), false)
			props.MaxLength = col.strStats.maxLen
			props.MaxStrValue = &maxHex
			props.MinStrValue = &minHex
		}

		if !col.isInt {
			props.MaxIntValue = json.RawMessage([]byte("0"))
			props.MinIntValue = json.RawMessage([]byte("0"))
		}

		chunkMeta.EPS.Columns[col.name] = props
	}

	out.Chunks = []uploadChunkMetadata{chunkMeta}

	log.WithFields(log.Fields{
		"fileName":                             tracked.fileName,
		"md5":                                  md5,
		"rows":                                 tracked.rows,
		"length":                               tracked.length,
		"lengthUncompressed":                   tracked.lengthUncompressed,
		"parquetMetadata.NumRows":              tracked.parquetMetadata.FileMetaData.NumRows,
		"parquetMetadata.NumRowGroups":         len(tracked.parquetMetadata.FileMetaData.RowGroups),
		"parquetMetadata.RowGroups[0].NumRows": tracked.parquetMetadata.FileMetaData.RowGroups[0].NumRows,
		"parquetMetadata.RowGroups[0].TotalCompressedSize": tracked.parquetMetadata.FileMetaData.RowGroups[0].TotalCompressedSize,
		"parquetMetadata.RowGroups[0].TotalByteSize":       tracked.parquetMetadata.FileMetaData.RowGroups[0].TotalByteSize,
	}).Info("generated bdec metadata")

	return &out
}

// marshalFloat writes special float values to be compatible with the metadata
// that Snowpipe expects.
func marshalFloat(f float64) json.RawMessage {
	if math.IsNaN(f) {
		return json.RawMessage(`"NaN"`)
	} else if math.IsInf(f, -1) {
		return json.RawMessage(`"-Infinity"`)
	} else if math.IsInf(f, 1) {
		return json.RawMessage(`"Infinity"`)
	}
	b, _ := json.Marshal(f)

	return b
}

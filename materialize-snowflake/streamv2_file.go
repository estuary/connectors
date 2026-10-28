package connector

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/estuary/connectors/go/blob"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

// streamV2EncryptionVersion is the encryption version a pipe-info request names.
// Snowflake refuses the request without one.
const streamV2EncryptionVersion = "AES-CTR-128.1"

// streamV2StageLocation is a stage bucket Snowflake lends a pipe for file appends,
// with credentials that expire at ExpiryTimeMs.
type streamV2StageLocation struct {
	LocationType   string `json:"location_type"`
	BucketName     string `json:"bucket_name"`
	PathPrefix     string `json:"path_prefix"`
	Region         string `json:"region"`
	StorageAccount string `json:"storage_account"`
	// StorageEndpoint is the Azure blob service host suffix, such as blob.core.windows.net.
	StorageEndpoint string            `json:"storage_endpoint"`
	Creds           map[string]string `json:"creds"`
	ExpiryTimeMs    int64             `json:"expiry_time_ms"`
}

// streamV2PipeInfo is what a pipe lends a client for file appends: a stage to
// upload into and the key material that files registered with the pipe name.
type streamV2PipeInfo struct {
	Stage      streamV2StageLocation `json:"rowset_stage_location"`
	Encryption struct {
		PipeKey string `json:"pipe_key"`
		// PipeKeyID is a number on some accounts and a string on others, so it is
		// echoed back as received.
		PipeKeyID         json.RawMessage `json:"pipe_key_id"`
		Diversifier       string          `json:"diversifier"`
		EncryptionVersion string          `json:"encryption_version"`
	} `json:"pipe_encryption_info"`

	uploader streamV2Uploader
}

// streamV2Uploader writes one object to a stage bucket.
type streamV2Uploader interface {
	Upload(ctx context.Context, key string, r io.Reader, opts ...blob.WriterOption) error
}

// newStreamV2Uploader opens the stage with the credentials Snowflake lent. Each
// upload is a single request, because Snowflake refuses a staged file whose
// object was assembled from parts.
func newStreamV2Uploader(ctx context.Context, stage streamV2StageLocation) (streamV2Uploader, error) {
	switch stage.LocationType {
	case "GCS":
		var token = &oauth2.Token{AccessToken: stage.Creds["GCS_ACCESS_TOKEN"]}
		return blob.NewGCSSingleRequestBucket(ctx, stage.BucketName, option.WithTokenSource(oauth2.StaticTokenSource(token)))
	case "S3":
		var creds = credentials.NewStaticCredentialsProvider(stage.Creds["AWS_KEY_ID"], stage.Creds["AWS_SECRET_KEY"], stage.Creds["AWS_TOKEN"])
		return blob.NewS3SingleRequestBucket(stage.BucketName, strings.ToLower(stage.Region), creds), nil
	case "AZURE":
		return blob.NewAzureSingleRequestBucket(ctx, stage.BucketName, stage.StorageAccount, blob.WithAzureSasToken(stage.Creds["AZURE_SAS_TOKEN"]), blob.WithAzureEndpoint(stage.StorageEndpoint))
	default:
		return nil, fmt.Errorf("Snowflake lent a stage of location type %q, which this connector cannot upload to", stage.LocationType)
	}
}

// pipeInfo returns the pipe's stage and key material, fetching them when none are
// held or the held ones are about to expire. Callers wait while a fetch is on the
// wire, which is fine because file appends are rare.
func (c *streamV2Client) pipeInfo(ctx context.Context, pipePath string) (*streamV2PipeInfo, error) {
	c.pipesMu.Lock()
	defer c.pipesMu.Unlock()

	if info, ok := c.pipes[pipePath]; ok && time.Until(time.UnixMilli(info.Stage.ExpiryTimeMs)) > streamV2TokenRefreshMargin {
		return info, nil
	}
	var body, _ = json.Marshal(map[string]any{"generate_new_token": true, "encryption_version": streamV2EncryptionVersion})
	var info = new(streamV2PipeInfo)
	if err := c.call(ctx, http.MethodPost, pipePath+":pipe-info", nil, body, nil, info); err != nil {
		return nil, err
	}
	var uploader, err = c.newUploader(ctx, info.Stage)
	if err != nil {
		return nil, err
	}
	info.uploader = uploader
	c.pipes[pipePath] = info
	return info, nil
}

// streamV2FileFragment describes one run of rows in a staged file to the channel
// it is appended to.
type streamV2FileFragment struct {
	FilePath                        string          `json:"file_path"`
	ContentType                     string          `json:"content_type"`
	FileEtag                        string          `json:"file_etag"`
	FileLengthBytes                 int             `json:"file_length_bytes"`
	FragmentLengthBytes             int             `json:"fragment_length_bytes"`
	FragmentLengthUncompressedBytes int             `json:"fragment_length_uncompressed_bytes"`
	FragmentStartOffsetToken        string          `json:"fragment_start_offset_token"`
	FragmentEndOffsetToken          string          `json:"fragment_end_offset_token"`
	FragmentRowCount                int             `json:"fragment_row_count"`
	FragmentStartByteOffset         int             `json:"fragment_start_byte_offset"`
	Diversifier                     string          `json:"diversifier"`
	MasterKeyID                     json.RawMessage `json:"master_key_id"`
	IVBase64                        string          `json:"iv_base64"`
	EncryptionVersion               string          `json:"encryption_version"`
	MinAppendTimestampMs            int64           `json:"min_append_timestamp_ms"`
}

// streamV2FileKey decodes the key that files appended to a pipe are encrypted
// under, which is the pipe key as lent.
func streamV2FileKey(pipeKeyBase64 string) ([]byte, error) {
	var pipeKey, err = base64.StdEncoding.DecodeString(pipeKeyBase64)
	if err != nil {
		return nil, fmt.Errorf("decoding the pipe key: %w", err)
	}
	return pipeKey, nil
}

// streamV2CryptFile applies AES-CTR under key and iv to data in place. CTR mode
// is its own inverse, so the same call encrypts and decrypts.
func streamV2CryptFile(key, iv, data []byte) error {
	var block, err = aes.NewCipher(key)
	if err != nil {
		return err
	}
	cipher.NewCTR(block, iv).XORKeyStream(data, data)
	return nil
}

// appendFile appends rows to a channel through the pipe's stage. It encrypts the
// compressed rows under the pipe's key, uploads them as one file, and registers
// the file with the channel as one fragment. The caller holds ch.mu.
//
// The pipe-info and filebuffer endpoints are not documented by Snowflake.
func (c *streamV2Client) appendFile(ctx context.Context, ch *streamV2OpenChannel, channelName, startToken, endToken string, rows, compressed []byte) error {
	var info, err = c.pipeInfo(ctx, ch.pipePath)
	if err != nil {
		return err
	}
	key, err := streamV2FileKey(info.Encryption.PipeKey)
	if err != nil {
		return err
	}
	var iv = make([]byte, aes.BlockSize)
	if _, err := rand.Read(iv); err != nil {
		return err
	}
	if err := streamV2CryptFile(key, iv, compressed); err != nil {
		return err
	}
	var name = info.Stage.PathPrefix + uuid.NewString() + ".ndjson"
	var storeETag string
	if err := info.uploader.Upload(ctx, name, bytes.NewReader(compressed), blob.WithETag(&storeETag)); err != nil {
		return fmt.Errorf("uploading %d bytes to the %s stage of channel %q: %w", len(compressed), info.Stage.LocationType, channelName, streamV2RedactURLQuery(err))
	}
	var sum = md5.Sum(compressed)
	// Snowflake checks a file on an Azure stage against the blob's ETag, and a
	// file on an S3 or GCS stage against the MD5 of its bytes.
	var fileETag = hex.EncodeToString(sum[:])
	if info.Stage.LocationType == "AZURE" {
		fileETag = storeETag
	}
	var fragment = streamV2FileFragment{
		FilePath:                        info.Stage.BucketName + "/" + name,
		ContentType:                     "application/x-ndjson",
		FileEtag:                        fileETag,
		FileLengthBytes:                 len(compressed),
		FragmentLengthBytes:             len(compressed),
		FragmentLengthUncompressedBytes: len(rows),
		FragmentStartOffsetToken:        startToken,
		FragmentEndOffsetToken:          endToken,
		FragmentRowCount:                bytes.Count(rows, []byte{'\n'}),
		Diversifier:                     info.Encryption.Diversifier,
		MasterKeyID:                     info.Encryption.PipeKeyID,
		IVBase64:                        base64.StdEncoding.EncodeToString(iv),
		EncryptionVersion:               info.Encryption.EncryptionVersion,
		MinAppendTimestampMs:            time.Now().UnixMilli(),
	}
	log.WithFields(log.Fields{
		"channel":      channelName,
		"locationType": info.Stage.LocationType,
		"rows":         fragment.FragmentRowCount,
		"bytes":        len(rows),
		"compressed":   len(compressed),
	}).Info("appending rows through a staged file")

	var query = url.Values{
		"continuationToken": {ch.continuation},
		"requestId":         {uuid.NewString()},
	}
	var body, _ = json.Marshal([]streamV2FileFragment{fragment})
	var res struct {
		Continuation string `json:"next_continuation_token"`
	}
	if err := c.call(ctx, http.MethodPost, "data/"+ch.pipePath+"/channels/"+url.PathEscape(channelName)+"/filebuffer", query, body, nil, &res); err != nil {
		return err
	}
	ch.continuation = res.Continuation
	return nil
}

// streamV2RedactURLQuery removes the query of the URL that err names, because a
// stage credential can be lent as that query.
func streamV2RedactURLQuery(err error) error {
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return err
	}
	if u, parseErr := url.Parse(urlErr.URL); parseErr == nil && u.RawQuery != "" {
		u.RawQuery = "REDACTED"
		urlErr.URL = u.String()
	}
	return err
}

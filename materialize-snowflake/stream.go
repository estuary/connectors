package main

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/estuary/connectors/go/blob"
	sql "github.com/estuary/connectors/materialize-sql"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

// channelName produces a reasonably readable channel name that is globally
// unique per materialization and shard. Channels are specific to a table, so
// there is no need to include the database or schema in the name, and they can
// be at least 1000 characters long. It's important for the channel name to
// never change to maintain data consistency.
func channelName(materialization string, keyBegin uint32) string {
	return sanitizeAndAppendHash(materialization) + "_" + fmt.Sprintf("%08x", keyBegin)
}

type tableStream struct {
	mappedColumns []*sql.Column
	channel       *channel
}

// streamManager is a high-level orchestrator for Snowpipe streaming operations,
// which mostly entails writing rows of data for bindings and registering the
// resulting blobs to tables.
type streamManager struct {
	c            *streamClient
	tableStreams map[int]*tableStream
	tenant       string // used for the `ingestclientname` metadata
	keyBegin     uint32
	channelName  string

	// Returned from the "configure" API.
	prefix       string
	deploymentId int
	bucketPath   string

	// For writing blob data to object storage.
	bucket          blob.Bucket
	bucketExpiresAt time.Time
	bdecWriter      *bdecWriter
	counter         int
	lastBinding     int // bookkeeping for when to flush the writer when a new binding starts writing rows

	blobStats map[int][]*blobStatsTracker
}

func newStreamManager(cfg *config, materialization string, tenant string, account string, keyBegin uint32) (*streamManager, error) {
	c, err := newStreamClient(cfg, account)
	if err != nil {
		return nil, fmt.Errorf("newStreamClient: %w", err)
	}

	return &streamManager{
		c:            c,
		tableStreams: make(map[int]*tableStream),
		tenant:       tenant,
		keyBegin:     keyBegin,
		channelName:  channelName(materialization, keyBegin),
		lastBinding:  -1,
		blobStats:    make(map[int][]*blobStatsTracker),
		counter:      0,
	}, nil
}

func (sm *streamManager) addBinding(ctx context.Context, schema string, table sql.Table) error {
	channel, err := sm.c.openChannel(ctx, schema, table.Identifier, sm.channelName)
	if err != nil {
		return fmt.Errorf("openChannel: %w", err)
	}

	// Verify that we support this table's columns for Snowpipe streaming.
	for _, col := range channel.TableColumns {
		if _, err := makeSchemaElement(col); err != nil {
			return err
		}
	}

	sm.tableStreams[table.Binding] = &tableStream{
		mappedColumns: table.Columns(),
		channel:       channel,
	}

	return nil
}

func (sm *streamManager) encodeRow(ctx context.Context, binding int, row []any) error {
	if sm.lastBinding != -1 && binding != sm.lastBinding {
		if err := sm.finishBlob(); err != nil {
			return fmt.Errorf("finishBlob: %w", err)
		}
	}
	sm.lastBinding = binding

	if sm.bdecWriter == nil {
		if err := sm.startNewBlob(ctx, binding); err != nil {
			return fmt.Errorf("startNewBlob: %w", err)
		}
	}

	if err := sm.bdecWriter.encodeRow(row); err != nil {
		return fmt.Errorf("bdecWriter encodeRow: %w", err)
	}

	if sm.bdecWriter.done {
		if err := sm.finishBlob(); err != nil {
			return fmt.Errorf("finishBlob: %w", err)
		}
	}

	return nil
}

func (sm *streamManager) finishBlob() error {
	if sm.bdecWriter == nil {
		return nil
	}

	if err := sm.bdecWriter.close(); err != nil {
		return fmt.Errorf("bdecWriter close: %w", err)
	}
	sm.blobStats[sm.lastBinding] = append(sm.blobStats[sm.lastBinding], sm.bdecWriter.blobStats)
	sm.bdecWriter = nil

	return nil
}

func (sm *streamManager) startNewBlob(ctx context.Context, binding int) error {
	if err := sm.maybeInitializeBucket(ctx); err != nil {
		return fmt.Errorf("initializing bucket: %w", err)
	}

	fName := sm.getNextFileName(time.Now(), fmt.Sprintf("%s_%d", sm.prefix, sm.deploymentId))
	w := sm.bucket.NewWriter(ctx, path.Join(sm.bucketPath, string(fName)), sm.objMetadata())

	ts := sm.tableStreams[binding]
	bdecWriter, err := newBdecWriter(w, ts.mappedColumns, ts.channel.TableColumns, ts.channel.EncryptionKey, fName)
	if err != nil {
		return fmt.Errorf("newBdecWriter: %w", err)
	}
	sm.bdecWriter = bdecWriter

	return nil
}

func (sm *streamManager) objMetadata() blob.WriterOption {
	return blob.WithObjectMetadata(map[string]string{
		"ingestclientname": fmt.Sprintf("%s_EstuaryFlow", sm.tenant),
		"ingestclientkey":  sm.prefix,
	})
}

func (sm *streamManager) flush(baseToken string) (map[int][]*blobMetadata, error) {
	if sm.bdecWriter != nil {
		if err := sm.finishBlob(); err != nil {
			return nil, fmt.Errorf("finishBlob: %w", err)
		}
	}

	out := make(map[int][]*blobMetadata)
	for binding, trackedBlobs := range sm.blobStats {
		for idx, trackedBlob := range trackedBlobs {
			out[binding] = append(out[binding], generateBlobMetadata(
				trackedBlob,
				sm.tableStreams[binding].channel,
				blobToken(baseToken, idx)),
			)
		}
	}
	maps.Clear(sm.blobStats)

	return out, nil
}

// write registers a series of blobs to the table. All the blobs must be for the
// same table.
//
// Exactly-once semantics are achieved using the offset tokens for the blobs:
// The offset token consists of the "base" token which is a string, and for each
// blob it is appended with a sequential counter, starting at 0. So if there are
// multiple blobs to append, they are organized with offset tokens like
// "basetoken:0", "basetoken:1", "basetoken2:" etc.
//
// Blobs are registered in this order, and it's possible that only some of the
// blobs get registered in a single Acknowledge before the connector fails for
// some reason, or all of the blobs get registered but the Acknowledge response
// is not persisted to the runtime before the connector restarts. The Snowpipe
// channel itself persists the last registered token, and this allows us to
// filter out blobs that had previously been registered and don't need
// registered again on a re-attempt of an Acknowledge.
func (sm *streamManager) write(ctx context.Context, blobs []*blobMetadata) error {
	if err := validWriteBlobs(blobs); err != nil {
		return fmt.Errorf("validWriteBlobs: %w", err)
	}

	var schema = blobs[0].Chunks[0].Schema
	var table = blobs[0].Chunks[0].Table
	var channelName = blobs[0].Chunks[0].Channels[0].Channel
	var thisChannel *channel
	for _, v := range sm.tableStreams {
		matches := v.channel.Schema == schema && v.channel.Table == table && v.channel.Channel == channelName
		if matches && thisChannel != nil {
			return fmt.Errorf("internal error: found duplicate duplicate channel %s in tableStreams", channelName)
		} else if matches {
			thisChannel = v.channel
		}
	}
	if thisChannel == nil {
		return fmt.Errorf("unknown channel %s", channelName)
	}

	for _, blob := range blobs {
		blobToken := blob.Chunks[0].Channels[0].OffsetToken
		currentChannelToken := thisChannel.OffsetToken
		if shouldWrite, err := shouldWriteNextToken(blobToken, currentChannelToken); err != nil {
			return fmt.Errorf("shouldWriteNextToken: %w", err)
		} else if !shouldWrite {
			continue
		}

		blob.Chunks[0].Channels[0].ClientSequencer = thisChannel.ClientSequencer
		blob.Chunks[0].Channels[0].RowSequencer = thisChannel.RowSequencer + 1

		if err := sm.c.write(ctx, blob); err != nil {
			var apiError *streamingApiError
			if errors.As(err, &apiError) && apiError.Code == 38 {
				// The "blob has wrong format or extension" error occurs if the
				// blob was written not-so-recently; apparently anything older
				// than an hour or so is rejected by what seems to be a
				// server-side check the examines the name of the file, which
				// contains the timestamp it was written.
				//
				// In these cases, which may arise from re-enabling a disabled
				// binding / materialization, or extended outages, we have to
				// download the file and re-upload it with an up-to-date name.
				//
				// This should be quite rare, even more rare than one may think,
				// since blob registration tokens are persisted in Snowflake
				// rather than exclusively managed by our Acknowledge
				// checkpoints. But it is still technically possible and so it
				// is handled with this.
				if err := sm.maybeInitializeBucket(ctx); err != nil {
					return fmt.Errorf("initializing bucket to rename: %w", err)
				}
				nextName := sm.getNextFileName(time.Now(), fmt.Sprintf("%s_%d", sm.prefix, sm.deploymentId))

				ll := log.WithFields(log.Fields{
					"oldName": blob.Path,
					"newName": nextName,
					"token":   blobToken,
				})
				ll.Info("attempting to register blob with malformed name by renaming")

				if err := sm.renameBlob(ctx, blob, thisChannel.EncryptionKey, nextName); err != nil {
					return fmt.Errorf("renameBlob: %w", err)
				}

				if err := sm.c.write(ctx, blob); err != nil {
					return fmt.Errorf("failed to write renamed blob: %w", err)
				}
				ll.Info("successfully registered renamed blob")
			} else {
				return fmt.Errorf("write: %w", err)
			}
		}

		thisChannel.RowSequencer++
		thisChannel.OffsetToken = &blobToken
	}

	// We don't need to wait for each individual token to be persisted, but need
	// to wait to the final one to be persisted for our idempotency strategy to
	// work.
	if err := sm.c.waitForTokenPersisted(
		ctx,
		*thisChannel.OffsetToken,
		thisChannel.ClientSequencer,
		blobs[0].Chunks[0].Schema,
		blobs[0].Chunks[0].Table,
		channelName,
	); err != nil {
		return fmt.Errorf("waitForTokenPersisted: %w", err)
	}

	return nil
}

func (sm *streamManager) renameBlob(ctx context.Context, blob *blobMetadata, encryptionKey string, newName blobFileName) error {
	r, err := sm.bucket.NewReader(ctx, path.Join(sm.bucketPath, blob.Path))
	if err != nil {
		return fmt.Errorf("NewReader: %w", err)
	}
	w := sm.bucket.NewWriter(ctx, path.Join(sm.bucketPath, string(newName)), sm.objMetadata())

	if err := reencrypt(r, w, blob, encryptionKey, newName); err != nil {
		return fmt.Errorf("reencrypt: %w", err)
	} else if err := r.Close(); err != nil {
		return fmt.Errorf("closing r: %w", err)
	} else if err := w.Close(); err != nil {
		return fmt.Errorf("closing w: %w", err)
	}

	return nil
}

// maybeInitializeBucket retrieves blob storage parameters and initializes the
// appropriate blob storage bucket. A basic expiry mechanism is used to prevent
// this from being re-done too frequently.
func (sm *streamManager) maybeInitializeBucket(ctx context.Context) error {
	if time.Now().Before(sm.bucketExpiresAt) {
		return nil
	}

	cfg, err := sm.c.configure(ctx)
	if err != nil {
		return fmt.Errorf("configuring channel: %w", err)
	}

	parts := strings.Split(cfg.StageLocation.Location, "/")
	bucket := parts[0]
	sm.bucketPath = path.Join(parts[1:]...)
	sm.prefix = cfg.Prefix
	sm.deploymentId = cfg.DeploymentID

	switch cfg.StageLocation.LocationType {
	case "S3":
		provider := credentials.NewStaticCredentialsProvider(
			cfg.StageLocation.Creds.AwsKeyId,
			cfg.StageLocation.Creds.AwsSecretKey,
			cfg.StageLocation.Creds.AwsToken,
		)
		if sm.bucket, err = blob.NewS3Bucket(ctx, bucket, provider); err != nil {
			return fmt.Errorf("new s3 blob bucket: %w", err)
		}
	case "GCS":
		opts := []option.ClientOption{
			option.WithTokenSource(oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: cfg.StageLocation.Creds.GcsAccessToken,
			})),
		}
		if sm.bucket, err = blob.NewGCSBucket(ctx, bucket, opts); err != nil {
			return fmt.Errorf("new gcs blob bucket: %w", err)
		}
	case "AZURE":
		opts := []blob.AzureConfigOption{
			blob.WithAzureSasToken(cfg.StageLocation.Creds.AzureSasToken),
			blob.WithAzureEndpoint(cfg.StageLocation.Endpoint),
		}
		if sm.bucket, err = blob.NewAzureBlobBucket(ctx, bucket, cfg.StageLocation.StorageAccount, opts); err != nil {
			return fmt.Errorf("new azure blob bucket: %w", err)
		}
	default:
		return fmt.Errorf("unknown stage location type %q", cfg.StageLocation.LocationType)
	}

	sm.bucketExpiresAt = time.Now().Add(30 * time.Minute)
	log.WithFields(log.Fields{
		"locationType": cfg.StageLocation.LocationType,
		"location":     cfg.StageLocation.Location,
		"prefix":       cfg.Prefix,
		"deploymentId": cfg.DeploymentID,
	}).Info("configured bucket for Snowpipe streaming")

	return nil
}

// blobFileName is the file name for a blob, which is part of the file key. It
// is also used as the "diversifier" for encryption. It's just a string, but
// this custom type helps keeps its usage comprehensible in both of these
// capacities.
type blobFileName string

// Gets the next file name, with "next" being relative to the tracked counter.
// The threadID is for the Java thread, so any random int value should work.
//
// The names of these files need to be globally unique and the timestamp parts
// have only second resolution. The client prefix includes a random nonce from
// the stream configure response that appears to change every time, so that
// should be sufficient.
//
// This code is written kind of weirdly so that it matches the Java SDK as
// closely as possible, since the filenames must be constructed in exactly the
// same way.
//
// Ref:
// https://github.com/snowflakedb/snowflake-ingest-java/blob/3cbaebfe26f59dc3a8b8e973649e3f1a1014438c/src/main/java/net/snowflake/ingest/streaming/internal/InternalStageManager.java#L161-L206
func (sm *streamManager) getNextFileName(calendar time.Time, clientPrefix string) blobFileName {
	calendar = calendar.UTC()
	year := strconv.Itoa(calendar.Year())
	month := strconv.Itoa(int(calendar.Month()))
	day := strconv.Itoa(calendar.Day())
	hour := strconv.Itoa(calendar.Hour())
	minute := strconv.Itoa(calendar.Minute())
	timestamp := calendar.Unix()
	blobShortName := strconv.FormatInt(timestamp, 36) +
		"_" +
		clientPrefix +
		"_" +
		strconv.Itoa(int(sm.keyBegin)) +
		"_" +
		strconv.Itoa(sm.getAndIncrementCounter()) +
		"." +
		BLOB_EXTENSION_TYPE

	return blobFileName(year + "/" + month + "/" + day + "/" + hour + "/" + minute + "/" + blobShortName)
}

func (sm *streamManager) getAndIncrementCounter() int {
	out := sm.counter
	sm.counter++
	return out
}

// blobToken encodes `baseToken`, which is per-transaction, and the counter `n`
// which is based on the number of blobs written for the specific binding within
// the transaction. In typical streaming cases there will only be a single blob
// in a transaction, but larger backfill scenarios may write out more than one
// blob since there is a limit on how large a single blob can be.
func blobToken(baseToken string, n int) string {
	return fmt.Sprintf("%s:%d", baseToken, n)
}

// shouldWriteNextToken determines if a blob with the `next` token should be
// written or not, based on the `current` persisted token. Generally this means
// if the `n` value for `next` is exactly one larger than `current` (or there is
// no `current`), the blob should be written.
func shouldWriteNextToken(next string, current *string) (bool, error) {
	if current == nil {
		// Maybe this should be more strict and error out unless `next` is the
		// first one in the sequence, but that would block cases where a user
		// has manually dropped a table for some reason.
		return true, nil
	}

	currentToken := *current
	nextBase, nextN, err := splitToken(next)
	if err != nil {
		return false, err
	}
	currentBase, currentN, err := splitToken(currentToken)
	if err != nil {
		return false, err
	}

	if nextBase != currentBase && nextN != 0 {
		return false, fmt.Errorf("expected blob token %s to start a new sequence (current: %s)", next, currentToken)
	} else if nextBase == currentBase && nextN <= currentN {
		return false, nil
	} else if nextBase == currentBase && nextN != currentN+1 {
		return false, fmt.Errorf("expected blob token %s to be written immediately after %s", next, currentToken)
	}

	return true, nil
}

func splitToken(token string) (string, int, error) {
	idx := strings.Index(token, ":")
	if idx == -1 {
		return "", 0, fmt.Errorf("invalid token %q: no ':' found", token)
	} else if idx == 0 {
		return "", 0, fmt.Errorf("invalid token %q: no base token found", token)
	} else if idx == len(token)-1 {
		return "", 0, fmt.Errorf("invalid token %q: no number found", token)
	}

	baseToken := token[:idx]
	nStr := token[idx+1:]
	n, err := strconv.Atoi(nStr)
	if err != nil {
		return "", 0, err
	}

	return baseToken, n, nil
}

// validWriteBlobs does some sanity checking the a series of blobs is valid to
// write per our invariants. Theoretically this shouldn't be needed, but is a
// nice guard against some hypothetical bugs which would otherwise be more
// difficult to troubleshoot.
func validWriteBlobs(blobs []*blobMetadata) error {
	var baseToken, channelName, schema, table, database string
	var n int
	for _, blob := range blobs {
		if l := len(blob.Chunks); l != 1 {
			return fmt.Errorf("internal error: expected chunks to have length 1 but was %d", l)
		} else if l := len(blob.Chunks[0].Channels); l != 1 {
			return fmt.Errorf("internal error: expected chunk channel to have length 1 but was %d", l)
		}

		persistToken := blob.Chunks[0].Channels[0].OffsetToken
		token, thisN, err := splitToken(persistToken)
		if err != nil {
			return fmt.Errorf("invalid token %q: %w", persistToken, err)
		}

		if baseToken == "" {
			baseToken = token
			channelName = blob.Chunks[0].Channels[0].Channel
			schema = blob.Chunks[0].Schema
			table = blob.Chunks[0].Table
			database = blob.Chunks[0].Database
			n = thisN
			continue
		}

		if baseToken != token {
			return fmt.Errorf("expected all blobs to have the same base token %q but got %q", baseToken, token)
		} else if channelName != blob.Chunks[0].Channels[0].Channel {
			return fmt.Errorf("expected all blobs to have the same channel %q but got %q", channelName, blob.Chunks[0].Channels[0].Channel)
		} else if schema != blob.Chunks[0].Schema {
			return fmt.Errorf("expected all blobs to have the same schema %q but got %q", schema, blob.Chunks[0].Schema)
		} else if table != blob.Chunks[0].Table {
			return fmt.Errorf("expected all blobs to have the same table %q but got %q", table, blob.Chunks[0].Table)
		} else if database != blob.Chunks[0].Database {
			return fmt.Errorf("expected all blobs to have the same database %q but got %q", database, blob.Chunks[0].Database)
		} else if n+1 != thisN {
			return fmt.Errorf("expected blob tokens to be in ascending order but got %d vs %d", thisN, n)
		}

		n = thisN
	}

	return nil
}

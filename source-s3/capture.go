package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	//"strings"
	//"time"

	//"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	//"github.com/estuary/connectors/go-types/airbyte"
	"github.com/estuary/connectors/go-types/parser"
	log "github.com/sirupsen/logrus"
)

var StreamCompleted = errors.New("stream completed")

type readResult struct {
	err         error
	bucket      string
	streamID    string
	relativeKey string
	state       *objectState
	record      json.RawMessage
}

func NewStream(config *Config, client *s3.S3, streamID string, state streamState) (*stream, error) {
	var bucket, prefix = parseStreamName(streamID)
	var matchKey *regexp.Regexp
	var err error
	if config.MatchKeys != "" {
		matchKey, err = regexp.Compile(config.MatchKeys)
		if err != nil {
			return nil, fmt.Errorf("invalid regex for matchKey: %w", err)
		}
	}
	return &stream{
		streamID:  streamID,
		bucket:    bucket,
		prefix:    prefix,
		state:     state,
		client:    client,
		config:    config,
		matchKeys: matchKey,
	}, nil
}

func (c *stream) Start(ctx context.Context, onResult func(readResult) error) {
	var err = c.captureStreamInternal(ctx, onResult)
	// Send a StreamCompleted to indicate that we've completed successfully
	if err == nil {
		err = StreamCompleted
	}
	onResult(readResult{err: err})
}

type stream struct {
	streamID  string
	bucket    string
	prefix    string
	state     streamState
	client    *s3.S3
	config    *Config
	matchKeys *regexp.Regexp
}

func (c *stream) captureStreamInternal(ctx context.Context, onResult func(readResult) error) error {
	log.Debug("Starting cature")
	var listResults = listAllObjects(ctx, c.bucket, c.prefix, c.client)
	var objectCount = 0
	for result := range listResults {
		objectCount++
		if result.err != nil {
			return fmt.Errorf("listing objects: %w", result.err)
		}
		// Should we skip importing this item because it doesn't match the configured regex?
		if c.matchKeys != nil && !c.matchKeys.MatchString(result.object.relativeKey) {
			continue
		}
		if err := c.importObject(ctx, result.object, onResult); err != nil {
			return fmt.Errorf("failed to import object: '%s': %w", result.object.relativeKey, err)
		}
	}
	log.WithField("observedObjects", objectCount).Info("completed processing stream")
	return nil
}

func (c *stream) importObject(ctx context.Context, obj *s3Object, onResult func(readResult) error) error {
	var fullKey = obj.fullKey()

	// Does this key hash into our shard's assigned range?
	// We hash the bucket and the full key just to be on the safe side.
	var hashKey = obj.bucket + "/" + fullKey
	if !c.config.ShardRange.IncludesHwHash([]byte(hashKey)) {
		return nil
	}

	var skipRecords uint64
	var prevState = c.state[obj.relativeKey]
	// Does our state indiate that we've already imported this object?
	if prevState != nil && prevState.ETag == obj.etag {
		if prevState.Complete {
			return nil
		} else {
			skipRecords = prevState.RecordCount
		}
	}

	var getInput = s3.GetObjectInput{
		Bucket: &obj.bucket,
		Key:    &fullKey,
	}
	var resp, err = c.client.GetObject(&getInput)
	if err != nil {
		return fmt.Errorf("getObject failed: %w", err)
	}
	defer resp.Body.Close()

	var parseConfig = c.makeParseConfig(obj.relativeKey, resp.ContentType, resp.ContentEncoding)
	configFile, err := ioutil.TempFile("", "parser-config-*.json")
	if err != nil {
		return fmt.Errorf("creating parser config temp file: %w", err)
	}
	defer os.Remove(configFile.Name())
	parseConfig.WriteToFile(configFile)

	var docCount uint64 = 0
	err = parser.ParseStream(ctx, configFile.Name(), resp.Body, func(data json.RawMessage) error {
		docCount++
		// Do we need to skip this record because it's already been ingested, as indicated by the
		// offset in the state?
		if docCount > skipRecords {
			return onResult(readResult{
				bucket:      c.bucket,
				streamID:    c.streamID,
				relativeKey: obj.relativeKey,
				record:      data,
				state: &objectState{
					ETag:         *resp.ETag,
					LastModified: *resp.LastModified,
					RecordCount:  docCount,
					Complete:     false,
				},
			})
		}
		return nil
	})
	if err != nil {
		return err
	}

	return onResult(readResult{
		bucket:      c.bucket,
		streamID:    c.streamID,
		relativeKey: obj.relativeKey,
		state: &objectState{
			ETag:         *resp.ETag,
			LastModified: *resp.LastModified,
			RecordCount:  docCount,
			Complete:     true,
		},
	})
}

func (c *stream) makeParseConfig(relativeKey string, contentType *string, contentEncoding *string) *parser.Config {
	var parseConfig = parser.Config{}
	if c.config.Parser != nil {
		parseConfig = c.config.Parser.Copy()
	}

	parseConfig.Filename = relativeKey
	if contentType != nil {
		parseConfig.ContentType = *contentType
	}
	if contentEncoding != nil {
		parseConfig.ContentEncoding = *contentEncoding
	}
	// If the user supplied a location for this, then we'll use that. Otherwise, use the default
	if parseConfig.AddRecordOffset == "" {
		parseConfig.AddRecordOffset = defaultRecordOffsetLocation
	}
	if parseConfig.AddValues == nil {
		parseConfig.AddValues = make(map[parser.JsonPointer]interface{})
	}
	parseConfig.AddValues[sourceFilenameLocation] = relativeKey
	return &parseConfig
}

const sourceFilenameLocation = "/_meta/sourceFile"
const defaultRecordOffsetLocation = "/_meta/recordOffset"

func shouldImport(obj *s3Object, matchKey *regexp.Regexp, state streamState) bool {
	if matchKey != nil && !matchKey.MatchString(obj.relativeKey) {
		return false
	}

	// Have we started importing this file before
	if s, ok := state[obj.relativeKey]; ok {
		if s.ETag == obj.etag {
			return !s.Complete
		}
	}
	return true
}

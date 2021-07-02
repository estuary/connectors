package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/estuary/connectors/go-types/airbyte"
	"github.com/estuary/connectors/go-types/parser"
	"github.com/estuary/connectors/go-types/shardrange"
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

func NewStream(config *Config, client *s3.S3, configuredStream airbyte.ConfiguredStream, state streamState, shardRange shardrange.Range) (*stream, error) {
	var bucket, prefix = parseStreamName(configuredStream.Stream.Name)
	var matchKey *regexp.Regexp
	var err error
	if config.MatchKeys != "" {
		matchKey, err = regexp.Compile(config.MatchKeys)
		if err != nil {
			return nil, fmt.Errorf("invalid regex for matchKey: %w", err)
		}
	}

	var projections = make(map[string]parser.JsonPointer)
	for k, v := range configuredStream.Projections {
		projections[k] = parser.JsonPointer(v)
	}

	return &stream{
		streamID:         configuredStream.Stream.Name,
		bucket:           bucket,
		prefix:           prefix,
		state:            state,
		client:           client,
		config:           config,
		matchKeys:        matchKey,
		collectionSchema: configuredStream.Stream.JSONSchema,
		projections:      projections,
		shardRange:       shardRange,
	}, nil
}

func (s *stream) Start(ctx context.Context, onResult func(readResult) error) {
	var err = s.captureStreamInternal(ctx, onResult)
	// Send a StreamCompleted to indicate that we've completed successfully
	if err == nil {
		err = StreamCompleted
	}
	onResult(readResult{err: err})
}

type stream struct {
	streamID         string
	bucket           string
	prefix           string
	state            streamState
	client           *s3.S3
	config           *Config
	matchKeys        *regexp.Regexp
	collectionSchema json.RawMessage
	projections      map[string]parser.JsonPointer
	shardRange       shardrange.Range
}

func (s *stream) captureStreamInternal(ctx context.Context, onResult func(readResult) error) error {
	log.Debug("Starting cature")
	var listResults = listAllObjects(ctx, s.bucket, s.prefix, s.client)
	var objectCount = 0
	var importedCount = 0
	for result := range listResults {
		objectCount++
		if result.err != nil {
			return fmt.Errorf("listing objects: %w", result.err)
		}
		if yes, skipRecords := s.shouldImport(result.object); yes {
			if err := s.importObject(ctx, result.object, skipRecords, onResult); err != nil {
				return fmt.Errorf("failed to import object: '%s': %w", result.object.relativeKey, err)
			}
			importedCount++
		}
	}
	log.WithFields(log.Fields{
		"observedObjects": objectCount,
		"importedObjects": importedCount,
	}).Info("completed processing stream")
	return nil
}

func (c *stream) shouldImport(obj *s3Object) (bool, uint64) {
	// Should we skip importing this item because it doesn't match the configured regex?
	if c.matchKeys != nil && !c.matchKeys.MatchString(obj.relativeKey) {
		return false, 0
	}
	// Does this key hash into our shard's assigned range?
	// We hash the bucket and the full key just to be on the safe side.
	var hashKey = obj.bucket + "/" + obj.fullKey()
	if !c.shardRange.IncludesHwHash([]byte(hashKey)) {
		return false, 0
	}

	var prevState = c.state[obj.relativeKey]
	// Does our state indiate that we've already imported this object?
	if prevState != nil && prevState.ETag == obj.etag {
		if prevState.Complete {
			return false, 0
		} else {
			return true, prevState.RecordCount
		}
	}
	return true, 0
}

func (s *stream) importObject(ctx context.Context, obj *s3Object, skipRecords uint64, onResult func(readResult) error) error {
	var fullKey = obj.fullKey()

	var getInput = s3.GetObjectInput{
		Bucket: &obj.bucket,
		Key:    &fullKey,
	}
	var resp, err = s.client.GetObject(&getInput)
	if err != nil {
		return fmt.Errorf("getObject failed: %w", err)
	}
	defer resp.Body.Close()

	var parseConfig = s.makeParseConfig(obj.relativeKey, resp.ContentType, resp.ContentEncoding)
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
				bucket:      s.bucket,
				streamID:    s.streamID,
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
		bucket:      s.bucket,
		streamID:    s.streamID,
		relativeKey: obj.relativeKey,
		state: &objectState{
			ETag:         *resp.ETag,
			LastModified: *resp.LastModified,
			RecordCount:  docCount,
			Complete:     true,
		},
	})
}

func (s *stream) makeParseConfig(relativeKey string, contentType *string, contentEncoding *string) *parser.Config {
	var parseConfig = parser.Config{}
	if s.config.Parser != nil {
		parseConfig = s.config.Parser.Copy()
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
	if parseConfig.Schema == nil {
		parseConfig.Schema = s.collectionSchema
	}
	parseConfig.Projections = s.projections
	parseConfig.AddValues[sourceFilenameLocation] = relativeKey
	return &parseConfig
}

const sourceFilenameLocation = "/_meta/source/file"
const defaultRecordOffsetLocation = "/_meta/source/record"

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

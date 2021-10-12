package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
)

// ExternalDataConnectionFile is a Google Cloud Storage file in a format suitable for using from
// BiqQuery as an ExternalDataConnection.
type ExternalDataConnectionFile struct {
	URI         string
	edc         *bigquery.ExternalDataConfig
	gcsObject   *storage.ObjectHandle
	gcsWriter   *storage.Writer
	bufWriter   *bufio.Writer
	jsonEncoder *json.Encoder
}

// NewExternalDataConnectionFile returns an ExternalDataConnectionFile configured and ready for writing rows.
func (ep *Endpoint) NewExternalDataConnectionFile(ctx context.Context, file string, edc *bigquery.ExternalDataConfig) (*ExternalDataConnectionFile, error) {

	if edc.SourceFormat != bigquery.JSON {
		return nil, fmt.Errorf("external data connection file only supports json at this time")
	}

	filePath := path.Join(ep.config.BucketPath, file)

	f := &ExternalDataConnectionFile{
		URI:       fmt.Sprintf("gs://%s/%s", ep.config.Bucket, filePath),
		edc:       edc,
		gcsObject: ep.cloudStorageClient.Bucket(ep.config.Bucket).Object(filePath),
	}

	// Make sure this ExternalDataConfig has no configured file already.
	// This ensures we are following the proper lifecycle which means the last file was not closed.
	// We want to re-use the ExternalDataConfig. This is an extra check to ensure that two processes
	// are not using the same ExternalDataConfig.
	if len(edc.SourceURIs) != 0 {
		return nil, fmt.Errorf("external data config already has configured source uri: %v", edc.SourceURIs)
	}

	edc.SourceURIs = []string{f.URI}

	f.gcsWriter = f.gcsObject.NewWriter(ctx)
	f.bufWriter = bufio.NewWriter(f.gcsWriter)

	f.jsonEncoder = json.NewEncoder(f.bufWriter)
	f.jsonEncoder.SetEscapeHTML(false)
	f.jsonEncoder.SetIndent("", "")

	return f, nil

}

// WriteRow takes either a slice of interface{} witch a count that matches the *bigquery.ExternalDataConfig
// this file was opened with a map[string]interface{} with just the columns that need to be set. (it must
// also have the same fields as *bigquery.ExternalDataConfig).
func (f *ExternalDataConnectionFile) WriteRow(rowi interface{}) error {

	var v map[string]interface{}

	switch row := rowi.(type) {
	case []interface{}:
		// Sanity check our column count
		if len(f.edc.Schema) != len(row) {
			return errors.New("schema column / value count mismatch")
		}

		// Build the json object and encode it into the file
		v = make(map[string]interface{})
		for i, col := range f.edc.Schema {
			v[col.Name] = row[i]
		}

	case map[string]interface{}:
		v = row

	default:
		return fmt.Errorf("can not write %T", rowi)

	}

	return f.jsonEncoder.Encode(v)

}

// Close flushes the buffer and closes the file.
func (f *ExternalDataConnectionFile) Close() error {
	if err := f.bufWriter.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return f.gcsWriter.Close()
}

// Delete removes the file.
func (f *ExternalDataConnectionFile) Delete(ctx context.Context) error {
	f.edc.SourceURIs = nil // Clear the SourceURIs so the next process can use it.
	return f.gcsObject.Delete(ctx)
}

// tmpFileName generates unique file names.
func tmpFileName() string {
	tempUUID, err := uuid.NewUUID()
	if err != nil {
		panic(err)
	}
	return tempUUID.String()
}

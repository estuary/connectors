package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/xitongsys/parquet-go-source/local"

	// TODO revisit this after https://issues.apache.org/jira/browse/ARROW-13986 is completed.
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

// FileProcessor defines an interface to materialize data into files.
type FileProcessor interface {
	// Stores data  to a local file dedicated to the specified binding.
	Store(binding int, values tuple.Tuple) error

	// Uploads local files to the cloud storage for all bindings, and returns the updated nextSeqNumList upon success.
	Commit() (nextSeqNumList []int, e error)

	// Get cloud storage prefix
	GetCloudPrefix(binding int) (prefix string)

	// Deletes files from cloud storage
	Delete(prefix string) error

	// Releases resources occupied by the processor.
	Destroy() error
}

// ParquetFileProcessor implements the FileProcessor interface.
type ParquetFileProcessor struct {
	// each pqBinding materializes the data in a binding (resource) into parquet files.
	pqBindings []*pqBinding
	s3Operator CloudOperator
	tmpDir     string
}

// NewParquetFileProcessor initializes an object of ParquetFileProcessor.
func NewParquetFileProcessor(
	ctx context.Context,
	s3Operator CloudOperator,
	nextSeqNumList []int,
	bindings []*binding,
	pathPrefix string) (*ParquetFileProcessor, error) {

	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, fmt.Errorf("creating temp dir: %w", err)
	}

	var pqBindings = make([]*pqBinding, 0, len(bindings))
	for i, binding := range bindings {
		generatedPrefix := fmt.Sprintf("%s/%s", strings.TrimSuffix(strings.TrimPrefix(pathPrefix, "/"), "/"), uuid.New().String())
		var s3PathPrefix = fmt.Sprintf("%s/", generatedPrefix)
		var localPathPrefix = fmt.Sprintf("%s/%s_", tmpDir, strings.Replace(generatedPrefix, "/", "_", -1))

		pqDataConverter, err := NewParquetDataConverter(binding.materializationSpec)
		if err != nil {
			return nil, fmt.Errorf("creating parquet data converter: %w", err)
		}

		var nextSeqNum = 0
		if nextSeqNumList != nil && len(nextSeqNumList) > i {
			nextSeqNum = nextSeqNumList[i]
		}

		pqBindings = append(pqBindings, &pqBinding{
			ctx:              ctx,
			s3Operator:       s3Operator,
			s3PathPrefix:     s3PathPrefix,
			pqDataConverter:  pqDataConverter,
			pqWriter:         nil,
			compressionCodec: parquet.CompressionCodec_SNAPPY,
			localFile:        nil,
			localPathPrefix:  localPathPrefix,
			nextSeqNum:       nextSeqNum,
		})
	}

	return &ParquetFileProcessor{
		pqBindings: pqBindings,
		s3Operator: s3Operator,
		tmpDir:     tmpDir,
	}, nil
}

// Store implements the FileProcessor interface.
func (pfp *ParquetFileProcessor) Store(binding int, values tuple.Tuple) error {
	if err := pfp.pqBindings[binding].Store(values); err != nil {
		return fmt.Errorf("storing data: %w", err)
	}
	return nil
}

// Commit implements the FileProcessor interface.
func (pfp *ParquetFileProcessor) Commit() (nextSeqNumList []int, err error) {
	for _, binding := range pfp.pqBindings {
		if err := binding.Commit(); err != nil {
			return nil, err
		}
	}
	return pfp.nextSeqNumList(), nil
}

func (pfp *ParquetFileProcessor) GetCloudPrefix(binding int) (prefix string) {
	return pfp.pqBindings[binding].s3PathPrefix
}

func (pfp *ParquetFileProcessor) Delete(prefix string) error {
	return pfp.s3Operator.Delete(prefix)
}

// Destroy implements the FileProcessor interface.
func (pfp *ParquetFileProcessor) Destroy() error {
	return os.RemoveAll(pfp.tmpDir)
}

func (pfp *ParquetFileProcessor) nextSeqNumList() []int {
	nextSeqNumList := make([]int, 0, len(pfp.pqBindings))
	for _, b := range pfp.pqBindings {
		nextSeqNumList = append(nextSeqNumList, b.nextSeqNum)
	}
	return nextSeqNumList
}

// TODO(jixiang) better understand the impact of ParallelNumber to the performance of the connector.
const parquetWriterParallelNumber int64 = 4

const pqContentType string = "application/octet-stream"

// A pqBinding is responsible for converting, storing, and uploading the materialization results from
// a single binding of flow txns into the cloud.
type pqBinding struct {
	ctx context.Context
	// For uploading files to S3.
	s3Operator   CloudOperator
	s3PathPrefix string
	// For converting flow data of key/values into formats acceptable by the parquet file writer.
	pqDataConverter *ParquetDataConverter
	// For generating local parquet files.
	pqWriter           *writer.ParquetWriter
	compressionCodec   parquet.CompressionCodec
	localFile          source.ParquetFile
	localPathPrefix    string
	nextSeqNum         int
	uploadedFilesPaths []string
}

// Stores the input data into local files.
func (b *pqBinding) Store(values tuple.Tuple) error {
	pqData, err := b.pqDataConverter.Convert(values)

	if err != nil {
		return fmt.Errorf("converting data: %w", err)
	}

	if b.localFile == nil {
		if b.pqWriter != nil {
			return fmt.Errorf("pqWriting is expected not to be nil if localFile is nil")
		} else if b.localFile, err = local.NewLocalFileWriter(b.localFileName()); err != nil {
			return fmt.Errorf("creating local file: %w", err)
		} else if b.pqWriter, err = writer.NewParquetWriter(
			b.localFile,
			b.pqDataConverter.JSONFileSchema(),
			parquetWriterParallelNumber); err != nil {
			return fmt.Errorf("creating parquet writer: %w", err)
		} else {
			b.pqWriter.CompressionType = b.compressionCodec
		}
	}

	if err = b.pqWriter.Write(pqData); err != nil {
		return fmt.Errorf("writing to parquet: %w", err)
	}

	return nil
}

// Uploads local files to the cloud.
func (b *pqBinding) Commit() error {
	if b.pqWriter != nil {
		defer func() { b.pqWriter = nil }()
		if err := b.pqWriter.WriteStop(); err != nil {
			return fmt.Errorf("stopping writer: %w", err)
		}
	}

	if b.localFile != nil {
		defer func() { b.localFile = nil }()

		if err := b.localFile.Close(); err != nil {
			return fmt.Errorf("closing localFile: %w", err)
		}

		// TODO(whb): The Go AWS SDK version 2 handles retryable errors out of the box. At some point it
		// might make sense to update this connector to use the version 2 SDK and get rid of this retry
		// loop. For now we will use a reasonable maximum limit on the number of attempts to upload
		// before failing without trying to distinguish between retry-able errors and terminal errors.
		var err error
		maxRetryAttempts := 10
		for attempt, backoffInSec := 0, 1; attempt < maxRetryAttempts; attempt++ {
			// If upload failed, keep retrying until succeed or canceled.
			if err = b.upload(); err == nil {
				break
			} else {
				log.WithFields(log.Fields{
					"attempt": attempt,
					"err":     err,
				}).Warn(fmt.Sprintf("uploading file failed, Retrying (attempt %d of %d)...", attempt, maxRetryAttempts))
			}

			select {
			case <-b.ctx.Done():
				return b.ctx.Err()
			case <-time.After(time.Duration(backoffInSec) * time.Second):
				// Fallthrough intended.
			}

			if backoffInSec < 16 {
				backoffInSec *= 2
			}
		}
		if err != nil {
			return fmt.Errorf("retry attempts exhausted: %w", err)
		}

		if err := os.Remove(b.localFileName()); err != nil {
			return fmt.Errorf("removing local file: %w", err)

		}
		b.uploadedFilesPaths = append(b.uploadedFilesPaths, b.s3Path())
		b.nextSeqNum++
	}
	return nil
}

func (b *pqBinding) filename() string {
	// Pad the sequence number with 0's so that the lexicographical ordering of files will match the
	// sequence number ordering.
	return fmt.Sprintf("%09d.parquet", b.nextSeqNum)
}

func (b *pqBinding) localFileName() string {
	return b.localPathPrefix + b.filename()
}

func (b *pqBinding) s3Path() string {
	return b.s3PathPrefix + b.filename()
}

func (b *pqBinding) upload() error {
	return b.s3Operator.Upload(b.s3Path(), b.localFileName(), pqContentType)
}

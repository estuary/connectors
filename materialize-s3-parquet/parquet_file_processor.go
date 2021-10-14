package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	"github.com/xitongsys/parquet-go-source/local"

	"github.com/benbjohnson/clock"

	// TODO revisit this after https://issues.apache.org/jira/browse/ARROW-13986 is completed.
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

// FileProcessor defines an interface to materialize data into files.
type FileProcessor interface {
	// Stores data (key values) to a local file dedicated to the specified binding.
	Store(binding int, key tuple.Tuple, values tuple.Tuple) error

	// Uploads local files to the cloud storage for all bindings, and returns the updated nextSeqNumList upon success.
	// Refer to `struct driverCheckpoint` definition in driver.go for details of nextSeqNumList.
	Commit() (nextSeqNumList []int, e error)

	// Releases resources occupied by the processor.
	Destroy() error
}

// FileProcessorProxy implements FileProcessor interface.
// It proxies a file processor with the following additional logic:
// a) It maintains a ticker, which triggers the Commit() command on the proxied file processor
//      if no Commit call to the proxied file processor is observed over a given period of time.
//      It makes sure that no local file remains in local without being uploaded to the cloud.
// b) It ensures the mutual exclusive access to the proxied file processor between the ticker
//      and the transactor(via API calls to the proxy).
type FileProcessorProxy struct {
	ctx context.Context

	// The proxied file processor.
	fileProcessor FileProcessor

	// The time interval, during which if no commit is triggered from the transactor, the proxy triggers one.
	forceUploadInterval time.Duration

	// A mutex to control mutual exclusive access to the proxied fileProcessor,
	// and related states of hasLocalStagingData and mostRecentCommitTime
	mu sync.Mutex
	// The force upload is enabled only if there are calls to `Store` without a call to `Commit`.
	hasLocalStagingData bool
	// The most recent time a Commit call is made.
	mostRecentCommitTime time.Time

	// A channel to signal the termination of service.
	stopServingCh chan struct{}

	// A channel to deliver the last error occurred during the proxy service.
	errorCh chan error

	clock clock.Clock
}

// NewFileProcessorProxy initializes a FileProcessorProxy.
func NewFileProcessorProxy(
	ctx context.Context,
	fileProcessor FileProcessor,
	forceUploadInterval time.Duration,
	clock clock.Clock) *FileProcessorProxy {

	var fp = &FileProcessorProxy{
		ctx:                  ctx,
		fileProcessor:        fileProcessor,
		forceUploadInterval:  forceUploadInterval,
		hasLocalStagingData:  false,
		mostRecentCommitTime: clock.Now(),
		stopServingCh:        make(chan struct{}),
		errorCh:              make(chan error, 1),
		clock:                clock,
	}

	go fp.startServing()

	return fp
}

// Store implements the FileProcessor interface.
func (fp *FileProcessorProxy) Store(binding int, key tuple.Tuple, values tuple.Tuple) error {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	if err := fp.fileProcessor.Store(binding, key, values); err != nil {
		return fmt.Errorf("storing data: %w", err)
	}

	fp.hasLocalStagingData = true
	return nil
}

// Commit implements the FileProcessor interface.
func (fp *FileProcessorProxy) Commit() (nextSeqNumList []int, e error) {
	fp.mu.Lock()
	defer func() {
		fp.mostRecentCommitTime = fp.clock.Now()
		fp.mu.Unlock()
	}()

	if !fp.hasLocalStagingData {
		return
	}

	if nextSeqNumList, err := fp.fileProcessor.Commit(); err != nil {
		return nil, fmt.Errorf("committing data: %w", err)
	} else {
		fp.hasLocalStagingData = false
		return nextSeqNumList, nil
	}
}

// Destroy implements the FileProcessor interface.
func (fp *FileProcessorProxy) Destroy() error {
	close(fp.stopServingCh)
	return <-fp.errorCh
}

func (fp *FileProcessorProxy) startServing() {
	var ticker = fp.clock.Ticker(fp.forceUploadInterval)
	defer func() {
		ticker.Stop()
		fp.errorCh <- fp.fileProcessor.Destroy()
	}()

	for {
		select {
		case <-fp.stopServingCh:
			fp.forceCommit()
			return
		case <-fp.ctx.Done():
			fp.forceCommit()
			return
		case <-ticker.C:
			var now = fp.clock.Now()
			if now.Sub(fp.mostRecentCommitTime) >= fp.forceUploadInterval {
				// No Flow txn is received after forceUploadInterval expires. Force a Commit action to the cloud if needed.
				fp.forceCommit()
			}
		}
	}
}

func (fp *FileProcessorProxy) forceCommit() {
	if _, err := fp.Commit(); err != nil {
		// TODO(jixiang): Consider returning the error via errorCh?
		panic(fmt.Sprintf("failed to commit with error: %v", err))
	}
}

// ParquetFileProcessor implements the FileProcessor interface.
type ParquetFileProcessor struct {
	// each pqBinding materializes the data in a binding (resource) into parquet files.
	pqBindings []*pqBinding
	tmpDir     string
}

// NewParquetFileProcessor initializes an object of ParquetFileProcessor.
func NewParquetFileProcessor(
	ctx context.Context,
	S3Uploader Uploader,
	nextSeqNumList []int,
	open *pm.TransactionRequest_Open) (*ParquetFileProcessor, error) {

	tmpDir, err := ioutil.TempDir("", strings.Replace(string(open.Materialization.Materialization), "/", "_", -1))
	if err != nil {
		return nil, fmt.Errorf("creating temp dir: %w", err)
	}

	var pqBindings = make([]*pqBinding, 0, len(open.Materialization.Bindings))
	for i, binding := range open.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		var s3PathPrefix = fmt.Sprintf("%s/%d_%d", res.PathPrefix, open.KeyBegin, open.KeyEnd)
		var localPathPrefix = fmt.Sprintf("%s/%d_%d_%d", tmpDir, i, open.KeyBegin, open.KeyEnd)

		pqDataConverter, err := NewParquetDataConverter(binding)
		if err != nil {
			return nil, fmt.Errorf("creating parquet data converter: %w", err)
		}

		var nextSeqNum = 0
		if nextSeqNumList != nil {
			nextSeqNum = nextSeqNumList[i]
		}

		pqBindings = append(pqBindings, &pqBinding{
			ctx:              ctx,
			S3Uploader:       S3Uploader,
			s3PathPrefix:     s3PathPrefix,
			pqDataConverter:  pqDataConverter,
			pqWriter:         nil,
			compressionCodec: res.CompressionCodec(),
			localFile:        nil,
			localPathPrefix:  localPathPrefix,
			nextSeqNum:       nextSeqNum,
		})
	}

	return &ParquetFileProcessor{
		pqBindings: pqBindings,
		tmpDir:     tmpDir,
	}, nil
}

// Store implements the FileProcessor interface.
func (pfp *ParquetFileProcessor) Store(binding int, key tuple.Tuple, values tuple.Tuple) error {
	if err := pfp.pqBindings[binding].Store(key, values); err != nil {
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
	S3Uploader   Uploader
	s3PathPrefix string
	// For converting flow data of key/values into formats acceptable by the parquet file writer.
	pqDataConverter *ParquetDataConverter
	// For generating local parquet files.
	pqWriter         *writer.ParquetWriter
	compressionCodec parquet.CompressionCodec
	localFile        source.ParquetFile
	localPathPrefix  string
	nextSeqNum       int
}

// Stores the input data into local files.
func (b *pqBinding) Store(key tuple.Tuple, values tuple.Tuple) error {
	pqData, err := b.pqDataConverter.Convert(key, values)

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

		for attempt, backoffInSec := 0, 1; true; attempt++ {
			// If upload failed, keep retrying until succeed or canceled.
			if err := b.upload(); err == nil {
				break
			} else {
				log.WithFields(log.Fields{
					"attempt": attempt,
					"err":     err,
				}).Warn("uploading file failed, Retrying...")
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
		if err := os.Remove(b.localFileName()); err != nil {
			return fmt.Errorf("removing local file: %w", err)

		}
		b.nextSeqNum++
	}
	return nil
}

func (b *pqBinding) localFileName() string {
	return fmt.Sprintf("%s_%d.pq", b.localPathPrefix, b.nextSeqNum)
}

func (b *pqBinding) s3Path() string {
	return fmt.Sprintf("%s_%d.pq", b.s3PathPrefix, b.nextSeqNum)
}

func (b *pqBinding) upload() error {
	return b.S3Uploader.Upload(b.s3Path(), b.localFileName(), pqContentType)
}

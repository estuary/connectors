package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	"github.com/xitongsys/parquet-go-source/local"

	"github.com/benbjohnson/clock"
	// TODO revist this after https://issues.apache.org/jira/browse/ARROW-13986 is completed.

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
// a) It maintains a timer, which triggers the Commit() command on the proxied file processor
//      if no interaction with the proxied file processor is observed for a given period of time.
//      It makes sure that no local file remains in local without being uploaded to the cloud.
// b) It ensures the mutual exclusive access to the proxied file processor between the timer
//      and the transactor(via API calls to the proxy).
type FileProcessorProxy struct {
	ctx context.Context

	fileProcessor FileProcessor
	// A channel to manage the fileProcessor.
	fileProcessorCh chan FileProcessor

	// A channel to signal the termination of service.
	stopServingCh chan struct{}

	// A channel to deliver the last error occurred during the proxy service.
	errorCh chan error

	// The time interval, during which if no commit is triggered from the transactor, the proxy triggers one.
	forceUploadInterval time.Duration
	// The force upload is enabled only if there are calls to `Store` without a call to `Commit`.
	forceUploadEnabled bool
	timer              *clock.Timer
}

// NewFileProcessorProxy initializes a FileProcessorProxy.
func NewFileProcessorProxy(
	ctx context.Context,
	fileProcessor FileProcessor,
	forceUploadInterval time.Duration,
	clock clock.Clock) *FileProcessorProxy {

	// Expire immediately.
	var timer = clock.Timer(0)

	var fp = &FileProcessorProxy{
		ctx:                 ctx,
		fileProcessorCh:     make(chan FileProcessor),
		fileProcessor:       fileProcessor,
		stopServingCh:       make(chan struct{}),
		errorCh:             make(chan error, 1),
		forceUploadInterval: forceUploadInterval,
		forceUploadEnabled:  false,
		timer:               timer,
	}

	fp.stopTimer()
	go fp.startServing()

	return fp
}

// Store implements the FileProcessor interface.
func (fp *FileProcessorProxy) Store(binding int, key tuple.Tuple, values tuple.Tuple) error {
	var fileProcessor = fp.acquireFileProcessor()
	defer fp.returnFileProcessor(fileProcessor)

	if err := fileProcessor.Store(binding, key, values); err != nil {
		return fmt.Errorf("storing data: %w", err)
	}

	fp.forceUploadEnabled = true
	return nil
}

// Commit implements the FileProcessor interface.
func (fp *FileProcessorProxy) Commit() (nextSeqNumList []int, e error) {
	var fileProcessor = fp.acquireFileProcessor()
	defer fp.returnFileProcessor(fileProcessor)

	if nextSeqNumList, err := fileProcessor.Commit(); err != nil {
		return nil, fmt.Errorf("committing data: %w", err)
	} else {
		fp.forceUploadEnabled = false
		return nextSeqNumList, nil
	}
}

// Destroy implements the FileProcessor interface.
func (fp *FileProcessorProxy) Destroy() error {
	close(fp.stopServingCh)
	return <-fp.errorCh
}

func (fp *FileProcessorProxy) acquireFileProcessor() FileProcessor {
	if fp, ok := <-fp.fileProcessorCh; ok {
		return fp
	}
	panic("trying to acquire file processor after it is destroyed.")
}

func (fp *FileProcessorProxy) returnFileProcessor(fileProcessor FileProcessor) {
	if fileProcessor == nil {
		panic("returning a file processor that is not acquired.")
	}
	fp.fileProcessorCh <- fileProcessor
}

func (fp *FileProcessorProxy) stopTimer() {
	if !fp.timer.Stop() {
		<-fp.timer.C
	}
}

func (fp *FileProcessorProxy) startServing() {
	var err error = nil
	defer func() {
		fp.errorCh <- err
	}()
	defer func(f FileProcessor) {
		err = f.Destroy()
		close(fp.fileProcessorCh)
	}(fp.fileProcessor)

	for {
		fp.timer.Reset(fp.forceUploadInterval)
		select {
		case <-fp.stopServingCh:
			fp.stopTimer()
			fp.forceCommit()
			return
		case <-fp.ctx.Done():
			fp.stopTimer()
			fp.forceCommit()
			return
		case fp.fileProcessorCh <- fp.fileProcessor:
			fp.fileProcessor = nil
			fp.stopTimer()
			// Block until the file processor is returned.
			select {
			// For the first and second cases, some data might be missing in the cloud,
			// b/c the client code is destroying the proxy when writing to the file processor, which
			// prevents a force commit from the proxy.
			case <-fp.stopServingCh:
				return
			case <-fp.ctx.Done():
				return
			case fp.fileProcessor = <-fp.fileProcessorCh:
				// Fallthrough intended
			}
		case <-fp.timer.C:
			// No Flow txn is received after forceUploadInterval expires. Force a Commit action to the cloud if needed.
			fp.forceCommit()
		}
	}
}

func (fp *FileProcessorProxy) forceCommit() {
	if fp.forceUploadEnabled {
		defer func() { fp.forceUploadEnabled = false }()
		if _, err := fp.fileProcessor.Commit(); err != nil {
			// TODO(jixiang): Consider returning the error via errorCh?
			panic(fmt.Sprintf("failed to commit with error: %v", err))
		}
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
			bucket:           res.Bucket,
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

// A pqBinding is responsible for converting, storing, and uploading the materialization results from
// a single binding of flow txns into the cloud.
type pqBinding struct {
	ctx context.Context
	// For uploading files to S3.
	S3Uploader   Uploader
	bucket       string
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
	return b.S3Uploader.Upload(b.bucket, b.s3Path(), b.localFileName())
}

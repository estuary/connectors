package main

import (
	"context"
	"fmt"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// Structure of test data for MockS3Operator.
type TestData struct {
	Id       int64
	Message  string
	Document string
}

// MockS3Operator implements the Uploader interface.
type MockS3Operator struct {
	t        *testing.T
	contents map[string][]TestData
}

func (u *MockS3Operator) Upload(key, localFileName string, contentType string) error {
	fr, err := local.NewLocalFileReader(localFileName)
	require.NoError(u.t, err)
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, 4)
	require.NoError(u.t, err)
	defer pr.ReadStop()

	var num = int(pr.GetNumRows())
	var data = make([]TestData, num)
	err = pr.Read(&data)
	require.NoError(u.t, err)

	// Instead of uploading to the cloud, MockS3Operator records the uploaded contents in memory.
	u.contents[fmt.Sprintf("s3://test_bucket/%s", key)] = data

	return nil
}
func (u *MockS3Operator) Delete(prefix string) error {
	for key, _ := range u.contents {
		if strings.HasPrefix(key, fmt.Sprintf("s3://test_bucket/%s", prefix)) {
			delete(u.contents, key)
		}
	}
	return nil
}
func newMockS3Operator(t *testing.T) *MockS3Operator {
	return &MockS3Operator{t: t, contents: make(map[string][]TestData)}
}

func TestParquetFileProcessor_NoBinding(t *testing.T) {
	var mockS3Uploader = newMockS3Operator(t)
	var bindings = make([]*binding, 0, 0)

	fileProcessor, _ := NewParquetFileProcessor(context.Background(), mockS3Uploader, nil, bindings, "test_prefix")
	nextSeqNumList, err := fileProcessor.Commit()
	require.NoError(t, err)
	require.Equal(t, []int{}, nextSeqNumList)
}

func buildTestBindings(numOfBindings int) []*binding {
	var bindings = make([]*binding, 0, numOfBindings)
	for i := 0; i < numOfBindings; i++ {
		var materializationSpec = &pf.MaterializationSpec_Binding{
			FieldSelection: pf.FieldSelection{Keys: []string{"Id"}, Values: []string{"Message"}, Document: "Document"},
			Collection: pf.CollectionSpec{Projections: []pf.Projection{
				{Field: "Document", Inference: pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MUST}},
				{Field: "Id", Inference: pf.Inference{Types: []string{"integer"}, Exists: pf.Inference_MUST}},
				{Field: "Message", Inference: pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MUST}},
			}},
		}
		singleBinding := binding{materializationSpec: materializationSpec}
		bindings = append(bindings, &singleBinding)
	}
	return bindings
}

func TestParquetFileProcessor_SingleBinding(t *testing.T) {
	var mockS3Uploader = newMockS3Operator(t)
	var bindings = buildTestBindings(1)

	fileProcessor, _ := NewParquetFileProcessor(context.Background(), mockS3Uploader, nil, bindings, "test_prefix")
	cloudPrefix := fileProcessor.GetCloudPrefix(0)

	require.Equal(t, []int{0}, fileProcessor.nextSeqNumList())

	require.NoError(t, fileProcessor.Store(0, tuple.Tuple{1, "msg #1", "doc_string"}))
	require.NoError(t, fileProcessor.Store(0, tuple.Tuple{1, "msg #1", "doc_string"}))
	require.Equal(t, 0, len(mockS3Uploader.contents))

	nextSeqNumList, err := fileProcessor.Commit()
	require.NoError(t, err)
	var expectedA = []TestData{{Id: 1, Message: "msg #1", Document: "doc_string"}, {Id: 1, Message: "msg #1", Document: "doc_string"}}

	require.Equal(t, expectedA, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix+"000000000.parquet"])
	require.Equal(t, []int{1}, nextSeqNumList)

	require.NoError(t, fileProcessor.Store(0, tuple.Tuple{2, "msg #2", "doc_string2"}))
	nextSeqNumList, err = fileProcessor.Commit()
	require.NoError(t, err)
	var expectedB = []TestData{{Id: 2, Message: "msg #2", Document: "doc_string2"}}
	require.Equal(t, expectedA, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix+"000000000.parquet"])
	require.Equal(t, expectedB, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix+"000000001.parquet"])
	require.Equal(t, []int{2}, nextSeqNumList)

	// Delete files after usage
	require.NoError(t, fileProcessor.Delete(cloudPrefix))
	require.Equal(t, 0, len(mockS3Uploader.contents))
}

func TestParquetFileProcessor_MultipleBindings(t *testing.T) {
	var mockS3Uploader = newMockS3Operator(t)
	var bindings = buildTestBindings(4)

	fileProcessor, _ := NewParquetFileProcessor(context.Background(), mockS3Uploader, []int{1, 2, 3, 4}, bindings, "test_prefix")
	cloudPrefix0 := fileProcessor.GetCloudPrefix(0)
	cloudPrefix1 := fileProcessor.GetCloudPrefix(1)
	cloudPrefix2 := fileProcessor.GetCloudPrefix(2)
	cloudPrefix3 := fileProcessor.GetCloudPrefix(3)
	require.Equal(t, []int{1, 2, 3, 4}, fileProcessor.nextSeqNumList())

	require.NoError(t, fileProcessor.Store(0, tuple.Tuple{0, "msg #0", "doc_string0"}))
	require.NoError(t, fileProcessor.Store(1, tuple.Tuple{1, "msg #1", "doc_string1"}))
	require.Equal(t, 0, len(mockS3Uploader.contents))

	nextSeqNumList, err := fileProcessor.Commit()
	require.NoError(t, err)
	require.Equal(t, []int{2, 3, 3, 4}, nextSeqNumList)
	require.Equal(t, []TestData{{Id: 0, Message: "msg #0", Document: "doc_string0"}}, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix0+"000000001.parquet"])
	require.Equal(t, []TestData{{Id: 1, Message: "msg #1", Document: "doc_string1"}}, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix1+"000000002.parquet"])

	require.NoError(t, fileProcessor.Store(0, tuple.Tuple{2, "msg #2", "doc_string2"}))
	require.NoError(t, fileProcessor.Store(1, tuple.Tuple{3, "msg #3", "doc_string3"}))
	require.NoError(t, fileProcessor.Store(2, tuple.Tuple{4, "msg #4", "doc_string4"}))
	require.NoError(t, fileProcessor.Store(3, tuple.Tuple{5, "msg #5", "doc_string5"}))

	nextSeqNumList, err = fileProcessor.Commit()
	require.NoError(t, err)
	require.Equal(t, []int{3, 4, 4, 5}, nextSeqNumList)
	require.Equal(t, []TestData{{Id: 0, Message: "msg #0", Document: "doc_string0"}}, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix0+"000000001.parquet"])
	require.Equal(t, []TestData{{Id: 2, Message: "msg #2", Document: "doc_string2"}}, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix0+"000000002.parquet"])
	require.Equal(t, []TestData{{Id: 1, Message: "msg #1", Document: "doc_string1"}}, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix1+"000000002.parquet"])
	require.Equal(t, []TestData{{Id: 3, Message: "msg #3", Document: "doc_string3"}}, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix1+"000000003.parquet"])
	require.Equal(t, []TestData{{Id: 4, Message: "msg #4", Document: "doc_string4"}}, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix2+"000000003.parquet"])
	require.Equal(t, []TestData{{Id: 5, Message: "msg #5", Document: "doc_string5"}}, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix3+"000000004.parquet"])
	require.Equal(t, 6, len(mockS3Uploader.contents))

	// Delete files after usage
	require.NoError(t, fileProcessor.Delete(cloudPrefix0))
	require.Equal(t, []TestData{{Id: 1, Message: "msg #1", Document: "doc_string1"}}, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix1+"000000002.parquet"])
	require.Equal(t, []TestData{{Id: 3, Message: "msg #3", Document: "doc_string3"}}, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix1+"000000003.parquet"])
	require.Equal(t, []TestData{{Id: 4, Message: "msg #4", Document: "doc_string4"}}, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix2+"000000003.parquet"])
	require.Equal(t, []TestData{{Id: 5, Message: "msg #5", Document: "doc_string5"}}, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix3+"000000004.parquet"])
	require.Equal(t, 4, len(mockS3Uploader.contents))

	require.NoError(t, fileProcessor.Delete(cloudPrefix1))
	require.Equal(t, []TestData{{Id: 4, Message: "msg #4", Document: "doc_string4"}}, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix2+"000000003.parquet"])
	require.Equal(t, []TestData{{Id: 5, Message: "msg #5", Document: "doc_string5"}}, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix3+"000000004.parquet"])
	require.Equal(t, 2, len(mockS3Uploader.contents))

	require.NoError(t, fileProcessor.Delete(cloudPrefix2))
	require.Equal(t, []TestData{{Id: 5, Message: "msg #5", Document: "doc_string5"}}, mockS3Uploader.contents["s3://test_bucket/"+cloudPrefix3+"000000004.parquet"])
	require.Equal(t, 1, len(mockS3Uploader.contents))

	require.NoError(t, fileProcessor.Delete(cloudPrefix3))
	require.Equal(t, 0, len(mockS3Uploader.contents))
}

package filesink

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type InitiateMultipartUploadResult struct {
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	UploadID string `xml:"UploadId"`
}

type CompletedPart struct {
	PartNumber string `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type CompleteMultipartUpload struct {
	Parts []CompletedPart `xml:"Part"`
}

type CompleteMultipartUploadResult struct {
	Location string `xml:"Location"`
	Bucket   string `xml:"Bucket"`
	Key      string `xml:"Key"`
	ETag     string `xml:"ETag"`
}

type FakePart struct {
	PartNumber int32
	ETag       string
	Size       int
}

type FakeUpload struct {
	Parts    []FakePart
	Complete bool
}

func (u FakeUpload) Size() int {
	size := 0
	for _, part := range u.Parts {
		size += part.Size
	}
	return size
}

// FakeS3 is a minimal implementation of the S3 API for testing.
type FakeS3 struct {
	HandleCreateMultipartUpload   func(w http.ResponseWriter, r *http.Request)
	HandleCompleteMultipartUpload func(w http.ResponseWriter, r *http.Request)
	HandleUploadPart              func(w http.ResponseWriter, r *http.Request)

	Uploads map[string]*FakeUpload
}

func NewFakeS3() *FakeS3 {
	server := &FakeS3{
		Uploads: make(map[string]*FakeUpload),
	}
	server.HandleCreateMultipartUpload = server.defaultHandleCreateMultipartUpload
	server.HandleCompleteMultipartUpload = server.defaultHandleCompleteMultipartUpload
	server.HandleUploadPart = server.defaultHandleUploadPart
	return server
}

func (s *FakeS3) GenerateUploadId() string {
	buf := make([]byte, 0, 24)
	rand.Read(buf)
	return base64.StdEncoding.EncodeToString(buf)
}

func (s *FakeS3) defaultHandleCreateMultipartUpload(w http.ResponseWriter, r *http.Request) {
	bucket, key := filepath.Split(r.URL.Path)
	bucket = strings.Trim(bucket, "/")

	w.WriteHeader(http.StatusOK)
	resp := InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadID: s.GenerateUploadId(),
	}

	s.Uploads[key] = &FakeUpload{}

	respBody, err := xml.Marshal(&resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(respBody)
}

func (s *FakeS3) defaultHandleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request) {
	bucket, key := filepath.Split(r.URL.Path)
	bucket = strings.Trim(bucket, "/")

	buf, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	req := CompleteMultipartUpload{}
	err = xml.Unmarshal(buf, &req)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	hash := md5.New()
	for _, part := range req.Parts {
		io.WriteString(hash, part.ETag)
	}
	etag := fmt.Sprintf("%x-%d", hash.Sum(nil), len(req.Parts))

	s.Uploads[key].Complete = true

	w.WriteHeader(http.StatusOK)
	resp := CompleteMultipartUploadResult{
		Location: "",
		Bucket:   bucket,
		Key:      key,
		ETag:     etag,
	}
	respBody, err := xml.Marshal(&resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Write(respBody)
}

func (s *FakeS3) defaultHandleUploadPart(w http.ResponseWriter, r *http.Request) {
	bucket, key := filepath.Split(r.URL.Path)
	bucket = strings.Trim(bucket, "/")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	partNumberStr, ok := r.URL.Query()["partNumber"]
	if !ok || len(partNumberStr) != 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	partNumber, err := strconv.ParseInt(partNumberStr[0], 10, 32)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	hash := md5.New()
	hash.Write(body)
	etag := fmt.Sprintf("%x", hash.Sum(nil))

	upload, ok := s.Uploads[key]
	if !ok {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	upload.Parts = append(upload.Parts, FakePart{
		PartNumber: int32(partNumber),
		ETag:       etag,
		Size:       len(body),
	})

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func (s *FakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	switch r.Method {
	case "POST":
		if _, ok := query["uploads"]; ok {
			s.HandleCreateMultipartUpload(w, r)
		} else if _, ok := query["uploadId"]; ok {
			s.HandleCompleteMultipartUpload(w, r)
		}
	case "PUT":
		s.HandleUploadPart(w, r)
	}
}

func TestS3StoreMultipart(t *testing.T) {
	key := "my-key"
	tests := []struct {
		name                          string
		handleCreateMultipartUpload   func(w http.ResponseWriter, r *http.Request)
		handleCompleteMultipartUpload func(w http.ResponseWriter, r *http.Request)
		handleUploadPart              func(w http.ResponseWriter, r *http.Request)
		reader                        io.Reader
		check                         func(t *testing.T, server *FakeS3)
	}{
		{
			name:   "single part",
			reader: bytes.NewReader(make([]byte, 64)),
			check: func(t *testing.T, server *FakeS3) {
				upload, ok := server.Uploads[key]
				require.True(t, ok)
				require.Len(t, upload.Parts, 1)
				require.True(t, upload.Complete)
				require.Equal(t, 64, upload.Size())
			},
		},
		{
			name:   "multi part",
			reader: bytes.NewReader(make([]byte, 16*1024*1024)),
			check: func(t *testing.T, server *FakeS3) {
				upload, ok := server.Uploads[key]
				require.True(t, ok)
				require.Len(t, upload.Parts, 2)
				require.True(t, upload.Complete)
				require.Equal(t, 16*1024*1024, upload.Size())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := NewFakeS3()
			ts := httptest.NewServer(server)
			defer ts.Close()

			ctx := context.Background()

			config := S3StoreConfig{
				Bucket: "test-bucket",
				Region: "us-west-2",
				Credentials: &CredentialsConfig{
					AuthType: AWSAccessKey,
					AccessKeyCredentials: AccessKeyCredentials{
						AWSAccessKeyID:     "123",
						AWSSecretAccessKey: "456",
					},
				},
				Endpoint:     ts.URL,
				UsePathStyle: true,
			}
			store, err := NewS3Store(ctx, config)
			require.NoError(t, err)

			upload, err := store.StageObject(ctx, tt.reader, key)
			require.NoError(t, err)

			err = store.CompleteObject(ctx, upload)
			require.NoError(t, err)

			tt.check(t, server)
		})
	}
}

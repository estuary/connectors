package main

import (
	"bytes"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"net/url"
	"strings"
	"text/template"
	"time"
)

type PipeClient struct {
	cfg             *config
	base            string
	token           string
	httpClient      http.Client
	insertFilesTpl  *template.Template
	insertReportTpl *template.Template
	account         string
}

func publicKeyFingerprint(publicKey *rsa.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return "", fmt.Errorf("marshalling public key: %w", err)
	}
	var hash = sha256.Sum256(der)
	return fmt.Sprintf("SHA256:%s", base64.StdEncoding.EncodeToString(hash[:])), nil
}

func NewPipeClient(cfg *config, tenant string) (*PipeClient, error) {
	httpClient := http.Client{}

	var key, err = cfg.privateKey()
	if err != nil {
		return nil, err
	}

	var account = strings.ToUpper(cfg.Account)
	var user = strings.ToUpper(cfg.Credentials.User)
	var qualifiedUser = fmt.Sprintf("%s.%s", account, user)

	fingerprint, err := publicKeyFingerprint(key.Public().(*rsa.PublicKey))
	if err != nil {
		return nil, err
	}

	var claims = &jwt.RegisteredClaims{
		IssuedAt: jwt.NewNumericDate(time.Now()),
		// TODO: automatically refresh the JWT token
		// JWT tokens for Snowflake can live up to an hour
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(59 * time.Minute)),
		Issuer:    fmt.Sprintf("%s.%s", qualifiedUser, fingerprint),
		Subject:   qualifiedUser,
	}

	var t = jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	jwtToken, err := t.SignedString(key)
	if err != nil {
		return nil, fmt.Errorf("signing key: %w", err)
	}

	var dsn = cfg.ToURI(tenant)
	dsnURL, err := url.Parse(fmt.Sprintf("https://%s", dsn))
	if err != nil {
		return nil, fmt.Errorf("parsing snowflake dsn: %w", err)
	}

	var insertFilesTpl = template.Must(template.New("insertFiles").Parse(insertFilesRawTpl))
	var insertReportTpl = template.Must(template.New("insertFiles").Parse(insertReportRawTpl))

	return &PipeClient{
		cfg:             cfg,
		base:            dsnURL.Hostname(),
		httpClient:      httpClient,
		token:           jwtToken,
		insertFilesTpl:  insertFilesTpl,
		insertReportTpl: insertReportTpl,
	}, nil
}

type insertFilesURLTemplate struct {
	Base     string
	PipeName string
	// Used to track the request, we generate a random uuid for this
	RequestId string
}

type insertFilesRequest struct {
	Files []FileRequest `json:"files"`
}

type FileRequest struct {
	Path string `json:"path"`
	Size int    `json:"size"`
}

type InsertFilesResponse struct {
	Status    string `json:"responseCode"`
	RequestId string `json:"requestId"`
}

const insertFilesRawTpl = "https://{{ $.Base }}/v1/data/pipes/{{ $.PipeName }}/insertFiles?requestId={{ $.RequestId }}"
const contentType = "application/json;charset=UTF-8"
const userAgent = "Estuary Technologies Flow"

// pipeName must be a fully-qualified name, e.g.: database.schema.pipe
func (c *PipeClient) InsertFiles(pipeName string, files []FileRequest) (*InsertFilesResponse, error) {
	var urlTemplate = insertFilesURLTemplate{
		Base:      c.base,
		PipeName:  strings.ToLower(pipeName),
		RequestId: uuid.New().String(),
	}

	var reqBody = insertFilesRequest{
		Files: files,
	}

	var w strings.Builder
	c.insertFilesTpl.Execute(&w, urlTemplate)
	var url = w.String()

	reqBodyJson, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshal insertFiles body: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(reqBodyJson))
	if err != nil {
		return nil, fmt.Errorf("creating insertFiles request: %w", err)
	}

	req.Header.Add("Authorization", fmt.Sprintf("BEARER %s", c.token))
	req.Header.Add("Content-Type", contentType)
	req.Header.Add("User-Agent", userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("insertFiles request: %w", err)
	}

	var respBuf = new(strings.Builder)
	_, err = io.Copy(respBuf, resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("reading response of insertFiles: %w", err)
	}

	log.WithFields(log.Fields{
		"url":  url,
		"body": string(reqBodyJson),
		"resp": respBuf.String(),
	}).Info("pipe client")

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("response error code %d, %s", resp.StatusCode, respBuf)
	}

	var response InsertFilesResponse
	err = json.Unmarshal([]byte(respBuf.String()), &response)
	if err != nil {
		return nil, fmt.Errorf("parsing response of insertFiles failed: %w", err)
	}

	if response.Status != "SUCCESS" {
		return nil, fmt.Errorf("response status %q", response.Status)
	}

	return &response, nil
}

const insertReportRawTpl = "https://{{ $.Base }}/v1/data/pipes/{{ $.PipeName }}/insertReport?requestId={{ $.RequestId }}&beginMark={{ $.BeginMark }}"

type insertReportURLTemplate struct {
	Base     string
	PipeName string
	// Used to track the request, we generate a random uuid for this
	RequestId string

	BeginMark string
}

type fileReport struct {
	Path                   string `json:"path"`
	StageLocation          string `json:"stageLocation"`
	FileSize               int    `json:"fileSize"`
	TimeReceived           string `json:"timeReceived"`
	LastInsertTime         string `json:"lastInsertTime"`
	RowsInserted           int    `json:"rowsInserted"`
	RowsParsed             int    `json:"rowsParsed"`
	ErrorsSeen             int    `json:"errorsSeen"`
	ErrorLimit             int    `json:"errorLimit"`
	FirstError             string `json:"firstError"`
	FirstErrorLineNum      int    `json:"firstErrorLineNum"`
	FirstErrorCharacterPos int    `json:"firstErrorCharacterPos"`
	FirstErrorColumnName   int    `json:"firstErrorColumnName"`
	SystemError            string `json:"systemError"`
	Complete               bool   `json:"complete"`
	Status                 string `json:"status"`
}

type InsertReportResponse struct {
	NextBeginMark string       `json:"nextBeginMark"`
	Files         []fileReport `json:"files"`
}

func (c *PipeClient) InsertReport(pipeName string, beginMark string) (*InsertReportResponse, error) {
	var urlTemplate = insertReportURLTemplate{
		Base:      c.base,
		PipeName:  strings.ToLower(pipeName),
		RequestId: uuid.New().String(),
		BeginMark: beginMark,
	}

	var w strings.Builder
	c.insertReportTpl.Execute(&w, urlTemplate)
	var url = w.String()

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating insertReport request: %w", err)
	}

	req.Header.Add("Authorization", fmt.Sprintf("BEARER %s", c.token))
	req.Header.Add("Content-Type", contentType)
	req.Header.Add("User-Agent", userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("insertReport request: %w", err)
	}

	var respBuf = new(strings.Builder)
	_, err = io.Copy(respBuf, resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("reading response of insertReport: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("response error code %d, %s", resp.StatusCode, respBuf)
	}

	var response InsertReportResponse
	err = json.Unmarshal([]byte(respBuf.String()), &response)
	if err != nil {
		return nil, fmt.Errorf("parsing response of insertReport failed: %w", err)
	}

	log.WithFields(log.Fields{
		"url":      url,
		"headers":  req.Header,
		"response": response,
	}).Info("insertReport")

	return &response, nil
}
